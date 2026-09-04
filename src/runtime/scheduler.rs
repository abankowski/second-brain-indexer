use std::{sync::Arc, time::Duration};

use thiserror::Error;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{
    domain::model::{RunId, RunTrigger, Selector},
    ports::{EnqueueOutcome, EnqueueRequest, StateError, StateRepository},
    runtime::shutdown::Shutdown,
};

/// Enqueues periodic full scans. It never executes index work itself, which
/// preserves the single durable worker boundary enforced by SQLite leasing.
pub struct PollScheduler<S> {
    state: Arc<S>,
    interval: Duration,
    shutdown: Shutdown,
}

impl<S> PollScheduler<S>
where
    S: StateRepository + 'static,
{
    pub fn new(state: Arc<S>, interval: Duration, shutdown: Shutdown) -> Self {
        Self {
            state,
            interval,
            shutdown,
        }
    }

    /// Returns `None` after shutdown, otherwise records one durable full scan.
    pub async fn poll_once(&self, now: OffsetDateTime) -> Option<Result<RunId, SchedulerError>> {
        if !self.shutdown.is_accepting() {
            return None;
        }
        let run_id = match RunId::parse(Uuid::new_v4().to_string()) {
            Ok(value) => value,
            Err(error) => return Some(Err(SchedulerError::RunId(error.to_string()))),
        };
        match self
            .state
            .enqueue(EnqueueRequest {
                run_id: run_id.clone(),
                trigger: RunTrigger::Poll,
                selector: Selector::Full,
                requested_at: now,
                idempotency: None,
            })
            .await
        {
            Ok(EnqueueOutcome::Queued { run_id } | EnqueueOutcome::Coalesced { run_id }) => {
                Some(Ok(run_id))
            }
            Ok(EnqueueOutcome::IdempotentReplay { run_id, .. }) => Some(Ok(run_id)),
            Ok(EnqueueOutcome::IdempotencyConflict | EnqueueOutcome::Conflict) => {
                Some(Err(SchedulerError::QueueConflict))
            }
            Err(error) => Some(Err(SchedulerError::State(error))),
        }
    }

    pub async fn run(&self, run_on_start: bool) -> Result<(), SchedulerError> {
        if run_on_start {
            if let Some(result) = self.poll_once(OffsetDateTime::now_utc()).await {
                result?;
            }
        }
        loop {
            tokio::select! {
                () = self.shutdown.cancelled() => return Ok(()),
                () = tokio::time::sleep(self.interval) => {
                    if let Some(result) = self.poll_once(OffsetDateTime::now_utc()).await {
                        result?;
                    } else {
                        return Ok(());
                    }
                }
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum SchedulerError {
    #[error("generated run ID was invalid: {0}")]
    RunId(String),
    #[error("scheduler request conflicts with an existing run")]
    QueueConflict,
    #[error("scheduler could not persist a run: {0}")]
    State(#[source] StateError),
}
