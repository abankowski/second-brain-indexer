use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;
use time::OffsetDateTime;

use crate::{
    domain::model::ClaimedRun,
    ports::{StateError, StateRepository},
};

/// Runtime-facing execution seam. It receives an already durable, fenced claim;
/// implementations must finish or fail it through the state repository.
#[async_trait]
pub trait RunProcessor: Send + Sync {
    async fn execute(&self, claimed: ClaimedRun) -> Result<(), String>;
}

/// Claims at most one run per call. Multiple loops may call this concurrently:
/// only the repository's atomic lease winner reaches the processor.
pub struct Worker<S, P> {
    owner: String,
    state: Arc<S>,
    processor: Arc<P>,
}

impl<S, P> Worker<S, P>
where
    S: StateRepository + 'static,
    P: RunProcessor + 'static,
{
    pub fn new(owner: impl Into<String>, state: Arc<S>, processor: Arc<P>) -> Self {
        Self {
            owner: owner.into(),
            state,
            processor,
        }
    }

    /// `Ok(true)` denotes exactly one claimed run, `Ok(false)` an empty queue.
    pub async fn process_once(&self, now: OffsetDateTime) -> Result<bool, WorkerError> {
        let claimed = self.state.claim_next(&self.owner, now).await?;
        let Some(claimed) = claimed else {
            return Ok(false);
        };
        self.processor
            .execute(claimed)
            .await
            .map_err(WorkerError::Processor)?;
        Ok(true)
    }
}

#[derive(Debug, Error)]
pub enum WorkerError {
    #[error("worker could not claim a queued run: {0}")]
    State(#[from] StateError),
    #[error("run processor failed: {0}")]
    Processor(String),
}
