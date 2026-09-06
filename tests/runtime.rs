use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use second_brain_indexer::{
    application::execute_run::ExecutionReport,
    domain::model::{ClaimedRun, IndexedEntityState, Lease, RunId},
    ports::{
        CompletedWork, EnqueueOutcome, EnqueueRequest, FailedWork, RunCompletion, StageWork,
        StateError, StateRepository,
    },
    runtime::{
        bootstrap::Bootstrap,
        lock::ProcessLock,
        logging::record_execution_report,
        metrics::{Metrics, RunOutcome, RuntimeErrorClass},
        scheduler::PollScheduler,
        shutdown::Shutdown,
        worker::{RunProcessor, Worker},
    },
};
use tempfile::TempDir;
use time::OffsetDateTime;

#[derive(Default)]
struct FakeState {
    requests: Mutex<Vec<EnqueueRequest>>,
    recoveries: Mutex<u32>,
    next_claim: Mutex<Option<ClaimedRun>>,
}

#[async_trait]
impl StateRepository for FakeState {
    async fn enqueue(&self, request: EnqueueRequest) -> Result<EnqueueOutcome, StateError> {
        self.requests
            .lock()
            .expect("test mutex")
            .push(request.clone());
        Ok(EnqueueOutcome::Queued {
            run_id: request.run_id,
        })
    }

    async fn claim_next(
        &self,
        _: &str,
        _: OffsetDateTime,
    ) -> Result<Option<ClaimedRun>, StateError> {
        Ok(self.next_claim.lock().expect("test mutex").take())
    }
    async fn renew_claim(
        &self,
        _: &RunId,
        _: &Lease,
        _: OffsetDateTime,
    ) -> Result<bool, StateError> {
        Ok(false)
    }
    async fn recover_expired_claims(&self, _: OffsetDateTime) -> Result<(), StateError> {
        Ok(())
    }
    async fn recover_after_exclusive_start(&self, _: OffsetDateTime) -> Result<(), StateError> {
        *self.recoveries.lock().expect("test mutex") += 1;
        Ok(())
    }
    async fn list_indexed_entities(&self) -> Result<Vec<IndexedEntityState>, StateError> {
        Ok(Vec::new())
    }
    async fn record_snapshot(
        &self,
        _: &RunId,
        _: &Lease,
        _: bool,
        _: u32,
        _: u32,
        _: OffsetDateTime,
    ) -> Result<(), StateError> {
        Ok(())
    }
    async fn stage_work(&self, _: StageWork) -> Result<(), StateError> {
        Ok(())
    }
    async fn complete_work(&self, _: CompletedWork) -> Result<(), StateError> {
        Ok(())
    }
    async fn fail_work(&self, _: FailedWork) -> Result<(), StateError> {
        Ok(())
    }
    async fn finish_run(&self, _: RunCompletion) -> Result<(), StateError> {
        Ok(())
    }
}

#[test]
fn process_lock_rejects_a_second_instance_and_releases_on_drop() {
    let directory = TempDir::new().expect("temporary directory");
    let path = directory.path().join("indexer.lock");
    let first = ProcessLock::acquire(&path).expect("first process acquires lock");
    assert!(ProcessLock::acquire(&path).is_err());
    drop(first);
    let _second = ProcessLock::acquire(&path).expect("lock is released with first process");
}

#[tokio::test]
async fn bootstrap_recovers_state_only_after_acquiring_the_process_lock() {
    let directory = TempDir::new().expect("temporary directory");
    let state = Arc::new(FakeState::default());
    let bootstrap = Bootstrap::start(
        state.clone(),
        directory.path().join("indexer.lock"),
        OffsetDateTime::now_utc(),
    )
    .await
    .expect("exclusive bootstrap succeeds");
    assert_eq!(*state.recoveries.lock().expect("test mutex"), 1);
    assert!(ProcessLock::acquire(directory.path().join("indexer.lock").as_path()).is_err());
    drop(bootstrap);
}

#[tokio::test]
async fn poll_scheduler_enqueues_only_full_scans_and_stops_after_shutdown() {
    let state = Arc::new(FakeState::default());
    let shutdown = Shutdown::new();
    let scheduler = PollScheduler::new(state.clone(), Duration::from_secs(1), shutdown.clone());

    let _run_id = scheduler
        .poll_once(OffsetDateTime::now_utc())
        .await
        .expect("poll queues a run")
        .expect("queue accepts poll");
    shutdown.begin();
    assert!(
        scheduler
            .poll_once(OffsetDateTime::now_utc())
            .await
            .is_none()
    );

    let requests = state.requests.lock().expect("test mutex");
    assert_eq!(requests.len(), 1);
    assert!(matches!(
        requests[0].selector,
        second_brain_indexer::domain::model::Selector::Full
    ));
}

#[derive(Default)]
struct FakeProcessor(Mutex<u32>);

#[async_trait]
impl RunProcessor for FakeProcessor {
    async fn execute(&self, _: ClaimedRun) -> Result<(), String> {
        *self.0.lock().expect("test mutex") += 1;
        Ok(())
    }
}

#[tokio::test]
async fn two_worker_loops_execute_a_durable_claim_only_once() {
    let state = Arc::new(FakeState::default());
    *state.next_claim.lock().expect("test mutex") = Some(ClaimedRun {
        id: RunId::parse("durable-run".to_owned()).expect("valid run ID"),
        selector: second_brain_indexer::domain::model::Selector::Full,
        lease: Lease {
            owner: "first".to_owned(),
            epoch: 1,
        },
    });
    let processor = Arc::new(FakeProcessor::default());
    let first = Worker::new("first", state.clone(), processor.clone());
    let second = Worker::new("second", state, processor.clone());

    let (first_result, second_result) = tokio::join!(
        first.process_once(OffsetDateTime::now_utc()),
        second.process_once(OffsetDateTime::now_utc())
    );
    assert!(
        first_result.expect("first worker result") || second_result.expect("second worker result")
    );
    assert_eq!(*processor.0.lock().expect("test mutex"), 1);
}

#[test]
fn metrics_have_bounded_labels_and_never_render_observations_or_secrets() {
    let metrics = Metrics::default();
    metrics.record_run(RunOutcome::Succeeded);
    metrics.record_error(RuntimeErrorClass::McpTransport);
    let rendered = metrics.render();

    assert!(rendered.contains("second_brain_indexer_runs_total{outcome=\"succeeded\"} 1"));
    assert!(rendered.contains("second_brain_indexer_errors_total{class=\"mcp_transport\"} 1"));
    assert!(!rendered.contains("Bearer"));
    assert!(!rendered.contains("observation"));
}

#[test]
fn failed_execution_report_is_recorded_as_a_failed_run() {
    let metrics = Metrics::default();

    record_execution_report(
        &metrics,
        &ExecutionReport {
            indexed: 0,
            skipped: 0,
            deleted: 0,
            failed: 253,
            failure_counts: std::collections::BTreeMap::from([("embedding_unauthorized", 253)]),
        },
    );

    assert!(
        metrics
            .render()
            .contains("second_brain_indexer_runs_total{outcome=\"failed\"} 1")
    );
}
