use std::{
    num::NonZeroU32,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use second_brain_indexer::{
    application::execute_run::{ExecuteRunError, RunExecutor},
    config::{
        EmbeddingConfig, EmbeddingEngine, McpConfig, McpTransport, RepresentationConfig,
        RetryConfig,
    },
    domain::model::{
        ClaimedRun, DeletionProof, Dimension, Embedding, EntityName, EntityType, GraphEntity,
        GraphSnapshot, Lease, RunId, Selector,
    },
    ports::{
        BatchWriteFailure, BatchWriteResult, CompletedWork, EmbeddingError, EmbeddingProvider,
        EnqueueOutcome, EnqueueRequest, FailedWork, McpError, McpMemoryPort, RunCompletion,
        StageWork, StateError, StateRepository, VectorWrite,
    },
};
use secrecy::SecretString;
use time::OffsetDateTime;
use url::Url;

fn name(value: &str) -> EntityName {
    EntityName::parse(value.to_owned()).expect("test entity name is valid")
}

fn claimed() -> ClaimedRun {
    ClaimedRun {
        id: RunId::parse("run-1".to_owned()).expect("test run id is valid"),
        selector: Selector::Full,
        lease: Lease {
            owner: "worker".to_owned(),
            epoch: 1,
        },
    }
}

fn graph(names: &[&str]) -> GraphSnapshot {
    GraphSnapshot {
        entities: names
            .iter()
            .map(|value| GraphEntity {
                name: name(value),
                entity_type: EntityType::parse("Projekt".to_owned())
                    .expect("test entity type is valid"),
                observations: vec![format!("observation for {value}")],
            })
            .collect(),
        relations: Vec::new(),
        deletion_proof: DeletionProof::Complete,
    }
}

fn mcp_config(batch_size: u16) -> McpConfig {
    McpConfig {
        transport: McpTransport::StreamableHttp,
        endpoint: Url::parse("https://example.test/mcp").expect("test URL is valid"),
        request_timeout: Duration::from_secs(1),
        batch_size,
        bearer_token: SecretString::from("not-a-real-secret"),
    }
}

fn embedding_config() -> EmbeddingConfig {
    EmbeddingConfig {
        engine: EmbeddingEngine::OpenAiCompatible {
            base_url: Url::parse("https://example.test/v1/").expect("test URL is valid"),
            api_key: SecretString::from("not-a-real-secret"),
        },
        model: "test-model".to_owned(),
        dimensions: Dimension::parse(2).expect("test dimension is valid"),
        max_input_chars: NonZeroU32::new(100).expect("non-zero chars"),
        max_input_tokens: NonZeroU32::new(100).expect("non-zero tokens"),
    }
}

fn representation_config() -> RepresentationConfig {
    RepresentationConfig {
        version: "entity-v1".to_owned(),
        taxonomy_version: "taxonomy-v1".to_owned(),
    }
}

fn retry_config() -> RetryConfig {
    RetryConfig {
        max_attempts: NonZeroU32::new(2).expect("non-zero attempts"),
        base_delay: Duration::from_millis(1),
        max_delay: Duration::from_millis(1),
    }
}

#[derive(Clone)]
enum BatchBehavior {
    Success,
    RateLimitedThenSuccess,
    Unauthorized,
    FailNames(Vec<String>),
}

struct FakeMcp {
    graph: GraphSnapshot,
    dimension: u32,
    behavior: BatchBehavior,
    upsert_calls: Arc<Mutex<Vec<Vec<String>>>>,
    attempts: Mutex<u32>,
}

impl FakeMcp {
    fn new(graph: GraphSnapshot, dimension: u32, behavior: BatchBehavior) -> Self {
        Self {
            graph,
            dimension,
            behavior,
            upsert_calls: Arc::new(Mutex::new(Vec::new())),
            attempts: Mutex::new(0),
        }
    }
}

#[async_trait]
impl McpMemoryPort for FakeMcp {
    async fn read_graph(&self) -> Result<GraphSnapshot, McpError> {
        Ok(self.graph.clone())
    }

    async fn upsert_batch(&self, items: &[VectorWrite]) -> Result<BatchWriteResult, McpError> {
        self.upsert_calls.lock().expect("calls lock").push(
            items
                .iter()
                .map(|item| item.entity_name.as_str().to_owned())
                .collect(),
        );
        let mut attempts = self.attempts.lock().expect("attempts lock");
        *attempts += 1;
        match &self.behavior {
            BatchBehavior::Success => Ok(BatchWriteResult {
                upserted: u32::try_from(items.len()).expect("test batch fits u32"),
                failed: Vec::new(),
            }),
            BatchBehavior::RateLimitedThenSuccess if *attempts == 1 => Err(McpError::RateLimited),
            BatchBehavior::RateLimitedThenSuccess => Ok(BatchWriteResult {
                upserted: u32::try_from(items.len()).expect("test batch fits u32"),
                failed: Vec::new(),
            }),
            BatchBehavior::Unauthorized => Err(McpError::Unauthorized),
            BatchBehavior::FailNames(failed_names) => {
                let failed = items
                    .iter()
                    .filter(|item| {
                        failed_names
                            .iter()
                            .any(|name| name == item.entity_name.as_str())
                    })
                    .map(|item| BatchWriteFailure {
                        entity_name: item.entity_name.clone(),
                        code: "bad_input".to_owned(),
                    })
                    .collect::<Vec<_>>();
                Ok(BatchWriteResult {
                    upserted: u32::try_from(items.len() - failed.len())
                        .expect("test batch fits u32"),
                    failed,
                })
            }
        }
    }

    async fn delete(&self, _entity_name: &EntityName) -> Result<(), McpError> {
        Ok(())
    }

    async fn vector_dimension(&self) -> Result<u32, McpError> {
        Ok(self.dimension)
    }
}

struct FakeEmbedding;

#[async_trait]
impl EmbeddingProvider for FakeEmbedding {
    async fn embed(&self, inputs: &[String]) -> Result<Vec<Embedding>, EmbeddingError> {
        inputs
            .iter()
            .map(|_| {
                Embedding::new(
                    vec![0.1, 0.2],
                    Dimension::parse(2).expect("test dimension is valid"),
                )
                .map_err(|_| EmbeddingError::InvalidResponse)
            })
            .collect()
    }
}

#[derive(Default)]
struct FakeStateData {
    staged: Vec<StageWork>,
    completed: Vec<CompletedWork>,
    failed: Vec<FailedWork>,
    completions: Vec<RunCompletion>,
}

struct FakeState {
    data: Mutex<FakeStateData>,
    fail_complete: bool,
}

impl FakeState {
    fn new(fail_complete: bool) -> Self {
        Self {
            data: Mutex::new(FakeStateData::default()),
            fail_complete,
        }
    }
}

#[async_trait]
impl StateRepository for FakeState {
    async fn enqueue(&self, _request: EnqueueRequest) -> Result<EnqueueOutcome, StateError> {
        Err(StateError::Storage("not used by executor tests".to_owned()))
    }
    async fn claim_next(
        &self,
        _owner: &str,
        _now: OffsetDateTime,
    ) -> Result<Option<ClaimedRun>, StateError> {
        Err(StateError::Storage("not used by executor tests".to_owned()))
    }
    async fn renew_claim(
        &self,
        _run_id: &RunId,
        _lease: &Lease,
        _now: OffsetDateTime,
    ) -> Result<bool, StateError> {
        Err(StateError::Storage("not used by executor tests".to_owned()))
    }
    async fn recover_expired_claims(&self, _now: OffsetDateTime) -> Result<(), StateError> {
        Ok(())
    }
    async fn recover_after_exclusive_start(&self, _now: OffsetDateTime) -> Result<(), StateError> {
        Ok(())
    }
    async fn list_indexed_entities(
        &self,
    ) -> Result<Vec<second_brain_indexer::domain::model::IndexedEntityState>, StateError> {
        Ok(Vec::new())
    }
    async fn record_snapshot(
        &self,
        _run_id: &RunId,
        _lease: &Lease,
        _complete: bool,
        _entity_count: u32,
        _relation_count: u32,
        _now: OffsetDateTime,
    ) -> Result<(), StateError> {
        Ok(())
    }
    async fn stage_work(&self, work: StageWork) -> Result<(), StateError> {
        self.data.lock().expect("state lock").staged.push(work);
        Ok(())
    }
    async fn complete_work(&self, work: CompletedWork) -> Result<(), StateError> {
        if self.fail_complete {
            return Err(StateError::Storage("simulated SQLite failure".to_owned()));
        }
        self.data.lock().expect("state lock").completed.push(work);
        Ok(())
    }
    async fn fail_work(&self, work: FailedWork) -> Result<(), StateError> {
        self.data.lock().expect("state lock").failed.push(work);
        Ok(())
    }
    async fn finish_run(&self, completion: RunCompletion) -> Result<(), StateError> {
        self.data
            .lock()
            .expect("state lock")
            .completions
            .push(completion);
        Ok(())
    }
}

fn executor<'a>(
    mcp: &'a FakeMcp,
    embedding: &'a FakeEmbedding,
    state: &'a FakeState,
) -> RunExecutor<'a, FakeMcp, FakeEmbedding, FakeState> {
    executor_with_batch(mcp, embedding, state, 2)
}

fn executor_with_batch<'a>(
    mcp: &'a FakeMcp,
    embedding: &'a FakeEmbedding,
    state: &'a FakeState,
    batch_size: u16,
) -> RunExecutor<'a, FakeMcp, FakeEmbedding, FakeState> {
    let mcp_config = Box::leak(Box::new(mcp_config(batch_size)));
    let embedding_config = Box::leak(Box::new(embedding_config()));
    let representation_config = Box::leak(Box::new(representation_config()));
    let retry_config = Box::leak(Box::new(retry_config()));
    RunExecutor::new(
        mcp,
        embedding,
        state,
        mcp_config,
        embedding_config,
        representation_config,
        retry_config,
    )
}

#[tokio::test]
async fn dimension_mismatch_writes_no_vectors() {
    let mcp = FakeMcp::new(graph(&["A"]), 384, BatchBehavior::Success);
    let state = FakeState::new(false);
    let error = executor(&mcp, &FakeEmbedding, &state)
        .execute(claimed(), OffsetDateTime::UNIX_EPOCH)
        .await
        .expect_err("mismatched dimensions reject the run");
    assert!(matches!(error, ExecuteRunError::DimensionMismatch { .. }));
    assert!(mcp.upsert_calls.lock().expect("calls lock").is_empty());
}

#[tokio::test]
async fn rate_limit_retries_inside_jitter_bound_and_auth_does_not() {
    let rate_limited_mcp = FakeMcp::new(graph(&["A"]), 2, BatchBehavior::RateLimitedThenSuccess);
    let rate_limited_state = FakeState::new(false);
    let result = executor(&rate_limited_mcp, &FakeEmbedding, &rate_limited_state)
        .execute(claimed(), OffsetDateTime::UNIX_EPOCH)
        .await;
    assert!(result.is_ok());
    assert_eq!(
        rate_limited_mcp
            .upsert_calls
            .lock()
            .expect("calls lock")
            .len(),
        2
    );

    let unauthorized_mcp = FakeMcp::new(graph(&["A"]), 2, BatchBehavior::Unauthorized);
    let unauthorized_state = FakeState::new(false);
    let result = executor(&unauthorized_mcp, &FakeEmbedding, &unauthorized_state)
        .execute(claimed(), OffsetDateTime::UNIX_EPOCH)
        .await;
    let report = result.expect("unauthorized MCP response is recorded as a failed item");
    assert_eq!(report.failed, 1);
    assert_eq!(
        report.failure_counts,
        std::collections::BTreeMap::from([("mcp_unauthorized", 1)])
    );
    assert_eq!(
        unauthorized_mcp
            .upsert_calls
            .lock()
            .expect("calls lock")
            .len(),
        1
    );
}

#[tokio::test]
async fn partial_batch_is_split_until_the_permanent_item_is_identified() {
    let mcp = FakeMcp::new(
        graph(&["A", "B", "C"]),
        2,
        BatchBehavior::FailNames(vec!["B".to_owned(), "C".to_owned()]),
    );
    let state = FakeState::new(false);
    let report = executor_with_batch(&mcp, &FakeEmbedding, &state, 3)
        .execute(claimed(), OffsetDateTime::UNIX_EPOCH)
        .await
        .expect("partial MCP result is represented as a partial run");
    assert_eq!(report.indexed, 1);
    assert_eq!(report.failed, 2);
    let calls = mcp.upsert_calls.lock().expect("calls lock").clone();
    assert_eq!(
        calls,
        vec![
            vec!["A".to_owned(), "B".to_owned(), "C".to_owned()],
            vec!["B".to_owned()],
            vec!["C".to_owned()],
        ]
    );
    let data = state.data.lock().expect("state lock");
    assert_eq!(data.completed.len(), 1);
    assert_eq!(data.failed.len(), 2);
}

#[tokio::test]
async fn mcp_success_before_sqlite_failure_leaves_no_indexed_transition_for_replay() {
    let mcp = FakeMcp::new(graph(&["A"]), 2, BatchBehavior::Success);
    let state = FakeState::new(true);
    let result = executor(&mcp, &FakeEmbedding, &state)
        .execute(claimed(), OffsetDateTime::UNIX_EPOCH)
        .await;
    assert!(matches!(result, Err(ExecuteRunError::State(_))));
    assert_eq!(mcp.upsert_calls.lock().expect("calls lock").len(), 1);
    let data = state.data.lock().expect("state lock");
    assert_eq!(data.completed.len(), 0);
    assert_eq!(data.staged.len(), 1);
}
