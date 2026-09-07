use std::{
    num::NonZeroU32,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use second_brain_indexer::{
    adapters::sqlite::SqliteStateRepository,
    application::execute_run::RunExecutor,
    config::{
        EmbeddingConfig, EmbeddingEngine, McpConfig, McpTransport, RepresentationConfig,
        RetryConfig,
    },
    domain::model::{
        Dimension, Embedding, EntityName, EntityType, GraphEntity, GraphSnapshot, RunId,
        RunTrigger, Selector,
    },
    http::{
        BearerAuth, HttpDependencyError, HttpQueryPort, RunView, StatsView, StatusView,
        router_with_shutdown_and_auth,
    },
    ports::{
        BatchWriteResult, EmbeddingError, EmbeddingProvider, EnqueueRequest, McpError,
        McpMemoryPort, StateRepository, VectorWrite,
    },
    runtime::shutdown::Shutdown,
};
use secrecy::SecretString;
use serde_json::{Value, json};
use tempfile::TempDir;
use time::OffsetDateTime;
use tower::ServiceExt;
use url::Url;

const RESET: &str = "/indexer/reset-local-index-state";

#[derive(Default)]
struct External {
    reads: AtomicUsize,
    embeds: AtomicUsize,
    upserts: AtomicUsize,
    deletes: AtomicUsize,
}

#[async_trait]
impl McpMemoryPort for External {
    async fn read_graph(&self) -> Result<GraphSnapshot, McpError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        GraphSnapshot::complete(
            vec![GraphEntity {
                name: EntityName::parse("Alpha".to_owned()).expect("name"),
                entity_type: EntityType::parse("Projekt".to_owned()).expect("type"),
                observations: vec!["unchanged observation".to_owned()],
            }],
            vec![],
        )
        .map_err(|_| McpError::InvalidResponse)
    }

    async fn upsert_batch(&self, items: &[VectorWrite]) -> Result<BatchWriteResult, McpError> {
        self.upserts.fetch_add(items.len(), Ordering::SeqCst);
        Ok(BatchWriteResult {
            upserted: u32::try_from(items.len()).expect("batch"),
            failed: vec![],
        })
    }

    async fn delete(&self, _: &EntityName) -> Result<(), McpError> {
        self.deletes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn vector_dimension(&self) -> Result<u32, McpError> {
        Ok(2)
    }
}

#[async_trait]
impl EmbeddingProvider for External {
    async fn embed(&self, inputs: &[String]) -> Result<Vec<Embedding>, EmbeddingError> {
        self.embeds.fetch_add(inputs.len(), Ordering::SeqCst);
        inputs
            .iter()
            .map(|_| {
                Embedding::new(vec![0.1, 0.2], Dimension::parse(2).expect("dimension"))
                    .map_err(|_| EmbeddingError::InvalidResponse)
            })
            .collect()
    }
}

struct NoQueries;

#[async_trait]
impl HttpQueryPort for NoQueries {
    async fn status(&self) -> Result<StatusView, HttpDependencyError> {
        Err(HttpDependencyError::Unavailable)
    }
    async fn stats(&self) -> Result<StatsView, HttpDependencyError> {
        Err(HttpDependencyError::Unavailable)
    }
    async fn run(&self, _: &RunId) -> Result<Option<RunView>, HttpDependencyError> {
        Err(HttpDependencyError::Unavailable)
    }
    async fn metrics(&self) -> Result<String, HttpDependencyError> {
        Err(HttpDependencyError::Unavailable)
    }
}

struct Fixture {
    _directory: TempDir,
    state: Arc<SqliteStateRepository>,
    external: Arc<External>,
    shutdown: Shutdown,
}

impl Fixture {
    async fn new() -> Self {
        let directory = tempfile::tempdir().expect("temporary directory");
        let state = SqliteStateRepository::connect(
            &directory.path().join("state.db"),
            Duration::from_secs(60),
        )
        .await
        .expect("repository");
        Self {
            _directory: directory,
            state: Arc::new(state),
            external: Arc::new(External::default()),
            shutdown: Shutdown::new(),
        }
    }

    fn app(&self, authenticated: bool) -> Router {
        router_with_shutdown_and_auth(
            self.state.clone(),
            self.external.clone(),
            Arc::new(NoQueries),
            time::Duration::hours(24),
            self.shutdown.clone(),
            if authenticated {
                BearerAuth::enabled(SecretString::from("test-token"))
            } else {
                BearerAuth::disabled()
            },
        )
    }

    async fn count(&self, table: &str) -> i64 {
        // Table names come only from literal test fixtures.
        sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
            .fetch_one(self.state.pool())
            .await
            .expect("count")
    }

    async fn seed(&self) {
        for statement in [
            "INSERT INTO index_configuration VALUES (1,'taxonomy-v1','entity-v1','model',2,'2026-01-01T00:00:00Z')",
            "INSERT INTO run (id,trigger,selector_kind,selector_json,status,queue_state,requested_at,not_before) VALUES ('history','api','full','{\"full\":true}','succeeded','finished','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')",
            "INSERT INTO run_snapshot VALUES ('history',1,1,0,'2026-01-01T00:00:00Z')",
            "INSERT INTO run_work (run_id,entity_name,action,content_hash,status) VALUES ('history','Alpha','upsert','old-hash','succeeded')",
            "INSERT INTO deletion_audit (run_id,entity_name,vector_address,requested_at,result) VALUES ('history','Gone','Gone','2026-01-01T00:00:00Z','deleted')",
            "INSERT INTO entity_index_state (entity_name,entity_type,vector_address,content_hash,status,last_seen_at) VALUES ('Alpha','Projekt','Alpha','old-hash','indexed','2026-01-01T00:00:00Z'), ('Missing','Projekt','Missing','old-hash','failed','2026-01-01T00:00:00Z')",
        ] {
            sqlx::query(statement)
                .execute(self.state.pool())
                .await
                .expect("non-empty seed");
        }
        assert_eq!(self.count("entity_index_state").await, 2);
    }

    async fn enqueue(&self, id: &str) {
        self.state
            .enqueue(EnqueueRequest {
                run_id: RunId::parse(id.to_owned()).expect("run id"),
                trigger: RunTrigger::Fullscan,
                selector: Selector::Full,
                requested_at: OffsetDateTime::now_utc(),
                idempotency: None,
            })
            .await
            .expect("enqueue");
    }
}

async fn post(
    app: Router,
    path: &str,
    body: &str,
    key: Option<&str>,
    token: Option<&str>,
) -> (StatusCode, String) {
    let mut request = Request::post(path).header("Content-Type", "application/json");
    if let Some(key) = key {
        request = request.header("Idempotency-Key", key);
    }
    if let Some(token) = token {
        request = request.header("Authorization", format!("Bearer {token}"));
    }
    let response = app
        .oneshot(request.body(Body::from(body.to_owned())).expect("request"))
        .await
        .expect("response");
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    (status, String::from_utf8(bytes.to_vec()).expect("text"))
}

async fn reset(fixture: &Fixture, key: &str) -> (StatusCode, String) {
    post(
        fixture.app(true),
        RESET,
        "{}",
        Some(key),
        Some("test-token"),
    )
    .await
}

#[tokio::test]
async fn reset_atomically_clears_only_entity_state_and_queues_full_without_mcp_calls() {
    let fixture = Fixture::new().await;
    fixture.seed().await;
    let (status, body) = reset(&fixture, "reset-1").await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    let response: Value = serde_json::from_str(&body).expect("JSON");
    assert_eq!(response["selector"], json!({"full": true}));
    assert_eq!(fixture.count("entity_index_state").await, 0);
    assert_eq!(fixture.count("run").await, 2);
    for table in [
        "index_configuration",
        "run_snapshot",
        "run_work",
        "deletion_audit",
        "idempotency_key",
    ] {
        assert_eq!(fixture.count(table).await, 1, "preserve {table}");
    }
    let run = fixture
        .state
        .claim_next("worker", OffsetDateTime::now_utc())
        .await
        .expect("claim")
        .expect("queued full");
    assert_eq!(run.selector, Selector::Full);
    assert_eq!(run.id.as_str(), response["runId"].as_str().expect("run id"));
    assert_eq!(fixture.external.reads.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.external.upserts.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.external.deletes.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn reset_requires_configured_auth_valid_bearer_empty_object_and_idempotency_key() {
    let fixture = Fixture::new().await;
    fixture.seed().await;
    assert_eq!(
        post(
            fixture.app(false),
            RESET,
            "{}",
            Some("key"),
            Some("test-token")
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    for token in [None, Some("wrong-token")] {
        assert_eq!(
            post(fixture.app(true), RESET, "{}", Some("key"), token)
                .await
                .0,
            StatusCode::UNAUTHORIZED
        );
    }
    for payload in [
        "",
        "null",
        "[]",
        "true",
        "{",
        r#"{"selector":{"full":true}}"#,
        r#"{"force":true}"#,
    ] {
        assert_eq!(
            post(
                fixture.app(true),
                RESET,
                payload,
                Some("key"),
                Some("test-token")
            )
            .await
            .0,
            StatusCode::BAD_REQUEST,
            "payload: {payload}"
        );
    }
    for key in [None, Some(" ")] {
        assert_eq!(
            post(fixture.app(true), RESET, "{}", key, Some("test-token"))
                .await
                .0,
            StatusCode::BAD_REQUEST
        );
    }
    assert_eq!(
        post(
            fixture.app(true),
            RESET,
            &" ".repeat(17000),
            Some("key"),
            Some("test-token")
        )
        .await
        .0,
        StatusCode::PAYLOAD_TOO_LARGE
    );
    assert_eq!(fixture.count("entity_index_state").await, 2);
    assert_eq!(fixture.count("run").await, 1);
}

#[tokio::test]
async fn reset_rejects_queued_and_leased_runs_without_clearing_state() {
    for leased in [false, true] {
        let fixture = Fixture::new().await;
        fixture.seed().await;
        fixture.enqueue("busy").await;
        if leased {
            fixture
                .state
                .claim_next("worker", OffsetDateTime::now_utc())
                .await
                .expect("claim")
                .expect("lease");
        }
        let (status, body) = reset(&fixture, "reset").await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert_eq!(fixture.count("entity_index_state").await, 2);
        assert_eq!(fixture.count("run").await, 2);
        assert_eq!(fixture.count("idempotency_key").await, 0);
    }
}

#[tokio::test]
async fn reset_replays_before_busy_check_and_does_not_clear_new_state() {
    let fixture = Fixture::new().await;
    fixture.seed().await;
    let first = reset(&fixture, "reset").await;
    assert_eq!(first.0, StatusCode::ACCEPTED);
    sqlx::query("INSERT INTO entity_index_state (entity_name,entity_type,vector_address,content_hash,status,last_seen_at) VALUES ('New','Projekt','New','new-hash','indexed','2026-01-01T00:00:00Z')")
        .execute(fixture.state.pool()).await.expect("new state");
    assert_eq!(reset(&fixture, "reset").await, first);
    fixture
        .state
        .claim_next("worker", OffsetDateTime::now_utc())
        .await
        .expect("claim")
        .expect("lease");
    assert_eq!(reset(&fixture, "reset").await, first);
    assert_eq!(fixture.count("entity_index_state").await, 1);
    assert_eq!(fixture.count("run").await, 2);
}

#[tokio::test]
async fn reset_idempotency_key_conflicts_with_regular_index_operations_in_both_directions() {
    for reset_first in [false, true] {
        let fixture = Fixture::new().await;
        fixture.seed().await;
        let (first, second) = if reset_first {
            (RESET, "/indexer/fullscan")
        } else {
            ("/indexer/fullscan", RESET)
        };
        assert_eq!(
            post(
                fixture.app(true),
                first,
                "{}",
                Some("shared"),
                Some("test-token")
            )
            .await
            .0,
            StatusCode::ACCEPTED
        );
        let (status, body) = post(
            fixture.app(true),
            second,
            "{}",
            Some("shared"),
            Some("test-token"),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert!(body.contains("idempotency_key_reused"), "{body}");
        assert_eq!(
            fixture.count("entity_index_state").await,
            if reset_first { 0 } else { 2 }
        );
        assert_eq!(fixture.count("run").await, 2);
    }
}

#[tokio::test]
async fn reset_rolls_back_clear_and_run_when_either_durable_insert_fails() {
    for table in ["run", "idempotency_key"] {
        let fixture = Fixture::new().await;
        fixture.seed().await;
        sqlx::query(&format!("CREATE TRIGGER fail_reset BEFORE INSERT ON {table} BEGIN SELECT RAISE(ABORT, 'injected failure'); END"))
            .execute(fixture.state.pool()).await.expect("failure injection");
        assert_eq!(
            reset(&fixture, "reset").await.0,
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(fixture.count("entity_index_state").await, 2);
        assert_eq!(fixture.count("run").await, 1);
        assert_eq!(fixture.count("idempotency_key").await, 0);
        sqlx::query("DROP TRIGGER fail_reset")
            .execute(fixture.state.pool())
            .await
            .expect("remove injection");
        assert_eq!(reset(&fixture, "reset").await.0, StatusCode::ACCEPTED);
    }
}

#[tokio::test]
async fn simultaneous_resets_have_exactly_one_winner() {
    let fixture = Fixture::new().await;
    fixture.seed().await;
    let (left, right) = tokio::join!(reset(&fixture, "left"), reset(&fixture, "right"));
    let statuses = [left.0, right.0];
    assert_eq!(
        statuses
            .iter()
            .filter(|status| **status == StatusCode::ACCEPTED)
            .count(),
        1
    );
    assert_eq!(
        statuses
            .iter()
            .filter(|status| **status == StatusCode::CONFLICT)
            .count(),
        1
    );
    assert_eq!(fixture.count("run").await, 2);
    assert_eq!(fixture.count("idempotency_key").await, 1);
}

#[tokio::test]
async fn shutdown_refuses_reset_before_mutation() {
    let fixture = Fixture::new().await;
    fixture.seed().await;
    fixture.shutdown.begin();
    assert_eq!(
        reset(&fixture, "reset").await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(fixture.count("entity_index_state").await, 2);
    assert_eq!(fixture.count("run").await, 1);
}

#[tokio::test]
async fn ordinary_full_skips_unchanged_rows_but_reset_full_reembeds_without_deletes() {
    let fixture = Fixture::new().await;
    let mcp = McpConfig {
        transport: McpTransport::StreamableHttp,
        endpoint: Url::parse("https://example.test/mcp").expect("URL"),
        request_timeout: Duration::from_secs(1),
        batch_size: 2,
        bearer_token: SecretString::from("test-token"),
    };
    let embedding = EmbeddingConfig {
        engine: EmbeddingEngine::Ollama {
            base_url: Url::parse("http://127.0.0.1:11434/").expect("URL"),
        },
        model: "test-model".to_owned(),
        dimensions: Dimension::parse(2).expect("dimension"),
        max_input_chars: NonZeroU32::new(100).expect("bound"),
        max_input_tokens: NonZeroU32::new(100).expect("bound"),
    };
    let representation = RepresentationConfig {
        version: "entity-v1".to_owned(),
        taxonomy_version: "taxonomy-v1".to_owned(),
    };
    let retry = RetryConfig {
        max_attempts: NonZeroU32::new(1).expect("attempts"),
        base_delay: Duration::from_millis(1),
        max_delay: Duration::from_millis(1),
    };
    let executor = RunExecutor::new(
        &*fixture.external,
        &*fixture.external,
        &*fixture.state,
        &mcp,
        &embedding,
        &representation,
        &retry,
    );
    for (id, expected_indexed, expected_skipped) in [
        ("first", 1, 0),
        ("ordinary-full-after-vector-rebuild", 0, 1),
    ] {
        fixture.enqueue(id).await;
        let claim = fixture
            .state
            .claim_next("worker", OffsetDateTime::now_utc())
            .await
            .expect("claim")
            .expect("queued");
        let report = executor
            .execute(claim, OffsetDateTime::now_utc())
            .await
            .expect("execute");
        assert_eq!(
            (report.indexed, report.skipped),
            (expected_indexed, expected_skipped)
        );
    }
    assert_eq!(fixture.external.embeds.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.external.upserts.load(Ordering::SeqCst), 1);
    assert_eq!(reset(&fixture, "migration").await.0, StatusCode::ACCEPTED);
    let claim = fixture
        .state
        .claim_next("worker", OffsetDateTime::now_utc())
        .await
        .expect("claim")
        .expect("reset queued");
    let report = executor
        .execute(claim, OffsetDateTime::now_utc())
        .await
        .expect("execute reset");
    assert_eq!((report.indexed, report.skipped, report.deleted), (1, 0, 0));
    assert_eq!(fixture.external.embeds.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.external.upserts.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.external.deletes.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.count("entity_index_state").await, 1);
    assert_eq!(fixture.count("deletion_audit").await, 0);
}
