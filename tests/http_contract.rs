use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use second_brain_indexer::{
    domain::model::{
        ClaimedRun, EntityName, EntityType, GraphEntity, GraphSnapshot, Lease, RunId, Selector,
    },
    http::{
        BearerAuth, GenerationView, HttpDependencyError, HttpQueryPort, PollingView,
        RunSummaryView, RunView, StatsView, StatusView, router, router_with_auth,
        router_with_shutdown,
    },
    ports::{
        BatchWriteResult, CompletedWork, EnqueueOutcome, EnqueueRequest, FailedWork, McpError,
        McpMemoryPort, RunCompletion, StageWork, StateError, StateRepository, VectorWrite,
    },
    runtime::shutdown::Shutdown,
};
use secrecy::SecretString;
use time::{Duration, OffsetDateTime};
use tower::ServiceExt;

struct FakeState {
    outcomes: Mutex<Vec<EnqueueOutcome>>,
    requests: Mutex<Vec<EnqueueRequest>>,
}

impl FakeState {
    fn queued() -> Self {
        Self {
            outcomes: Mutex::new(Vec::new()),
            requests: Mutex::new(Vec::new()),
        }
    }
    fn with(outcome: EnqueueOutcome) -> Self {
        Self {
            outcomes: Mutex::new(vec![outcome]),
            requests: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl StateRepository for FakeState {
    async fn enqueue(&self, request: EnqueueRequest) -> Result<EnqueueOutcome, StateError> {
        self.requests
            .lock()
            .map_err(|_| StateError::Storage("poisoned fake".to_owned()))?
            .push(request.clone());
        let outcome = self
            .outcomes
            .lock()
            .map_err(|_| StateError::Storage("poisoned fake".to_owned()))?
            .pop();
        Ok(outcome.unwrap_or(EnqueueOutcome::Queued {
            run_id: request.run_id,
        }))
    }
    async fn claim_next(
        &self,
        _: &str,
        _: OffsetDateTime,
    ) -> Result<Option<ClaimedRun>, StateError> {
        Ok(None)
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
        Ok(())
    }
    async fn list_indexed_entities(
        &self,
    ) -> Result<Vec<second_brain_indexer::domain::model::IndexedEntityState>, StateError> {
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

struct FakeMcp {
    available: bool,
}

#[async_trait]
impl McpMemoryPort for FakeMcp {
    async fn read_graph(&self) -> Result<GraphSnapshot, McpError> {
        if !self.available {
            return Err(McpError::Transport("test MCP is unavailable".to_owned()));
        }
        GraphSnapshot::complete(
            vec![GraphEntity {
                name: name("Alpha"),
                entity_type: entity_type("Projekt"),
                observations: Vec::new(),
            }],
            Vec::new(),
        )
        .map_err(|_| McpError::InvalidResponse)
    }
    async fn upsert_batch(&self, _: &[VectorWrite]) -> Result<BatchWriteResult, McpError> {
        Ok(BatchWriteResult {
            upserted: 0,
            failed: Vec::new(),
        })
    }
    async fn delete(&self, _: &EntityName) -> Result<(), McpError> {
        Ok(())
    }
    async fn vector_dimension(&self) -> Result<u32, McpError> {
        Ok(384)
    }
}

struct FakeQuery {
    available: bool,
}

#[async_trait]
impl HttpQueryPort for FakeQuery {
    async fn status(&self) -> Result<StatusView, HttpDependencyError> {
        if !self.available {
            return Err(HttpDependencyError::Unavailable);
        }
        Ok(StatusView {
            ready: true,
            version: "0.1.0".to_owned(),
            active_generation: Some(GenerationView {
                taxonomy_version: "1".to_owned(),
                representation_version: "entity-v1".to_owned(),
                embedding_model: "test".to_owned(),
                dimensions: 384,
            }),
            polling: PollingView {
                enabled: true,
                interval_seconds: 900,
                next_run_at: None,
            },
            run: RunSummaryView {
                in_progress: false,
                last_run_id: None,
                last_run_status: None,
            },
        })
    }
    async fn stats(&self) -> Result<StatsView, HttpDependencyError> {
        Ok(StatsView {
            indexed: 1,
            pending: 0,
            indexing: 0,
            failed: 0,
            delete_pending: 0,
            deleted: 0,
            last_success_at: None,
            last_run_changes: 0,
        })
    }
    async fn run(&self, run_id: &RunId) -> Result<Option<RunView>, HttpDependencyError> {
        Ok((run_id.as_str() == "known").then(|| RunView {
            run_id: "known".to_owned(),
            status: "succeeded".to_owned(),
            selector: second_brain_indexer::http::SelectorResponse::from(&Selector::Full),
            requested_at: "2026-01-01T00:00:00Z".to_owned(),
            started_at: None,
            finished_at: None,
            entities_seen: 1,
            entities_indexed: 1,
            entities_skipped: 0,
            entities_deleted: 0,
            entities_failed: 0,
        }))
    }
    async fn metrics(&self) -> Result<String, HttpDependencyError> {
        Ok("second_brain_indexer_runs_total 1\n".to_owned())
    }
}

fn name(value: &str) -> EntityName {
    EntityName::parse(value.to_owned())
        .unwrap_or_else(|error| panic!("test name rejected: {error}"))
}
fn entity_type(value: &str) -> EntityType {
    EntityType::parse(value.to_owned())
        .unwrap_or_else(|error| panic!("test type rejected: {error}"))
}

fn app(state: Arc<FakeState>, available: bool) -> axum::Router {
    router(
        state,
        Arc::new(FakeMcp { available }),
        Arc::new(FakeQuery { available: true }),
        Duration::hours(24),
    )
}

fn app_with_auth(state: Arc<FakeState>) -> axum::Router {
    router_with_auth(
        state,
        Arc::new(FakeMcp { available: true }),
        Arc::new(FakeQuery { available: true }),
        Duration::seconds(60),
        BearerAuth::enabled(SecretString::from("indexer-test-token")),
    )
}

fn app_with_shutdown(state: Arc<FakeState>, shutdown: Shutdown) -> axum::Router {
    router_with_shutdown(
        state,
        Arc::new(FakeMcp { available: true }),
        Arc::new(FakeQuery { available: true }),
        Duration::hours(24),
        shutdown,
    )
}

async fn response(
    app: axum::Router,
    request: Request<Body>,
) -> (StatusCode, String, Option<String>) {
    let response = app
        .oneshot(request)
        .await
        .unwrap_or_else(|error| panic!("request failed: {error}"));
    let status = response.status();
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap_or_else(|error| panic!("body unavailable: {error}"));
    (
        status,
        String::from_utf8_lossy(&bytes).into_owned(),
        content_type,
    )
}

#[tokio::test]
async fn index_validates_closed_selector_before_enqueueing() {
    let state = Arc::new(FakeState::queued());
    let (status, body, _) = response(
        app(Arc::clone(&state), true),
        Request::post("/indexer/index")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"selector":{"entity":{"name":"Alpha"},"entityType":"Projekt"}}"#,
            ))
            .unwrap_or_else(|error| panic!("request build failed: {error}")),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("invalid_selector"));
    assert!(
        state
            .requests
            .lock()
            .map(|requests| requests.is_empty())
            .unwrap_or(false)
    );
}

#[tokio::test]
async fn accepted_request_preserves_exact_selector_and_idempotency() {
    let state = Arc::new(FakeState::queued());
    let (status, body, _) = response(
        app(Arc::clone(&state), true),
        Request::post("/indexer/index")
            .header(header::CONTENT_TYPE, "application/json")
            .header("Idempotency-Key", "automation-1")
            .body(Body::from(r#"{"selector":{"entityType":"Projekt"}}"#))
            .unwrap_or_else(|error| panic!("request build failed: {error}")),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert!(body.contains("entityType"));
    let requests = state
        .requests
        .lock()
        .unwrap_or_else(|_| panic!("fake state poisoned"));
    assert!(
        matches!(requests.first().map(|request| (&request.selector, request.idempotency.is_some())), Some((Selector::EntityType(kind), true)) if kind.as_str() == "Projekt")
    );
}

#[tokio::test]
async fn coalesced_and_idempotent_outcomes_keep_the_stored_run_response() {
    let state = Arc::new(FakeState::with(EnqueueOutcome::Coalesced {
        run_id: RunId::parse("existing".to_owned())
            .unwrap_or_else(|error| panic!("test run rejected: {error}")),
    }));
    let (status, body, _) = response(
        app(state, true),
        Request::post("/indexer/fullscan")
            .body(Body::empty())
            .unwrap_or_else(|error| panic!("request build failed: {error}")),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert!(body.contains("existing") && body.contains("coalesced"));

    let replay = EnqueueOutcome::IdempotentReplay {
        run_id: RunId::parse("stored".to_owned())
            .unwrap_or_else(|error| panic!("test run rejected: {error}")),
        response_status: 202,
        response_body_json: r#"{"runId":"stored","status":"queued"}"#.to_owned(),
    };
    let (status, body, _) = response(
        app(Arc::new(FakeState::with(replay)), true),
        Request::post("/indexer/fullscan")
            .header("Idempotency-Key", "same")
            .body(Body::empty())
            .unwrap_or_else(|error| panic!("request build failed: {error}")),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(body, r#"{"runId":"stored","status":"queued"}"#);
}

#[tokio::test]
async fn missing_target_is_422_and_dependency_errors_are_redacted_503() {
    let state = Arc::new(FakeState::queued());
    let (status, body, _) = response(
        app(Arc::clone(&state), true),
        Request::post("/indexer/index")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"selector":{"entity":{"name":"Missing"}}}"#))
            .unwrap_or_else(|error| panic!("request build failed: {error}")),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(body.contains("target_not_found"));
    let (status, body, _) = response(
        app(state, false),
        Request::post("/indexer/fullscan")
            .body(Body::empty())
            .unwrap_or_else(|error| panic!("request build failed: {error}")),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(body.contains("unavailable") && !body.contains("Transport"));
}

#[tokio::test]
async fn read_routes_and_body_limit_follow_the_contract() {
    let state = Arc::new(FakeState::queued());
    let (status, body, _) = response(
        app(Arc::clone(&state), true),
        Request::get("/indexer/status")
            .body(Body::empty())
            .unwrap_or_else(|error| panic!("request build failed: {error}")),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("activeGeneration"));
    let (status, _, _) = response(
        app(Arc::clone(&state), true),
        Request::get("/indexer/runs/unknown")
            .body(Body::empty())
            .unwrap_or_else(|error| panic!("request build failed: {error}")),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, body, content_type) = response(
        app(Arc::clone(&state), true),
        Request::get("/indexer/metrics")
            .body(Body::empty())
            .unwrap_or_else(|error| panic!("request build failed: {error}")),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("runs_total"));
    assert!(content_type.unwrap_or_default().starts_with("text/plain"));
    let large = format!(
        r#"{{"selector":{{"entity":{{"name":"{}"}}}}}}"#,
        "x".repeat(17 * 1024)
    );
    let (status, body, _) = response(
        app(state, true),
        Request::post("/indexer/index")
            .body(Body::from(large))
            .unwrap_or_else(|error| panic!("request build failed: {error}")),
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert!(body.contains("request_too_large"));
}

#[tokio::test]
async fn bearer_auth_rejects_missing_or_wrong_tokens_without_leaking_the_expected_token() {
    let state = Arc::new(FakeState::queued());
    let (status, body, _) = response(
        app_with_auth(state.clone()),
        Request::builder()
            .uri("/indexer/status")
            .body(Body::empty())
            .expect("request builds"),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert!(!body.contains("indexer-test-token"));

    let (status, _, _) = response(
        app_with_auth(state.clone()),
        Request::builder()
            .uri("/indexer/status")
            .header("Authorization", "Bearer wrong-token")
            .body(Body::empty())
            .expect("request builds"),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let (status, _, _) = response(
        app_with_auth(state),
        Request::builder()
            .uri("/indexer/status")
            .header("Authorization", "Bearer indexer-test-token")
            .body(Body::empty())
            .expect("request builds"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn shutdown_refuses_new_posts_but_keeps_read_routes_available() {
    let shutdown = Shutdown::new();
    shutdown.begin();
    let state = Arc::new(FakeState::queued());

    let (post_status, post_body, _) = response(
        app_with_shutdown(Arc::clone(&state), shutdown),
        Request::post("/indexer/fullscan")
            .body(Body::empty())
            .unwrap_or_else(|error| panic!("request build failed: {error}")),
    )
    .await;
    assert_eq!(post_status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(post_body.contains("shutting_down"));
    assert!(
        state
            .requests
            .lock()
            .map(|requests| requests.is_empty())
            .unwrap_or(false)
    );

    let (get_status, _, _) = response(
        app_with_shutdown(state, Shutdown::new()),
        Request::get("/indexer/status")
            .body(Body::empty())
            .unwrap_or_else(|error| panic!("request build failed: {error}")),
    )
    .await;
    assert_eq!(get_status, StatusCode::OK);
}
