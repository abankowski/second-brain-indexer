use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use second_brain_indexer::{
    domain::model::{
        ClaimedRun, Dimension, Embedding, EntityName, EntityType, GraphEntity, GraphSnapshot,
        Lease, RunId, Selector,
    },
    http::{
        BearerAuth, GenerationView, HttpDependencyError, HttpQueryPort, PollingView,
        RunSummaryView, RunView, StatsView, StatusView, router, router_with_auth,
        router_with_shutdown,
    },
    ports::{
        BatchWriteResult, CompletedWork, EmbeddingError, EmbeddingProvider, EnqueueOutcome,
        EnqueueRequest, FailedWork, HybridQueryResult, McpError, McpMemoryPort, RunCompletion,
        SemanticQueryResult, StageWork, StateError, StateRepository, VectorWrite,
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
    async fn semantic_search(
        &self,
        embedding: &Embedding,
        kind: Option<&EntityType>,
        limit: Option<u32>,
    ) -> Result<Vec<SemanticQueryResult>, McpError> {
        if !self.available {
            return Err(McpError::Transport(
                "SECRET provider response body".to_owned(),
            ));
        }
        assert_eq!(embedding.values(), &[0.12345; 384]);
        assert_eq!(kind.map(EntityType::as_str), Some("Projekt"));
        assert_eq!(limit, Some(7));
        Ok(vec![SemanticQueryResult {
            entity_name: name("Alpha"),
            entity_type: entity_type("Projekt"),
            score: 0.9,
        }])
    }
    async fn hybrid_search(
        &self,
        embedding: &Embedding,
        query: &str,
        limit: Option<u32>,
    ) -> Result<Vec<HybridQueryResult>, McpError> {
        if !self.available {
            return Err(McpError::Transport(
                "SECRET provider response body".to_owned(),
            ));
        }
        assert_eq!(embedding.values(), &[0.12345; 384]);
        assert_eq!(query, "find a project");
        assert_eq!(limit, Some(7));
        Ok(vec![HybridQueryResult {
            entity_name: name("Alpha"),
            entity_type: entity_type("Projekt"),
            score: 0.8,
            text_score: 0.7,
            vec_score: 0.9,
        }])
    }
}

struct FakeEmbedding;

#[async_trait]
impl EmbeddingProvider for FakeEmbedding {
    async fn embed(&self, inputs: &[String]) -> Result<Vec<Embedding>, EmbeddingError> {
        assert_eq!(inputs, &["find a project"]);
        Ok(vec![
            Embedding::new(
                vec![0.12345; 384],
                Dimension::parse(384).expect("dimension"),
            )
            .expect("embedding"),
        ])
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
        Arc::clone(&state),
        Arc::new(FakeMcp { available: true }),
        Arc::new(FakeQuery { available: true }),
        Duration::seconds(60),
        BearerAuth::enabled(SecretString::from("indexer-test-token")),
    )
    .merge(second_brain_indexer::indexer_mcp::indexer_mcp_router(
        state,
        Arc::new(FakeMcp { available: true }),
        Arc::new(FakeQuery { available: true }),
        Arc::new(FakeEmbedding),
        Duration::seconds(60),
        Shutdown::new(),
        BearerAuth::enabled(SecretString::from("indexer-test-token")),
    ))
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

#[tokio::test]
async fn indexer_mcp_initializes_without_a_session() {
    let (status, body, _) = response(
        app_with_auth(Arc::new(FakeState::queued())),
        Request::post("/indexer/mcp")
            .header(header::AUTHORIZATION, "Bearer indexer-test-token")
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::ACCEPT, "application/json, text/event-stream")
            .body(Body::from(r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"contract-test","version":"1"}}}"#))
            .expect("request builds"),
    ).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let result: serde_json::Value = serde_json::from_str(&body).expect("JSON response");
    assert_eq!(result["result"]["protocolVersion"], "2025-03-26");
    assert_eq!(
        result["result"]["capabilities"]["tools"],
        serde_json::json!({"listChanged":false})
    );
}

fn mcp_request(value: serde_json::Value) -> Request<Body> {
    Request::post("/indexer/mcp")
        .header(header::AUTHORIZATION, "Bearer indexer-test-token")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::ACCEPT, "application/json, text/event-stream")
        .body(Body::from(value.to_string()))
        .expect("request builds")
}

fn tool_request(tool: &str, arguments: serde_json::Value) -> Request<Body> {
    mcp_request(
        serde_json::json!({"jsonrpc":"2.0","id":"query-1","method":"tools/call","params":{"name":tool,"arguments":arguments}}),
    )
}

fn tool_body(body: &str) -> serde_json::Value {
    let envelope: serde_json::Value = serde_json::from_str(body).expect("JSON-RPC response");
    assert_eq!(envelope["id"], "query-1");
    serde_json::from_str(
        envelope["result"]["content"][0]["text"]
            .as_str()
            .expect("text content"),
    )
    .expect("tool JSON")
}

#[tokio::test]
async fn indexer_mcp_lists_only_the_five_typed_tools() {
    let (status, body, _) = response(
        app_with_auth(Arc::new(FakeState::queued())),
        mcp_request(serde_json::json!({"jsonrpc":"2.0","id":2,"method":"tools/list"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let envelope: serde_json::Value = serde_json::from_str(&body).expect("JSON-RPC response");
    let tools = envelope["result"]["tools"].as_array().expect("tools");
    assert_eq!(
        tools
            .iter()
            .map(|tool| tool["name"].as_str().expect("name"))
            .collect::<Vec<_>>(),
        vec![
            "indexer_semantic_search",
            "indexer_hybrid_search",
            "indexer_reindex_entity",
            "indexer_reindex_all",
            "indexer_run_status"
        ]
    );
    for tool in tools {
        assert_eq!(tool["inputSchema"]["additionalProperties"], false);
    }
    assert_eq!(
        tools[0]["inputSchema"]["required"],
        serde_json::json!(["query"])
    );
}

#[tokio::test]
async fn indexer_mcp_authenticates_every_request() {
    for token in [None, Some("Bearer wrong-token")] {
        let mut request = tool_request("indexer_reindex_all", serde_json::json!({}));
        request.headers_mut().remove(header::AUTHORIZATION);
        if let Some(token) = token {
            request
                .headers_mut()
                .insert(header::AUTHORIZATION, token.parse().expect("header"));
        }
        let state = Arc::new(FakeState::queued());
        let (status, body, _) = response(app_with_auth(state.clone()), request).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert!(!body.contains("indexer-test-token"));
        assert!(state.requests.lock().expect("requests").is_empty());
    }
}

#[tokio::test]
async fn indexer_mcp_embeds_searches_server_side_and_returns_normalized_rows() {
    for (tool, expected) in [
        (
            "indexer_semantic_search",
            serde_json::json!({"results":[{"entityName":"Alpha","entityType":"Projekt","score":0.9}]}),
        ),
        (
            "indexer_hybrid_search",
            serde_json::json!({"results":[{"entityName":"Alpha","entityType":"Projekt","score":0.8,"textScore":0.7,"vecScore":0.9}]}),
        ),
    ] {
        let (status, body, _) = response(
            app_with_auth(Arc::new(FakeState::queued())),
            tool_request(
                tool,
                serde_json::json!({"query":"find a project","limit":7,"entityType":"Projekt"}),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(tool_body(&body), expected);
        for forbidden in ["embedding", "0.12345", "SECRET", "observations"] {
            assert!(!body.contains(forbidden));
        }
    }
}

#[tokio::test]
async fn indexer_mcp_invalid_arguments_are_tool_errors_without_queue_writes() {
    let state = Arc::new(FakeState::queued());
    for (tool, arguments) in [
        ("indexer_semantic_search", serde_json::json!({"query":" "})),
        (
            "indexer_semantic_search",
            serde_json::json!({"query":"find a project","limit":0}),
        ),
        (
            "indexer_hybrid_search",
            serde_json::json!({"query":"find a project","limit":101}),
        ),
        (
            "indexer_semantic_search",
            serde_json::json!({"query":"find a project","embedding":[1,2]}),
        ),
        (
            "indexer_reindex_entity",
            serde_json::json!({"entityName":""}),
        ),
        ("indexer_reindex_all", serde_json::json!({"force":true})),
    ] {
        let (status, body, _) =
            response(app_with_auth(state.clone()), tool_request(tool, arguments)).await;
        assert_eq!(status, StatusCode::OK);
        let envelope: serde_json::Value = serde_json::from_str(&body).expect("JSON response");
        assert_eq!(envelope["result"]["isError"], true, "{body}");
        assert_eq!(tool_body(&body)["error"]["code"], "invalid_arguments");
    }
    assert!(state.requests.lock().expect("requests").is_empty());
}

#[tokio::test]
async fn indexer_mcp_reindex_and_status_reuse_existing_ports() {
    let state = Arc::new(FakeState::queued());
    for (tool, args, selector) in [
        (
            "indexer_reindex_entity",
            serde_json::json!({"entityName":"Alpha"}),
            Selector::Entity(name("Alpha")),
        ),
        ("indexer_reindex_all", serde_json::json!({}), Selector::Full),
    ] {
        let (status, body, _) =
            response(app_with_auth(state.clone()), tool_request(tool, args)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(tool_body(&body)["status"], "queued");
        assert_eq!(
            state
                .requests
                .lock()
                .expect("requests")
                .last()
                .expect("request")
                .selector,
            selector
        );
    }
    let (_, body, _) = response(
        app_with_auth(state),
        tool_request("indexer_run_status", serde_json::json!({"runId":"known"})),
    )
    .await;
    assert_eq!(tool_body(&body)["entitiesIndexed"], 1);
    assert_eq!(tool_body(&body)["status"], "succeeded");
}

#[tokio::test]
async fn indexer_mcp_handles_notifications_and_protocol_errors() {
    let app = app_with_auth(Arc::new(FakeState::queued()));
    let (status, body, _) = response(
        app.clone(),
        mcp_request(serde_json::json!({"jsonrpc":"2.0","method":"notifications/initialized"})),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert!(body.is_empty());
    let (_, body, _) = response(
        app.clone(),
        mcp_request(serde_json::json!({"jsonrpc":"2.0","id":3,"method":"unknown"})),
    )
    .await;
    let envelope: serde_json::Value = serde_json::from_str(&body).expect("JSON response");
    assert_eq!(envelope["error"]["code"], -32601);
    let (status, _, _) = response(
        app,
        Request::get("/indexer/mcp")
            .header(header::AUTHORIZATION, "Bearer indexer-test-token")
            .body(Body::empty())
            .expect("request"),
    )
    .await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
}

fn mcp_app<S: StateRepository + 'static>(
    state: Arc<S>,
    available: bool,
    shutdown: Shutdown,
    embedding: Arc<dyn EmbeddingProvider>,
) -> axum::Router {
    second_brain_indexer::indexer_mcp::indexer_mcp_router(
        state,
        Arc::new(FakeMcp { available }),
        Arc::new(FakeQuery { available: true }),
        embedding,
        Duration::seconds(60),
        shutdown,
        BearerAuth::enabled(SecretString::from("indexer-test-token")),
    )
}

#[tokio::test]
async fn indexer_mcp_coalesces_real_durable_active_queue() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let state = Arc::new(
        second_brain_indexer::adapters::sqlite::SqliteStateRepository::connect(
            &directory.path().join("state.db"),
            std::time::Duration::from_secs(60),
        )
        .await
        .expect("repository"),
    );
    let app = mcp_app(
        state.clone(),
        true,
        Shutdown::new(),
        Arc::new(FakeEmbedding),
    );
    let (_, first, _) = response(
        app.clone(),
        tool_request("indexer_reindex_all", serde_json::json!({})),
    )
    .await;
    let first = tool_body(&first);
    assert_eq!(first["coalesced"], false);
    let (_, repeated, _) = response(
        app.clone(),
        tool_request("indexer_reindex_all", serde_json::json!({})),
    )
    .await;
    assert_eq!(tool_body(&repeated)["coalesced"], true);
    assert_eq!(tool_body(&repeated)["runId"], first["runId"]);
    assert!(
        state
            .claim_next("test-worker", OffsetDateTime::now_utc())
            .await
            .expect("claim")
            .is_some()
    );
    let (_, queued, _) = response(
        app.clone(),
        tool_request(
            "indexer_reindex_entity",
            serde_json::json!({"entityName":"Alpha"}),
        ),
    )
    .await;
    let queued = tool_body(&queued);
    assert_eq!(queued["coalesced"], false);
    assert_ne!(queued["runId"], first["runId"]);
    let (_, repeated, _) = response(
        app.clone(),
        tool_request(
            "indexer_reindex_entity",
            serde_json::json!({"entityName":"Alpha"}),
        ),
    )
    .await;
    assert_eq!(tool_body(&repeated)["coalesced"], true);
    assert_eq!(tool_body(&repeated)["runId"], queued["runId"]);
    let (_, conflict, _) = response(
        app,
        tool_request("indexer_reindex_all", serde_json::json!({})),
    )
    .await;
    assert_eq!(tool_body(&conflict)["error"]["code"], "run_in_progress");
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM run")
        .fetch_one(state.pool())
        .await
        .expect("count runs");
    assert_eq!(count, 2);
}

#[tokio::test]
async fn indexer_mcp_shutdown_refuses_new_work_but_allows_run_status() {
    let shutdown = Shutdown::new();
    shutdown.begin();
    let state = Arc::new(FakeState::queued());
    let app = mcp_app(state.clone(), true, shutdown, Arc::new(FakeEmbedding));
    for (tool, args) in [
        ("indexer_reindex_all", serde_json::json!({})),
        (
            "indexer_reindex_entity",
            serde_json::json!({"entityName":"Alpha"}),
        ),
        (
            "indexer_semantic_search",
            serde_json::json!({"query":"find a project","entityType":"Projekt","limit":7}),
        ),
    ] {
        let (_, body, _) = response(app.clone(), tool_request(tool, args)).await;
        assert_eq!(tool_body(&body)["error"]["code"], "shutting_down");
    }
    let (_, body, _) = response(
        app,
        tool_request("indexer_run_status", serde_json::json!({"runId":"known"})),
    )
    .await;
    assert_eq!(tool_body(&body)["status"], "succeeded");
    assert!(state.requests.lock().expect("requests").is_empty());
}

struct BrokenEmbedding;

#[async_trait]
impl EmbeddingProvider for BrokenEmbedding {
    async fn embed(&self, _: &[String]) -> Result<Vec<Embedding>, EmbeddingError> {
        Err(EmbeddingError::Unauthorized)
    }
}

#[tokio::test]
async fn indexer_mcp_redacts_embedding_and_backend_errors() {
    let providers: [(bool, Arc<dyn EmbeddingProvider>); 2] = [
        (false, Arc::new(FakeEmbedding)),
        (true, Arc::new(BrokenEmbedding)),
    ];
    for (available, provider) in providers {
        let app = mcp_app(
            Arc::new(FakeState::queued()),
            available,
            Shutdown::new(),
            provider,
        );
        for tool in ["indexer_semantic_search", "indexer_hybrid_search"] {
            let (_, body, _) = response(
                app.clone(),
                tool_request(
                    tool,
                    serde_json::json!({"query":"find a project","entityType":"Projekt","limit":7}),
                ),
            )
            .await;
            assert_eq!(
                tool_body(&body),
                serde_json::json!({"error":{"code":"unavailable","message":"service is unavailable"}})
            );
            for forbidden in [
                "SECRET",
                "Unauthorized",
                "find a project",
                "indexer-test-token",
            ] {
                assert!(!body.contains(forbidden));
            }
        }
    }
}

#[tokio::test]
async fn indexer_mcp_transport_rejects_untrusted_origins_and_invalid_envelopes() {
    let app = app_with_auth(Arc::new(FakeState::queued()));
    let mut request =
        mcp_request(serde_json::json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}));
    request.headers_mut().insert(
        header::ORIGIN,
        "https://untrusted.example".parse().expect("origin"),
    );
    assert_eq!(
        response(app.clone(), request).await.0,
        StatusCode::FORBIDDEN
    );
    for value in [
        serde_json::json!([]),
        serde_json::json!({"jsonrpc":"1.0","id":1,"method":"tools/list"}),
        serde_json::json!({"jsonrpc":"2.0","id":null,"method":"tools/list"}),
        serde_json::json!({"jsonrpc":"2.0","id":1,"result":{},"error":{"code":-32603,"message":"Internal error"}}),
        serde_json::json!({"jsonrpc":"2.0","id":1,"error":{"code":-32603}}),
    ] {
        let (_, body, _) = response(app.clone(), mcp_request(value)).await;
        let envelope: serde_json::Value = serde_json::from_str(&body).expect("JSON response");
        assert_eq!(envelope["error"]["code"], -32600);
    }
    let (_, body, _) = response(
        app,
        mcp_request(serde_json::json!([
            {"jsonrpc":"2.0","method":"notifications/initialized"},
            {"jsonrpc":"2.0","id":4,"method":"tools/list"}
        ])),
    )
    .await;
    let envelope: serde_json::Value = serde_json::from_str(&body).expect("batch response");
    assert_eq!(envelope.as_array().expect("batch").len(), 1);
    assert_eq!(envelope[0]["id"], 4);
}

#[tokio::test]
async fn indexer_mcp_checks_origin_on_get_and_accepts_client_response_envelopes() {
    let app = mcp_app(
        Arc::new(FakeState::queued()),
        true,
        Shutdown::new(),
        Arc::new(FakeEmbedding),
    );
    let (status, _, _) = response(
        app.clone(),
        Request::get("/indexer/mcp")
            .header(header::AUTHORIZATION, "Bearer indexer-test-token")
            .header(header::ORIGIN, "https://untrusted.example")
            .body(Body::empty())
            .expect("request"),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    for envelope in [
        serde_json::json!({"jsonrpc":"2.0","id":5,"result":{}}),
        serde_json::json!({"jsonrpc":"2.0","id":"request-5","error":{"code":-32603,"message":"Internal error"}}),
    ] {
        let (status, body, _) = response(app.clone(), mcp_request(envelope)).await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert!(body.is_empty());
    }
}

#[tokio::test]
async fn indexer_mcp_missing_runs_and_entities_report_bounded_tool_errors() {
    let state = Arc::new(FakeState::queued());
    for (tool, args, code) in [
        (
            "indexer_run_status",
            serde_json::json!({"runId":"unknown"}),
            "run_not_found",
        ),
        (
            "indexer_reindex_entity",
            serde_json::json!({"entityName":"Missing"}),
            "target_not_found",
        ),
    ] {
        let (_, body, _) = response(
            mcp_app(
                state.clone(),
                true,
                Shutdown::new(),
                Arc::new(FakeEmbedding),
            ),
            tool_request(tool, args),
        )
        .await;
        assert_eq!(tool_body(&body)["error"]["code"], code);
    }
    assert!(state.requests.lock().expect("requests").is_empty());
}

#[tokio::test]
async fn indexer_mcp_hybrid_filter_applies_to_normalized_top_results() {
    let (_, body, _) = response(
        app_with_auth(Arc::new(FakeState::queued())),
        tool_request(
            "indexer_hybrid_search",
            serde_json::json!({"query":"find a project","limit":7,"entityType":"Osoba"}),
        ),
    )
    .await;
    assert_eq!(tool_body(&body), serde_json::json!({"results":[]}));
}
