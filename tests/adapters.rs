use std::{sync::Arc, time::Duration};

use axum::{
    Json, Router,
    body::Body,
    extract::State,
    http::{HeaderMap, Response, StatusCode},
    routing::post,
};
use second_brain_indexer::{
    adapters::{
        mcp::StreamableHttpMcpAdapter, ollama::OllamaEmbeddingAdapter,
        openai::OpenAiEmbeddingAdapter,
    },
    config::{EmbeddingConfig, EmbeddingEngine, McpConfig, McpTransport},
    domain::model::{DeletionProof, Dimension, Embedding, EntityName, EntityType},
    ports::{
        EmbeddingError, EmbeddingProvider, HybridQueryResult, McpError, McpMemoryPort,
        SemanticQueryResult, VectorWrite,
    },
};
use secrecy::SecretString;
use serde_json::{Value, json};
use tokio::{net::TcpListener, sync::Mutex};
use url::Url;

const MCP_MEMORY_QUERY_TOOLS_CONTRACT: &str = r#"
{
  "tools": [
    {
      "name": "vector_search_entities",
      "inputSchema": {
        "type": "object",
        "required": ["embedding"],
        "properties": {
          "embedding": {"type": "array", "items": {"type": "number"}},
          "entityType": {"type": "string"},
          "topK": {"type": "integer", "minimum": 1, "maximum": 100}
        }
      }
    },
    {
      "name": "hybrid_search",
      "inputSchema": {
        "type": "object",
        "required": ["queryEmbedding", "queryText"],
        "properties": {
          "queryEmbedding": {"type": "array", "items": {"type": "number"}},
          "queryText": {"type": "string"},
          "textWeight": {"type": "number"},
          "vecWeight": {"type": "number"},
          "topK": {"type": "integer", "minimum": 1, "maximum": 100}
        }
      }
    }
  ]
}
"#;

#[derive(Clone, Default)]
struct RecordedRequests(Arc<Mutex<Vec<RecordedRequest>>>);

#[derive(Clone, Debug)]
struct RecordedRequest {
    authorization: Option<String>,
    session_id: Option<String>,
    body: Value,
}

async fn serve(app: Router) -> Url {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test listener binds");
    let address = listener.local_addr().expect("test listener has an address");
    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("test server completes cleanly");
    });
    Url::parse(&format!("http://{address}/")).expect("test URL is valid")
}

async fn record(headers: HeaderMap, body: Value, requests: &RecordedRequests) {
    requests.0.lock().await.push(RecordedRequest {
        authorization: headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .map(ToOwned::to_owned),
        session_id: headers
            .get("mcp-session-id")
            .and_then(|value| value.to_str().ok())
            .map(ToOwned::to_owned),
        body,
    });
}

fn mcp_config(endpoint: Url) -> McpConfig {
    McpConfig {
        transport: McpTransport::StreamableHttp,
        endpoint,
        request_timeout: Duration::from_secs(2),
        batch_size: 16,
        bearer_token: SecretString::from("mcp-secret"),
    }
}

fn embedding_config(base_url: Url) -> EmbeddingConfig {
    EmbeddingConfig {
        engine: EmbeddingEngine::OpenAiCompatible {
            base_url,
            api_key: SecretString::from("openai-secret"),
        },
        model: "test-model".to_owned(),
        dimensions: Dimension::parse(2).expect("test dimension is valid"),
        max_input_chars: std::num::NonZeroU32::new(100).expect("non-zero chars"),
        max_input_tokens: std::num::NonZeroU32::new(100).expect("non-zero tokens"),
    }
}

fn ollama_embedding_config(base_url: Url) -> EmbeddingConfig {
    EmbeddingConfig {
        engine: EmbeddingEngine::Ollama { base_url },
        model: "bge-m3".to_owned(),
        dimensions: Dimension::parse(2).expect("test dimension is valid"),
        max_input_chars: std::num::NonZeroU32::new(100).expect("non-zero chars"),
        max_input_tokens: std::num::NonZeroU32::new(100).expect("non-zero tokens"),
    }
}

fn mcp_json_response(id: Value, text: Value) -> Response<Body> {
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {"content": [{"type": "text", "text": text.to_string()}]},
            })
            .to_string(),
        ))
        .expect("JSON-RPC test response is valid")
}

fn mcp_tool_error_response(id: Value) -> Response<Body> {
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {
                    "content": [{"type": "text", "text": "fixture tool error"}],
                    "isError": true,
                },
            })
            .to_string(),
        ))
        .expect("JSON-RPC test response is valid")
}

async fn mcp_handler(
    State(requests): State<RecordedRequests>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response<Body> {
    record(headers, body.clone(), &requests).await;
    match body.get("method").and_then(Value::as_str) {
        Some("initialize") => Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .header("mcp-session-id", "session-1")
            .body(Body::from(
                json!({"jsonrpc":"2.0", "id": body["id"], "result": {}}).to_string(),
            ))
            .expect("initialize response is valid"),
        Some("notifications/initialized") => Response::builder()
            .status(StatusCode::ACCEPTED)
            .body(Body::empty())
            .expect("notification response is valid"),
        Some("tools/call") => {
            let payload = match body["params"]["name"].as_str() {
                Some("read_graph") => json!({
                    "entities": [{"name":"Źródło", "entityType":"Projekt", "observations":["x"]}],
                    "relations": [],
                    "complete": true,
                }),
                Some("vector_batch_upsert") => json!({"upserted": 1, "failed": 0, "errors": []}),
                Some("vector_delete_embedding") => json!({}),
                Some("vector_store_stats") => json!({"dims": 384}),
                _ => json!({}),
            };
            mcp_json_response(body["id"].clone(), payload)
        }
        _ => Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(Body::empty())
            .expect("bad request response is valid"),
    }
}

async fn mcp_query_handler(
    State(requests): State<RecordedRequests>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response<Body> {
    record(headers, body.clone(), &requests).await;
    match body["method"].as_str() {
        Some("initialize") => Response::builder()
            .header("mcp-session-id", "session-query")
            .body(Body::from(
                json!({"jsonrpc":"2.0", "id":body["id"], "result":{}}).to_string(),
            ))
            .expect("initialize response is valid"),
        Some("notifications/initialized") => Response::builder()
            .status(StatusCode::ACCEPTED)
            .body(Body::empty())
            .expect("notification response is valid"),
        Some("tools/call") => match body["params"]["name"].as_str() {
            Some("vector_search_entities") => mcp_json_response(
                body["id"].clone(),
                json!({"results":[{"name":"fixture-semantic","entityType":"Projekt","score":0.75}],"count":1}),
            ),
            Some("hybrid_search") => mcp_json_response(
                body["id"].clone(),
                json!({"results":[{"name":"fixture-hybrid","entityType":"Projekt","score":0.8,"textScore":0.7,"vecScore":0.9}],"count":1}),
            ),
            _ => Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::empty())
                .expect("bad request response is valid"),
        },
        _ => Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(Body::empty())
            .expect("bad request response is valid"),
    }
}

#[tokio::test]
async fn mcp_adapter_negotiates_session_and_sends_typed_tool_requests() {
    let requests = RecordedRequests::default();
    let endpoint = serve(
        Router::new()
            .route("/", post(mcp_handler))
            .with_state(requests.clone()),
    )
    .await;
    let adapter = StreamableHttpMcpAdapter::new(&mcp_config(endpoint)).expect("adapter builds");

    let graph = adapter.read_graph().await.expect("graph reads");
    let entity_name = EntityName::parse("Źródło".to_owned()).expect("name is valid");
    let embedding = Embedding::new(
        vec![0.1; 2],
        Dimension::parse(2).expect("test dimension is valid"),
    )
    .expect("embedding is valid");
    let write = adapter
        .upsert_batch(&[VectorWrite {
            entity_name: entity_name.clone(),
            embedding,
            model: "model-384".to_owned(),
        }])
        .await
        .expect("batch writes");
    adapter.delete(&entity_name).await.expect("delete succeeds");
    assert_eq!(
        adapter.vector_dimension().await.expect("dimension reads"),
        384
    );

    assert!(matches!(graph.deletion_proof, DeletionProof::Complete));
    assert_eq!(graph.entities[0].name.as_str(), "Źródło");
    assert_eq!(write.upserted, 1);
    let requests = requests.0.lock().await;
    assert_eq!(requests.len(), 6);
    assert!(
        requests
            .iter()
            .all(|request| { request.authorization.as_deref() == Some("Bearer mcp-secret") })
    );
    assert_eq!(requests[0].body["method"], "initialize");
    assert_eq!(requests[1].body["method"], "notifications/initialized");
    assert_eq!(requests[2].body["params"]["name"], "read_graph");
    assert_eq!(requests[3].body["params"]["name"], "vector_batch_upsert");
    assert_eq!(
        requests[3].body["params"]["arguments"]["items"][0]["entityName"],
        "Źródło"
    );
    assert_eq!(
        requests[4].body["params"]["name"],
        "vector_delete_embedding"
    );
    assert_eq!(
        requests[4].body["params"]["arguments"]["entityName"],
        "Źródło"
    );
    assert_eq!(requests[5].body["params"]["name"], "vector_store_stats");
    assert!(
        requests[2..]
            .iter()
            .all(|request| request.session_id.as_deref() == Some("session-1"))
    );
}

#[tokio::test]
async fn mcp_adapter_normalizes_semantic_and_hybrid_query_fixtures() {
    let requests = RecordedRequests::default();
    let endpoint = serve(
        Router::new()
            .route("/", post(mcp_query_handler))
            .with_state(requests.clone()),
    )
    .await;
    let adapter = StreamableHttpMcpAdapter::new(&mcp_config(endpoint)).expect("adapter builds");
    let embedding = Embedding::new(
        vec![0.0; 2],
        Dimension::parse(2).expect("test dimension is valid"),
    )
    .expect("embedding is valid");
    let entity_type = EntityType::parse("Projekt".to_owned()).expect("type is valid");

    let semantic = adapter
        .semantic_search(&embedding, Some(&entity_type), Some(2))
        .await
        .expect("semantic result normalizes");
    let hybrid = adapter
        .hybrid_search(&embedding, "fixture query", Some(2))
        .await
        .expect("hybrid result normalizes");

    assert_eq!(
        semantic,
        vec![SemanticQueryResult {
            entity_name: EntityName::parse("fixture-semantic".to_owned()).expect("name is valid"),
            entity_type: EntityType::parse("Projekt".to_owned()).expect("type is valid"),
            score: 0.75,
        }]
    );
    assert_eq!(
        hybrid,
        vec![HybridQueryResult {
            entity_name: EntityName::parse("fixture-hybrid".to_owned()).expect("name is valid"),
            entity_type: EntityType::parse("Projekt".to_owned()).expect("type is valid"),
            score: 0.8,
            text_score: 0.7,
            vec_score: 0.9,
        }]
    );
    let requests = requests.0.lock().await;
    assert_eq!(requests[2].body["params"]["name"], "vector_search_entities");
    assert_eq!(
        requests[2].body["params"]["arguments"],
        json!({
            "embedding": [0.0, 0.0],
            "entityType": "Projekt",
            "topK": 2,
        })
    );
    assert_eq!(requests[3].body["params"]["name"], "hybrid_search");
    assert_eq!(
        requests[3].body["params"]["arguments"],
        json!({
            "queryEmbedding": [0.0, 0.0],
            "queryText": "fixture query",
            "topK": 2,
        })
    );
}

#[test]
fn checked_synthetic_fixture_locks_the_mcp_memory_query_tool_contract() {
    let contract: Value = serde_json::from_str(MCP_MEMORY_QUERY_TOOLS_CONTRACT)
        .expect("synthetic tool contract fixture is JSON");
    let tools = contract["tools"].as_array().expect("tools are an array");
    let semantic = tools
        .iter()
        .find(|tool| tool["name"] == "vector_search_entities")
        .expect("semantic tool is advertised");
    let hybrid = tools
        .iter()
        .find(|tool| tool["name"] == "hybrid_search")
        .expect("hybrid tool is advertised");

    assert_eq!(semantic["inputSchema"]["required"], json!(["embedding"]));
    assert_eq!(
        semantic["inputSchema"]["properties"],
        json!({
            "embedding": {"type": "array", "items": {"type": "number"}},
            "entityType": {"type": "string"},
            "topK": {"type": "integer", "minimum": 1, "maximum": 100},
        })
    );
    assert_eq!(
        hybrid["inputSchema"]["required"],
        json!(["queryEmbedding", "queryText"])
    );
    assert_eq!(
        hybrid["inputSchema"]["properties"],
        json!({
            "queryEmbedding": {"type": "array", "items": {"type": "number"}},
            "queryText": {"type": "string"},
            "textWeight": {"type": "number"},
            "vecWeight": {"type": "number"},
            "topK": {"type": "integer", "minimum": 1, "maximum": 100},
        })
    );
}

#[tokio::test]
async fn mcp_adapter_omits_optional_query_arguments_when_they_are_none() {
    let requests = RecordedRequests::default();
    let endpoint = serve(
        Router::new()
            .route("/", post(mcp_query_handler))
            .with_state(requests.clone()),
    )
    .await;
    let adapter = StreamableHttpMcpAdapter::new(&mcp_config(endpoint)).expect("adapter builds");
    let embedding = Embedding::new(
        vec![0.0; 2],
        Dimension::parse(2).expect("test dimension is valid"),
    )
    .expect("embedding is valid");

    adapter
        .semantic_search(&embedding, None, None)
        .await
        .expect("semantic query succeeds");
    adapter
        .hybrid_search(&embedding, "synthetic fixture", None)
        .await
        .expect("hybrid query succeeds");

    let requests = requests.0.lock().await;
    assert_eq!(
        requests[2].body["params"]["arguments"],
        json!({"embedding": [0.0, 0.0]})
    );
    assert_eq!(
        requests[3].body["params"]["arguments"],
        json!({"queryEmbedding": [0.0, 0.0], "queryText": "synthetic fixture"})
    );
}

#[tokio::test]
async fn mcp_adapter_rejects_out_of_range_query_limits_before_making_an_http_call() {
    let requests = RecordedRequests::default();
    let endpoint = serve(
        Router::new()
            .route("/", post(mcp_query_handler))
            .with_state(requests.clone()),
    )
    .await;
    let adapter = StreamableHttpMcpAdapter::new(&mcp_config(endpoint)).expect("adapter builds");
    let embedding = Embedding::new(
        vec![0.0; 2],
        Dimension::parse(2).expect("test dimension is valid"),
    )
    .expect("embedding is valid");

    assert!(matches!(
        adapter.semantic_search(&embedding, None, Some(0)).await,
        Err(McpError::InvalidResponse)
    ));
    assert!(matches!(
        adapter
            .hybrid_search(&embedding, "synthetic fixture", Some(101))
            .await,
        Err(McpError::InvalidResponse)
    ));
    assert!(requests.0.lock().await.is_empty());
}

#[tokio::test]
async fn mcp_adapter_rejects_successful_hybrid_payloads_with_wrong_count_or_field_type() {
    async fn handler(Json(body): Json<Value>) -> Response<Body> {
        match body["method"].as_str() {
            Some("initialize") => Response::builder()
                .header("mcp-session-id", "session-hybrid-malformed")
                .body(Body::from(
                    json!({"jsonrpc":"2.0", "id":body["id"], "result":{}}).to_string(),
                ))
                .expect("initialize response is valid"),
            Some("notifications/initialized") => Response::builder()
                .status(StatusCode::ACCEPTED)
                .body(Body::empty())
                .expect("notification response is valid"),
            Some("tools/call") => {
                let payload = match body["params"]["arguments"]["queryText"].as_str() {
                    Some("synthetic-count") => json!({
                        "results":[{"name":"fixture","entityType":"Projekt","score":0.8,"textScore":0.7,"vecScore":0.9}],
                        "count":2,
                    }),
                    Some("synthetic-type") => json!({
                        "results":[{"name":"fixture","entityType":"Projekt","score":0.8,"textScore":"wrong","vecScore":0.9}],
                        "count":1,
                    }),
                    _ => json!({}),
                };
                mcp_json_response(body["id"].clone(), payload)
            }
            _ => Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::empty())
                .expect("bad request response is valid"),
        }
    }

    let endpoint = serve(Router::new().route("/", post(handler))).await;
    let adapter = StreamableHttpMcpAdapter::new(&mcp_config(endpoint)).expect("adapter builds");
    let embedding = Embedding::new(
        vec![0.0; 2],
        Dimension::parse(2).expect("test dimension is valid"),
    )
    .expect("embedding is valid");

    for query_text in ["synthetic-count", "synthetic-type"] {
        assert!(matches!(
            adapter.hybrid_search(&embedding, query_text, None).await,
            Err(McpError::InvalidResponse)
        ));
    }
}

#[tokio::test]
async fn mcp_adapter_rejects_malformed_query_results_and_tool_errors() {
    async fn handler(Json(body): Json<Value>) -> Response<Body> {
        match body["method"].as_str() {
            Some("initialize") => Response::builder()
                .header("mcp-session-id", "session-query-errors")
                .body(Body::from(
                    json!({"jsonrpc":"2.0", "id":body["id"], "result":{}}).to_string(),
                ))
                .expect("initialize response is valid"),
            Some("notifications/initialized") => Response::builder()
                .status(StatusCode::ACCEPTED)
                .body(Body::empty())
                .expect("notification response is valid"),
            Some("tools/call") => match body["params"]["name"].as_str() {
                Some("vector_search_entities") => mcp_json_response(
                    body["id"].clone(),
                    json!({"results":[{"name":"fixture","entityType":"Projekt","score":"not-a-number"}],"count":1}),
                ),
                Some("hybrid_search") => mcp_tool_error_response(body["id"].clone()),
                _ => Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Body::empty())
                    .expect("bad request response is valid"),
            },
            _ => Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::empty())
                .expect("bad request response is valid"),
        }
    }

    let endpoint = serve(Router::new().route("/", post(handler))).await;
    let adapter = StreamableHttpMcpAdapter::new(&mcp_config(endpoint)).expect("adapter builds");
    let embedding = Embedding::new(
        vec![0.0; 2],
        Dimension::parse(2).expect("test dimension is valid"),
    )
    .expect("embedding is valid");

    assert!(matches!(
        adapter.semantic_search(&embedding, None, None).await,
        Err(McpError::InvalidResponse)
    ));
    assert!(matches!(
        adapter
            .hybrid_search(&embedding, "fixture query", None)
            .await,
        Err(McpError::InvalidResponse)
    ));
}

#[tokio::test]
async fn mcp_adapter_keeps_data_but_refuses_deletion_proof_without_explicit_evidence() {
    async fn handler(Json(body): Json<Value>) -> Response<Body> {
        match body["method"].as_str() {
            Some("initialize") => Response::builder()
                .header("mcp-session-id", "session-1")
                .body(Body::from(
                    json!({"jsonrpc":"2.0", "id":body["id"], "result":{}}).to_string(),
                ))
                .expect("response is valid"),
            Some("notifications/initialized") => Response::builder()
                .status(StatusCode::ACCEPTED)
                .body(Body::empty())
                .expect("response is valid"),
            Some("tools/call") => mcp_json_response(
                body["id"].clone(),
                json!({"entities":[{"name":"A","entityType":"Projekt","observations":[]}],"relations":[]}),
            ),
            _ => Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::empty())
                .expect("response is valid"),
        }
    }
    let endpoint = serve(Router::new().route("/", post(handler))).await;
    let adapter = StreamableHttpMcpAdapter::new(&mcp_config(endpoint)).expect("adapter builds");
    let graph = adapter
        .read_graph()
        .await
        .expect("graph data is still useful");
    assert_eq!(graph.entities.len(), 1);
    assert!(matches!(graph.deletion_proof, DeletionProof::Unproven(_)));
}

#[tokio::test]
async fn mcp_adapter_connection_failure_names_safe_endpoint_and_cause() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test listener binds");
    let address = listener.local_addr().expect("test listener has an address");
    drop(listener);
    let endpoint = Url::parse(&format!(
        "http://{address}/mcp?accidentally-sensitive=not-for-output"
    ))
    .expect("test URL is valid");
    let adapter = StreamableHttpMcpAdapter::new(&mcp_config(endpoint)).expect("adapter builds");

    let error = adapter
        .vector_dimension()
        .await
        .expect_err("closed listener rejects the MCP request");
    let message = error.to_string();

    assert!(message.contains("could not connect"));
    assert!(message.contains(&format!("http://{address}/mcp")));
    assert!(!message.contains("accidentally-sensitive"));
    assert!(!message.contains("mcp-secret"));
}

async fn embedding_handler(
    State(requests): State<RecordedRequests>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response<Body> {
    record(headers, body, &requests).await;
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"data":[
                {"index":1,"embedding":[2.0, 2.1]},
                {"index":0,"embedding":[1.0, 1.1]}
            ]})
            .to_string(),
        ))
        .expect("embedding response is valid")
}

#[tokio::test]
async fn openai_adapter_sends_auth_and_restores_response_index_order() {
    let requests = RecordedRequests::default();
    let base_url = serve(
        Router::new()
            .route("/embeddings", post(embedding_handler))
            .with_state(requests.clone()),
    )
    .await;
    let adapter = OpenAiEmbeddingAdapter::new(&embedding_config(base_url)).expect("adapter builds");
    let embeddings = adapter
        .embed(&["first".to_owned(), "second".to_owned()])
        .await
        .expect("embedding request succeeds");

    assert_eq!(embeddings[0].values(), &[1.0, 1.1]);
    assert_eq!(embeddings[1].values(), &[2.0, 2.1]);
    let requests = requests.0.lock().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].authorization.as_deref(),
        Some("Bearer openai-secret")
    );
    assert_eq!(requests[0].body["model"], "test-model");
    assert_eq!(requests[0].body["input"], json!(["first", "second"]));
    assert_eq!(requests[0].body["encoding_format"], "float");
    assert_eq!(requests[0].body["dimensions"], 2);
}

#[tokio::test]
async fn ollama_adapter_posts_ordered_batch_without_authorization() {
    let requests = RecordedRequests::default();
    let base_url = serve(
        Router::new()
            .route(
                "/api/embed",
                post(
                    |State(requests): State<RecordedRequests>,
                     headers: HeaderMap,
                     Json(body): Json<Value>| async move {
                        record(headers, body, &requests).await;
                        Json(json!({"embeddings":[[1.0, 1.1], [2.0, 2.1]]}))
                    },
                ),
            )
            .with_state(requests.clone()),
    )
    .await;
    let adapter =
        OllamaEmbeddingAdapter::new(&ollama_embedding_config(base_url)).expect("adapter builds");

    let embeddings = adapter
        .embed(&["first".to_owned(), "second".to_owned()])
        .await
        .expect("embedding request succeeds");

    assert_eq!(embeddings[0].values(), &[1.0, 1.1]);
    assert_eq!(embeddings[1].values(), &[2.0, 2.1]);
    let requests = requests.0.lock().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].authorization, None);
    assert_eq!(requests[0].body["model"], "bge-m3");
    assert_eq!(requests[0].body["input"], json!(["first", "second"]));
    assert_eq!(requests[0].body["truncate"], false);
}

#[tokio::test]
async fn ollama_adapter_rejects_a_response_with_the_wrong_embedding_cardinality() {
    let base_url = serve(Router::new().route(
        "/api/embed",
        post(|| async { Json(json!({"embeddings":[[1.0, 1.1]]})) }),
    ))
    .await;
    let adapter =
        OllamaEmbeddingAdapter::new(&ollama_embedding_config(base_url)).expect("adapter builds");

    let error = adapter
        .embed(&["first".to_owned(), "second".to_owned()])
        .await
        .expect_err("a response with fewer vectors is invalid");

    assert!(matches!(error, EmbeddingError::InvalidResponse));
}

#[tokio::test]
async fn ollama_adapter_classifies_http_failures_without_leaking_provider_bodies() {
    for status in [
        StatusCode::UNAUTHORIZED,
        StatusCode::TOO_MANY_REQUESTS,
        StatusCode::INTERNAL_SERVER_ERROR,
        StatusCode::BAD_REQUEST,
    ] {
        let base_url = serve(Router::new().route(
            "/api/embed",
            post(move || async move {
                Response::builder()
                    .status(status)
                    .body(Body::from("provider-body-not-for-output"))
                    .expect("test response is valid")
            }),
        ))
        .await;
        let adapter = OllamaEmbeddingAdapter::new(&ollama_embedding_config(base_url))
            .expect("adapter builds");

        let error = adapter
            .embed(&["non-secret test input".to_owned()])
            .await
            .expect_err("non-success HTTP response fails");

        match status {
            StatusCode::UNAUTHORIZED => assert!(matches!(error, EmbeddingError::Unauthorized)),
            StatusCode::TOO_MANY_REQUESTS => assert!(matches!(error, EmbeddingError::RateLimited)),
            StatusCode::INTERNAL_SERVER_ERROR => assert!(matches!(error, EmbeddingError::Server)),
            StatusCode::BAD_REQUEST => assert!(matches!(error, EmbeddingError::InvalidResponse)),
            _ => unreachable!("test table only contains bounded error classes"),
        }
        assert!(!error.to_string().contains("provider-body-not-for-output"));
        assert!(!error.to_string().contains("non-secret test input"));
    }
}

#[tokio::test]
async fn ollama_adapter_rejects_a_vector_with_the_wrong_dimension() {
    let base_url = serve(Router::new().route(
        "/api/embed",
        post(|| async { Json(json!({"embeddings":[[1.0]]})) }),
    ))
    .await;
    let adapter =
        OllamaEmbeddingAdapter::new(&ollama_embedding_config(base_url)).expect("adapter builds");

    let error = adapter
        .embed(&["input".to_owned()])
        .await
        .expect_err("a wrong-dimension vector is invalid");

    assert!(matches!(error, EmbeddingError::InvalidResponse));
}

#[tokio::test]
async fn openai_adapter_rejects_auth_rate_limit_and_invalid_cardinality_without_leaking_secret() {
    async fn unauthorized() -> Response<Body> {
        Response::builder()
            .status(StatusCode::UNAUTHORIZED)
            .body(Body::empty())
            .expect("response is valid")
    }
    let endpoint = serve(Router::new().route("/embeddings", post(unauthorized))).await;
    let adapter = OpenAiEmbeddingAdapter::new(&embedding_config(endpoint)).expect("adapter builds");
    let error = adapter
        .embed(&["secret input".to_owned()])
        .await
        .expect_err("401 fails");
    assert!(matches!(error, EmbeddingError::Unauthorized));
    assert!(!error.to_string().contains("openai-secret"));
    assert!(!error.to_string().contains("secret input"));
}
