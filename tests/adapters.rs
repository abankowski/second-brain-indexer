use std::{sync::Arc, time::Duration};

use axum::{
    Json, Router,
    body::Body,
    extract::State,
    http::{HeaderMap, Response, StatusCode},
    routing::post,
};
use second_brain_indexer::{
    adapters::{mcp::StreamableHttpMcpAdapter, openai::OpenAiEmbeddingAdapter},
    config::{EmbeddingConfig, McpConfig, McpTransport},
    domain::model::{DeletionProof, Dimension, Embedding, EntityName},
    ports::{EmbeddingError, EmbeddingProvider, McpMemoryPort, VectorWrite},
};
use secrecy::SecretString;
use serde_json::{Value, json};
use tokio::{net::TcpListener, sync::Mutex};
use url::Url;

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
        model: "test-model".to_owned(),
        dimensions: Dimension::parse(2).expect("test dimension is valid"),
        max_input_chars: std::num::NonZeroU32::new(100).expect("non-zero chars"),
        max_input_tokens: std::num::NonZeroU32::new(100).expect("non-zero tokens"),
        openai_base_url: base_url,
        api_key: SecretString::from("openai-secret"),
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
                Some("vector_batch_upsert") => json!({"upserted": 1, "failed": []}),
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
