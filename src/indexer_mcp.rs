//! Session-free MCP 2025-03-26 delivery. Only normalized tool results cross this
//! boundary; query embeddings and dependency errors remain server-side.

use std::sync::Arc;

use axum::{
    Json, Router, body,
    extract::{Request, State},
    http::{HeaderMap, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::post,
};
use serde::Deserialize;
use serde_json::{Value, json};
use time::Duration;

use crate::{
    domain::model::{Embedding, EntityName, EntityType, RunId, RunTrigger, Selector},
    http::{AppState, BearerAuth, HttpQueryPort, enqueue_selector, require_bearer},
    ports::{EmbeddingProvider, McpMemoryPort, StateRepository},
    runtime::shutdown::Shutdown,
};

const MAX_REQUEST_BYTES: usize = 16 * 1024;
const PROTOCOL_VERSION: &str = "2025-03-26";

struct McpState<S, M, Q> {
    app: AppState<S, M, Q>,
    embedding: Arc<dyn EmbeddingProvider>,
}

impl<S, M, Q> Clone for McpState<S, M, Q> {
    fn clone(&self) -> Self {
        Self {
            app: self.app.clone(),
            embedding: Arc::clone(&self.embedding),
        }
    }
}

/// Mount alongside the REST router with the same bearer policy, durable queue,
/// query port and selected embedding provider. No per-client session is stored.
pub fn indexer_mcp_router<S, M, Q>(
    state: Arc<S>,
    mcp: Arc<M>,
    query: Arc<Q>,
    embedding: Arc<dyn EmbeddingProvider>,
    idempotency_ttl: Duration,
    shutdown: Shutdown,
    auth: BearerAuth,
) -> Router
where
    S: StateRepository + 'static,
    M: McpMemoryPort + 'static,
    Q: HttpQueryPort + 'static,
{
    Router::new()
        .route("/indexer/mcp", post(handle::<S, M, Q>))
        .layer(middleware::from_fn(validate_origin))
        .layer(middleware::from_fn_with_state(auth, require_bearer))
        .with_state(McpState {
            app: AppState {
                state,
                mcp,
                query,
                idempotency_ttl,
                shutdown: Some(shutdown),
            },
            embedding,
        })
}

async fn handle<S, M, Q>(State(state): State<McpState<S, M, Q>>, request: Request) -> Response
where
    S: StateRepository,
    M: McpMemoryPort,
    Q: HttpQueryPort,
{
    if !accepts_json_and_sse(request.headers()) {
        return StatusCode::NOT_ACCEPTABLE.into_response();
    }
    let content_type = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next());
    if content_type != Some("application/json") {
        return StatusCode::UNSUPPORTED_MEDIA_TYPE.into_response();
    }
    let bytes = match body::to_bytes(request.into_body(), MAX_REQUEST_BYTES).await {
        Ok(bytes) => bytes,
        Err(_) => return StatusCode::PAYLOAD_TOO_LARGE.into_response(),
    };
    let value: Value = match serde_json::from_slice(&bytes) {
        Ok(value) => value,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(rpc_error(Value::Null, -32700, "Parse error")),
            )
                .into_response();
        }
    };
    match value {
        Value::Array(messages) if !messages.is_empty() => {
            let mut responses = Vec::new();
            for message in messages {
                if let Some(response) = dispatch(&state, message).await {
                    responses.push(response);
                }
            }
            if responses.is_empty() {
                StatusCode::ACCEPTED.into_response()
            } else {
                Json(responses).into_response()
            }
        }
        value => match dispatch(&state, value).await {
            Some(response) => Json(response).into_response(),
            None => StatusCode::ACCEPTED.into_response(),
        },
    }
}

async fn validate_origin(request: Request, next: Next) -> Response {
    // This server-to-server endpoint has no browser-origin allowlist. Rejecting
    // every supplied Origin also protects configurations with disabled auth.
    if request.headers().contains_key(header::ORIGIN) {
        StatusCode::FORBIDDEN.into_response()
    } else {
        next.run(request).await
    }
}

fn accepts_json_and_sse(headers: &HeaderMap) -> bool {
    let media: Vec<_> = headers
        .get_all(header::ACCEPT)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .filter_map(|value| value.split(';').next())
        .map(str::trim)
        .collect();
    media.contains(&"application/json") && media.contains(&"text/event-stream")
}

async fn dispatch<S, M, Q>(state: &McpState<S, M, Q>, message: Value) -> Option<Value>
where
    S: StateRepository,
    M: McpMemoryPort,
    Q: HttpQueryPort,
{
    let invalid = || Some(rpc_error(Value::Null, -32600, "Invalid Request"));
    let Some(object) = message.as_object() else {
        return invalid();
    };
    if object.get("jsonrpc") != Some(&json!("2.0")) {
        return invalid();
    }
    if is_client_response(object) {
        // MCP clients may POST responses to server-initiated requests. Streamable
        // HTTP accepts a body containing only such responses with 202 and no body.
        return None;
    }
    let Some(method) = object.get("method").and_then(Value::as_str) else {
        return invalid();
    };
    let params = object.get("params").cloned().unwrap_or_else(|| json!({}));
    if !params.is_object() {
        return invalid();
    }
    let Some(id) = object.get("id") else {
        // Notifications never invoke tools and never receive JSON-RPC replies.
        return None;
    };
    if !is_valid_id(id) {
        return invalid();
    }
    let result = match method {
        "initialize" => {
            if serde_json::from_value::<InitializeParams>(params).is_err() {
                return Some(rpc_error(id.clone(), -32602, "Invalid params"));
            }
            json!({"protocolVersion":PROTOCOL_VERSION,"capabilities":{"tools":{"listChanged":false}},"serverInfo":{"name":"second-brain-indexer","version":env!("CARGO_PKG_VERSION")}})
        }
        "ping" => json!({}),
        "tools/list" => json!({"tools":tool_definitions()}),
        "tools/call" => match serde_json::from_value::<ToolCall>(params) {
            Ok(call) => {
                if !TOOL_NAMES.contains(&call.name.as_str()) {
                    return Some(rpc_error(id.clone(), -32602, "Unknown tool"));
                }
                match call_tool(state, call).await {
                    Ok(value) => tool_result(value, false),
                    Err(code) => tool_result(
                        json!({"error":{"code":code,"message":error_message(code)}}),
                        true,
                    ),
                }
            }
            Err(_) => return Some(rpc_error(id.clone(), -32602, "Invalid params")),
        },
        _ => return Some(rpc_error(id.clone(), -32601, "Method not found")),
    };
    Some(json!({"jsonrpc":"2.0","id":id,"result":result}))
}

fn is_client_response(object: &serde_json::Map<String, Value>) -> bool {
    if object.contains_key("method") || object.contains_key("params") {
        return false;
    }
    let Some(id) = object.get("id") else {
        return false;
    };
    if !is_valid_id(id) {
        return false;
    }
    match (object.get("result"), object.get("error")) {
        (Some(_), None) => true,
        (None, Some(error)) => is_valid_error(error),
        _ => false,
    }
}

fn is_valid_id(id: &Value) -> bool {
    id.is_string() || id.is_i64() || id.is_u64()
}

fn is_valid_error(error: &Value) -> bool {
    let Some(error) = error.as_object() else {
        return false;
    };
    error
        .get("code")
        .is_some_and(|code| code.is_i64() || code.is_u64())
        && error.get("message").is_some_and(Value::is_string)
}

#[derive(Deserialize)]
struct InitializeParams {
    #[serde(rename = "protocolVersion")]
    _protocol_version: String,
    #[serde(rename = "capabilities")]
    _capabilities: serde_json::Map<String, Value>,
    #[serde(rename = "clientInfo")]
    _client_info: ClientInfo,
}

#[derive(Deserialize)]
struct ClientInfo {
    #[serde(rename = "name")]
    _name: String,
    #[serde(rename = "version")]
    _version: String,
}

#[derive(Deserialize)]
struct ToolCall {
    name: String,
    #[serde(default = "empty_arguments")]
    arguments: Value,
}

fn empty_arguments() -> Value {
    json!({})
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct SearchArgs {
    query: String,
    limit: Option<u32>,
    entity_type: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct EntityArgs {
    entity_name: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct RunArgs {
    run_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EmptyArgs {}

async fn call_tool<S, M, Q>(
    state: &McpState<S, M, Q>,
    call: ToolCall,
) -> Result<Value, &'static str>
where
    S: StateRepository,
    M: McpMemoryPort,
    Q: HttpQueryPort,
{
    match call.name.as_str() {
        "indexer_semantic_search" | "indexer_hybrid_search" => {
            if state
                .app
                .shutdown
                .as_ref()
                .is_some_and(|shutdown| !shutdown.is_accepting())
            {
                return Err("shutting_down");
            }
            let args: SearchArgs =
                serde_json::from_value(call.arguments).map_err(|_| "invalid_arguments")?;
            if args.query.trim().is_empty()
                || args.limit.is_some_and(|limit| !(1..=100).contains(&limit))
            {
                return Err("invalid_arguments");
            }
            let entity_type = args
                .entity_type
                .map(EntityType::parse)
                .transpose()
                .map_err(|_| "invalid_arguments")?;
            let embedding = embed_query(&*state.embedding, &args.query).await?;
            if call.name == "indexer_semantic_search" {
                let rows = state
                    .app
                    .mcp
                    .semantic_search(&embedding, entity_type.as_ref(), args.limit)
                    .await
                    .map_err(|_| "unavailable")?;
                Ok(
                    json!({"results":rows.into_iter().map(|row| json!({"entityName":row.entity_name.as_str(),"entityType":row.entity_type.as_str(),"score":row.score})).collect::<Vec<_>>()}),
                )
            } else {
                let rows = state
                    .app
                    .mcp
                    .hybrid_search(&embedding, &args.query, args.limit)
                    .await
                    .map_err(|_| "unavailable")?;
                Ok(
                    json!({"results":rows.into_iter().filter(|row| entity_type.as_ref().is_none_or(|kind| kind == &row.entity_type)).map(|row| json!({"entityName":row.entity_name.as_str(),"entityType":row.entity_type.as_str(),"score":row.score,"textScore":row.text_score,"vecScore":row.vec_score})).collect::<Vec<_>>()}),
                )
            }
        }
        "indexer_reindex_entity" => {
            let args: EntityArgs =
                serde_json::from_value(call.arguments).map_err(|_| "invalid_arguments")?;
            let name = EntityName::parse(args.entity_name).map_err(|_| "invalid_arguments")?;
            enqueue_tool(state, Selector::Entity(name), RunTrigger::Api).await
        }
        "indexer_reindex_all" => {
            serde_json::from_value::<EmptyArgs>(call.arguments).map_err(|_| "invalid_arguments")?;
            enqueue_tool(state, Selector::Full, RunTrigger::Fullscan).await
        }
        "indexer_run_status" => {
            let args: RunArgs =
                serde_json::from_value(call.arguments).map_err(|_| "invalid_arguments")?;
            let run_id = RunId::parse(args.run_id).map_err(|_| "invalid_arguments")?;
            let run = state
                .app
                .query
                .run(&run_id)
                .await
                .map_err(|_| "unavailable")?
                .ok_or("run_not_found")?;
            serde_json::to_value(run).map_err(|_| "unavailable")
        }
        _ => Err("invalid_arguments"),
    }
}

async fn embed_query(
    provider: &dyn EmbeddingProvider,
    query: &str,
) -> Result<Embedding, &'static str> {
    let embeddings = provider
        .embed(&[query.to_owned()])
        .await
        .map_err(|_| "unavailable")?;
    if embeddings.len() != 1 {
        return Err("unavailable");
    }
    embeddings.into_iter().next().ok_or("unavailable")
}

async fn enqueue_tool<S, M, Q>(
    state: &McpState<S, M, Q>,
    selector: Selector,
    trigger: RunTrigger,
) -> Result<Value, &'static str>
where
    S: StateRepository,
    M: McpMemoryPort,
    Q: HttpQueryPort,
{
    // No client-provided idempotency payload can become an MCP response. The
    // existing durable queue performs active-run coalescing for both selectors.
    let response = enqueue_selector(state.app.clone(), HeaderMap::new(), selector, trigger).await;
    let status = response.status();
    let bytes = body::to_bytes(response.into_body(), MAX_REQUEST_BYTES)
        .await
        .map_err(|_| "unavailable")?;
    let value: Value = serde_json::from_slice(&bytes).map_err(|_| "unavailable")?;
    if status.is_success() {
        return Ok(value);
    }
    match value["error"]["code"].as_str() {
        Some("shutting_down") => Err("shutting_down"),
        Some("target_not_found") => Err("target_not_found"),
        Some("run_in_progress") => Err("run_in_progress"),
        _ => Err("unavailable"),
    }
}

fn rpc_error(id: Value, code: i32, message: &'static str) -> Value {
    json!({"jsonrpc":"2.0","id":id,"error":{"code":code,"message":message}})
}

fn tool_result(value: Value, is_error: bool) -> Value {
    json!({"content":[{"type":"text","text":value.to_string()}],"isError":is_error})
}

fn error_message(code: &str) -> &'static str {
    match code {
        "invalid_arguments" => "tool arguments are invalid",
        "shutting_down" => "service is shutting down",
        "target_not_found" => "entity was not found",
        "run_not_found" => "run was not found",
        "run_in_progress" => "request conflicts with the current queue",
        _ => "service is unavailable",
    }
}

const TOOL_NAMES: [&str; 5] = [
    "indexer_semantic_search",
    "indexer_hybrid_search",
    "indexer_reindex_entity",
    "indexer_reindex_all",
    "indexer_run_status",
];

fn tool_definitions() -> Vec<Value> {
    let search_properties = json!({"query":{"type":"string","minLength":1},"limit":{"type":"integer","minimum":1,"maximum":100},"entityType":{"type":"string","minLength":1}});
    vec![
        tool_definition(
            TOOL_NAMES[0],
            "Search entity vectors using a natural-language query embedded server-side.",
            search_properties.clone(),
            json!(["query"]),
            true,
        ),
        tool_definition(
            TOOL_NAMES[1],
            "Search text and vectors using a natural-language query. entityType filters the returned top results, so fewer than limit may remain.",
            search_properties,
            json!(["query"]),
            true,
        ),
        tool_definition(
            TOOL_NAMES[2],
            "Queue durable reindexing of one exact entity name.",
            json!({"entityName":{"type":"string","minLength":1}}),
            json!(["entityName"]),
            false,
        ),
        tool_definition(
            TOOL_NAMES[3],
            "Queue durable full graph reindexing.",
            json!({}),
            json!([]),
            false,
        ),
        tool_definition(
            TOOL_NAMES[4],
            "Read durable run status and counters.",
            json!({"runId":{"type":"string","minLength":1}}),
            json!(["runId"]),
            true,
        ),
    ]
}

fn tool_definition(
    name: &str,
    description: &str,
    properties: Value,
    required: Value,
    read_only: bool,
) -> Value {
    json!({"name":name,"description":description,"inputSchema":{"type":"object","properties":properties,"required":required,"additionalProperties":false},"annotations":{"readOnlyHint":read_only}})
}
