use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use reqwest::{Client, StatusCode, header};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::Mutex;
use url::Url;

use crate::{
    config::McpConfig,
    domain::model::{
        DeletionProof, Embedding, EntityName, EntityType, GraphEntity, GraphRelation,
        GraphSnapshot, IncompleteSnapshotReason,
    },
    ports::{
        BatchWriteFailure, BatchWriteResult, HybridQueryResult, McpError, McpMemoryPort,
        SemanticQueryResult, VectorWrite,
    },
};

const MCP_PROTOCOL_VERSION: &str = "2025-03-26";

pub struct StreamableHttpMcpAdapter {
    client: Client,
    endpoint: Url,
    bearer_token: SecretString,
    session_id: Mutex<Option<String>>,
    request_id: AtomicU64,
}

impl StreamableHttpMcpAdapter {
    pub fn new(config: &McpConfig) -> Result<Self, McpError> {
        let client = Client::builder()
            .timeout(config.request_timeout)
            .build()
            .map_err(|error| {
                McpError::Transport(format!(
                    "could not create HTTP client: {}",
                    error.without_url()
                ))
            })?;
        Ok(Self {
            client,
            endpoint: config.endpoint.clone(),
            bearer_token: config.bearer_token.clone(),
            session_id: Mutex::new(None),
            request_id: AtomicU64::new(1),
        })
    }

    async fn tool_text(&self, name: &str, arguments: Value) -> Result<String, McpError> {
        self.initialize().await?;
        let id = self.next_request_id();
        let response = self
            .request(
                json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "method": "tools/call",
                    "params": {"name": name, "arguments": arguments},
                }),
                true,
            )
            .await?;
        response.text_result(id)
    }

    async fn initialize(&self) -> Result<(), McpError> {
        let mut session_id = self.session_id.lock().await;
        if session_id.is_some() {
            return Ok(());
        }

        let id = self.next_request_id();
        let response = self
            .request_with_session(
                json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "method": "initialize",
                    "params": {
                        "protocolVersion": MCP_PROTOCOL_VERSION,
                        "capabilities": {},
                        "clientInfo": {"name": "second-brain-indexer", "version": "0.1.0"},
                    },
                }),
                None,
            )
            .await?;
        response.validate_result(id)?;
        let new_session = response.session_id;

        self.notification_with_session(
            json!({"jsonrpc": "2.0", "method": "notifications/initialized"}),
            new_session.as_deref(),
        )
        .await?;
        *session_id = new_session;
        Ok(())
    }

    fn next_request_id(&self) -> u64 {
        self.request_id.fetch_add(1, Ordering::Relaxed)
    }

    async fn request(&self, body: Value, use_session: bool) -> Result<McpHttpResponse, McpError> {
        let session = if use_session {
            self.session_id.lock().await.clone()
        } else {
            None
        };
        self.request_with_session(body, session.as_deref()).await
    }

    async fn request_with_session(
        &self,
        body: Value,
        session_id: Option<&str>,
    ) -> Result<McpHttpResponse, McpError> {
        let response = self
            .base_request(session_id)
            .json(&body)
            .send()
            .await
            .map_err(|error| transport_error(&self.endpoint, error))?;
        parse_mcp_response(response, &self.endpoint).await
    }

    async fn notification_with_session(
        &self,
        body: Value,
        session_id: Option<&str>,
    ) -> Result<(), McpError> {
        let response = self
            .base_request(session_id)
            .json(&body)
            .send()
            .await
            .map_err(|error| transport_error(&self.endpoint, error))?;
        classify_status(response.status())?;
        Ok(())
    }

    fn base_request(&self, session_id: Option<&str>) -> reqwest::RequestBuilder {
        let request = self
            .client
            .post(self.endpoint.clone())
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", self.bearer_token.expose_secret()),
            )
            .header(header::ACCEPT, "application/json, text/event-stream")
            .header("MCP-Protocol-Version", MCP_PROTOCOL_VERSION);
        match session_id {
            Some(value) => request.header("Mcp-Session-Id", value),
            None => request,
        }
    }
}

#[async_trait]
impl McpMemoryPort for StreamableHttpMcpAdapter {
    async fn read_graph(&self) -> Result<GraphSnapshot, McpError> {
        let payload: GraphPayload =
            deserialize_tool_payload(self.tool_text("read_graph", json!({})).await?)?;
        let entities = payload
            .entities
            .into_iter()
            .map(GraphEntityDto::into_domain)
            .collect::<Result<Vec<_>, _>>()?;
        let relations = payload
            .relations
            .into_iter()
            .map(GraphRelationDto::into_domain)
            .collect::<Result<Vec<_>, _>>()?;
        let deletion_proof = if payload.complete == Some(true) {
            DeletionProof::Complete
        } else {
            DeletionProof::Unproven(IncompleteSnapshotReason::PaginationNotExhausted)
        };
        Ok(GraphSnapshot {
            entities,
            relations,
            deletion_proof,
        })
    }

    async fn upsert_batch(&self, items: &[VectorWrite]) -> Result<BatchWriteResult, McpError> {
        let request_items = items
            .iter()
            .map(|item| VectorWriteDto {
                entity_name: item.entity_name.as_str(),
                embedding: item.embedding.values(),
                model: &item.model,
            })
            .collect::<Vec<_>>();
        let payload: BatchWritePayload = deserialize_tool_payload(
            self.tool_text("vector_batch_upsert", json!({"items": request_items}))
                .await?,
        )?;
        let failed = payload
            .errors
            .into_iter()
            .map(|failure| {
                Ok(BatchWriteFailure {
                    entity_name: EntityName::parse(failure.entity_name)
                        .map_err(|_| McpError::InvalidResponse)?,
                    code: failure.code,
                })
            })
            .collect::<Result<Vec<_>, McpError>>()?;
        if payload.failed != u32::try_from(failed.len()).map_err(|_| McpError::InvalidResponse)? {
            return Err(McpError::InvalidResponse);
        }
        Ok(BatchWriteResult {
            upserted: payload.upserted,
            failed,
        })
    }

    async fn delete(&self, entity_name: &EntityName) -> Result<(), McpError> {
        let _: DeletePayload = deserialize_tool_payload(
            self.tool_text(
                "vector_delete_embedding",
                json!({"entityName": entity_name.as_str()}),
            )
            .await?,
        )?;
        Ok(())
    }

    async fn vector_dimension(&self) -> Result<u32, McpError> {
        let payload: VectorStoreStatsPayload =
            deserialize_tool_payload(self.tool_text("vector_store_stats", json!({})).await?)?;
        (payload.dims > 0)
            .then_some(payload.dims)
            .ok_or(McpError::InvalidResponse)
    }

    async fn semantic_search(
        &self,
        embedding: &Embedding,
        entity_type: Option<&EntityType>,
        limit: Option<u32>,
    ) -> Result<Vec<SemanticQueryResult>, McpError> {
        let payload: SemanticQueryPayload = deserialize_tool_payload(
            self.tool_text(
                "vector_search_entities",
                semantic_query_arguments(embedding, entity_type, limit)?,
            )
            .await?,
        )?;
        payload.into_domain()
    }

    async fn hybrid_search(
        &self,
        embedding: &Embedding,
        query_text: &str,
        limit: Option<u32>,
    ) -> Result<Vec<HybridQueryResult>, McpError> {
        let payload: HybridQueryPayload = deserialize_tool_payload(
            self.tool_text(
                "hybrid_search",
                hybrid_query_arguments(embedding, query_text, limit)?,
            )
            .await?,
        )?;
        payload.into_domain()
    }
}

fn checked_limit(limit: Option<u32>) -> Result<Option<u32>, McpError> {
    match limit {
        Some(value @ 1..=100) => Ok(Some(value)),
        Some(_) => Err(McpError::InvalidResponse),
        None => Ok(None),
    }
}

fn semantic_query_arguments(
    embedding: &Embedding,
    entity_type: Option<&EntityType>,
    limit: Option<u32>,
) -> Result<Value, McpError> {
    let mut arguments = serde_json::Map::new();
    arguments.insert("embedding".to_owned(), json!(embedding.values()));
    if let Some(entity_type) = entity_type {
        arguments.insert("entityType".to_owned(), json!(entity_type.as_str()));
    }
    if let Some(limit) = checked_limit(limit)? {
        arguments.insert("topK".to_owned(), json!(limit));
    }
    Ok(Value::Object(arguments))
}

fn hybrid_query_arguments(
    embedding: &Embedding,
    query_text: &str,
    limit: Option<u32>,
) -> Result<Value, McpError> {
    if query_text.trim().is_empty() {
        return Err(McpError::InvalidResponse);
    }
    let mut arguments = serde_json::Map::new();
    arguments.insert("queryEmbedding".to_owned(), json!(embedding.values()));
    arguments.insert("queryText".to_owned(), json!(query_text));
    if let Some(limit) = checked_limit(limit)? {
        arguments.insert("topK".to_owned(), json!(limit));
    }
    Ok(Value::Object(arguments))
}

async fn parse_mcp_response(
    response: reqwest::Response,
    endpoint: &Url,
) -> Result<McpHttpResponse, McpError> {
    classify_status(response.status())?;
    let session_id = response
        .headers()
        .get("Mcp-Session-Id")
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned);
    let body = response
        .text()
        .await
        .map_err(|error| transport_error(endpoint, error))?;
    let json = if body.trim_start().starts_with("data:") {
        parse_sse(&body)?
    } else {
        serde_json::from_str(&body).map_err(|_| McpError::InvalidResponse)?
    };
    Ok(McpHttpResponse { json, session_id })
}

fn transport_error(endpoint: &Url, error: reqwest::Error) -> McpError {
    let error = error.without_url();
    let category = if error.is_timeout() {
        "timed out while contacting"
    } else if error.is_connect() {
        "could not connect to"
    } else {
        "request failed for"
    };
    McpError::Transport(format!(
        "{category} MCP endpoint {}: {error}",
        safe_endpoint(endpoint)
    ))
}

fn safe_endpoint(endpoint: &Url) -> String {
    let mut safe = endpoint.clone();
    let _ = safe.set_username("");
    let _ = safe.set_password(None);
    safe.set_query(None);
    safe.set_fragment(None);
    safe.to_string()
}

fn parse_sse(body: &str) -> Result<Value, McpError> {
    body.lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .map(str::trim)
        .filter(|data| !data.is_empty())
        .filter_map(|data| serde_json::from_str::<Value>(data).ok())
        .last()
        .ok_or(McpError::InvalidResponse)
}

fn classify_status(status: StatusCode) -> Result<(), McpError> {
    if status.is_success() {
        return Ok(());
    }
    match status {
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => Err(McpError::Unauthorized),
        StatusCode::TOO_MANY_REQUESTS => Err(McpError::RateLimited),
        value if value.is_server_error() => Err(McpError::Server),
        _ => Err(McpError::InvalidResponse),
    }
}

fn deserialize_tool_payload<T: for<'de> Deserialize<'de>>(text: String) -> Result<T, McpError> {
    serde_json::from_str(&text).map_err(|_| McpError::InvalidResponse)
}

struct McpHttpResponse {
    json: Value,
    session_id: Option<String>,
}

impl McpHttpResponse {
    fn validate_result(&self, expected_id: u64) -> Result<Value, McpError> {
        let response: JsonRpcResponse =
            serde_json::from_value(self.json.clone()).map_err(|_| McpError::InvalidResponse)?;
        if response.jsonrpc != "2.0"
            || response.id != json!(expected_id)
            || response.error.is_some()
        {
            return Err(McpError::InvalidResponse);
        }
        response.result.ok_or(McpError::InvalidResponse)
    }

    fn text_result(&self, expected_id: u64) -> Result<String, McpError> {
        let result = self.validate_result(expected_id)?;
        let payload: ToolResult =
            serde_json::from_value(result).map_err(|_| McpError::InvalidResponse)?;
        if payload.is_error.unwrap_or(false) || payload.content.len() != 1 {
            return Err(McpError::InvalidResponse);
        }
        match payload.content.into_iter().next() {
            Some(Content::Text { text }) => Ok(text),
            None => Err(McpError::InvalidResponse),
        }
    }
}

#[derive(Deserialize)]
struct JsonRpcResponse {
    jsonrpc: String,
    id: Value,
    result: Option<Value>,
    error: Option<Value>,
}

#[derive(Deserialize)]
struct ToolResult {
    content: Vec<Content>,
    #[serde(rename = "isError")]
    is_error: Option<bool>,
}

#[derive(Deserialize)]
#[serde(tag = "type")]
enum Content {
    #[serde(rename = "text")]
    Text { text: String },
}

#[derive(Deserialize)]
struct GraphPayload {
    entities: Vec<GraphEntityDto>,
    relations: Vec<GraphRelationDto>,
    complete: Option<bool>,
}

#[derive(Deserialize)]
struct GraphEntityDto {
    name: String,
    #[serde(rename = "entityType")]
    entity_type: String,
    observations: Vec<String>,
}

impl GraphEntityDto {
    fn into_domain(self) -> Result<GraphEntity, McpError> {
        Ok(GraphEntity {
            name: EntityName::parse(self.name).map_err(|_| McpError::InvalidResponse)?,
            entity_type: EntityType::parse(self.entity_type)
                .map_err(|_| McpError::InvalidResponse)?,
            observations: self.observations,
        })
    }
}

#[derive(Deserialize)]
struct GraphRelationDto {
    from: String,
    #[serde(rename = "relationType")]
    relation_type: String,
    to: String,
}

impl GraphRelationDto {
    fn into_domain(self) -> Result<GraphRelation, McpError> {
        if self.relation_type.trim().is_empty() {
            return Err(McpError::InvalidResponse);
        }
        Ok(GraphRelation {
            from: EntityName::parse(self.from).map_err(|_| McpError::InvalidResponse)?,
            relation_type: self.relation_type,
            to: EntityName::parse(self.to).map_err(|_| McpError::InvalidResponse)?,
        })
    }
}

#[derive(Serialize)]
struct VectorWriteDto<'a> {
    #[serde(rename = "entityName")]
    entity_name: &'a str,
    embedding: &'a [f32],
    model: &'a str,
}

#[derive(Deserialize)]
struct BatchWritePayload {
    upserted: u32,
    failed: u32,
    #[serde(default)]
    errors: Vec<BatchWriteFailureDto>,
}

#[derive(Deserialize)]
struct BatchWriteFailureDto {
    #[serde(rename = "entityName")]
    entity_name: String,
    #[serde(rename = "error")]
    code: String,
}

#[derive(Deserialize)]
struct DeletePayload {}

#[derive(Deserialize)]
struct VectorStoreStatsPayload {
    dims: u32,
}

#[derive(Deserialize)]
struct SemanticQueryPayload {
    results: Vec<SemanticQueryResultDto>,
    count: u32,
}

impl SemanticQueryPayload {
    fn into_domain(self) -> Result<Vec<SemanticQueryResult>, McpError> {
        if self.count != u32::try_from(self.results.len()).map_err(|_| McpError::InvalidResponse)? {
            return Err(McpError::InvalidResponse);
        }
        self.results
            .into_iter()
            .map(SemanticQueryResultDto::into_domain)
            .collect()
    }
}

#[derive(Deserialize)]
struct SemanticQueryResultDto {
    name: String,
    #[serde(rename = "entityType")]
    entity_type: String,
    score: f64,
}

impl SemanticQueryResultDto {
    fn into_domain(self) -> Result<SemanticQueryResult, McpError> {
        if !self.score.is_finite() {
            return Err(McpError::InvalidResponse);
        }
        Ok(SemanticQueryResult {
            entity_name: EntityName::parse(self.name).map_err(|_| McpError::InvalidResponse)?,
            entity_type: EntityType::parse(self.entity_type)
                .map_err(|_| McpError::InvalidResponse)?,
            score: self.score,
        })
    }
}

#[derive(Deserialize)]
struct HybridQueryPayload {
    results: Vec<HybridQueryResultDto>,
    count: u32,
}

impl HybridQueryPayload {
    fn into_domain(self) -> Result<Vec<HybridQueryResult>, McpError> {
        if self.count != u32::try_from(self.results.len()).map_err(|_| McpError::InvalidResponse)? {
            return Err(McpError::InvalidResponse);
        }
        self.results
            .into_iter()
            .map(HybridQueryResultDto::into_domain)
            .collect()
    }
}

#[derive(Deserialize)]
struct HybridQueryResultDto {
    name: String,
    #[serde(rename = "entityType")]
    entity_type: String,
    score: f64,
    #[serde(rename = "textScore")]
    text_score: f64,
    #[serde(rename = "vecScore")]
    vec_score: f64,
}

impl HybridQueryResultDto {
    fn into_domain(self) -> Result<HybridQueryResult, McpError> {
        if !self.score.is_finite() || !self.text_score.is_finite() || !self.vec_score.is_finite() {
            return Err(McpError::InvalidResponse);
        }
        Ok(HybridQueryResult {
            entity_name: EntityName::parse(self.name).map_err(|_| McpError::InvalidResponse)?,
            entity_type: EntityType::parse(self.entity_type)
                .map_err(|_| McpError::InvalidResponse)?,
            score: self.score,
            text_score: self.text_score,
            vec_score: self.vec_score,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::parse_sse;

    #[test]
    fn parses_the_last_json_rpc_data_event_from_sse() {
        let value = parse_sse("event: message\ndata: {\"id\":1}\n\ndata: {\"id\":2}\n")
            .expect("SSE JSON-RPC payload parses");
        assert_eq!(value["id"], 2);
    }

    #[test]
    fn rejects_sse_without_a_json_rpc_data_event() {
        assert!(parse_sse("event: message\ndata: not-json\n").is_err());
    }
}
