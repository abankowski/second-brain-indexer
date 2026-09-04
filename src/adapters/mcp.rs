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
        DeletionProof, EntityName, EntityType, GraphEntity, GraphRelation, GraphSnapshot,
        IncompleteSnapshotReason,
    },
    ports::{BatchWriteFailure, BatchWriteResult, McpError, McpMemoryPort, VectorWrite},
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
            .map_err(|_| McpError::Transport)?;
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
            .map_err(|_| McpError::Transport)?;
        parse_mcp_response(response).await
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
            .map_err(|_| McpError::Transport)?;
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
            .failed
            .into_iter()
            .map(|failure| {
                Ok(BatchWriteFailure {
                    entity_name: EntityName::parse(failure.entity_name)
                        .map_err(|_| McpError::InvalidResponse)?,
                    code: failure.code,
                })
            })
            .collect::<Result<Vec<_>, McpError>>()?;
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
}

async fn parse_mcp_response(response: reqwest::Response) -> Result<McpHttpResponse, McpError> {
    classify_status(response.status())?;
    let session_id = response
        .headers()
        .get("Mcp-Session-Id")
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned);
    let body = response.text().await.map_err(|_| McpError::Transport)?;
    let json = if body.trim_start().starts_with("data:") {
        parse_sse(&body)?
    } else {
        serde_json::from_str(&body).map_err(|_| McpError::InvalidResponse)?
    };
    Ok(McpHttpResponse { json, session_id })
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
    #[serde(default)]
    failed: Vec<BatchWriteFailureDto>,
}

#[derive(Deserialize)]
struct BatchWriteFailureDto {
    #[serde(rename = "entityName")]
    entity_name: String,
    code: String,
}

#[derive(Deserialize)]
struct DeletePayload {}

#[derive(Deserialize)]
struct VectorStoreStatsPayload {
    dims: u32,
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
