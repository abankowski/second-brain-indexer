//! Typed HTTP delivery boundary. It validates untrusted JSON and translates it
//! into the domain's closed selector and durable enqueue request.

use std::sync::Arc;

use async_trait::async_trait;
use axum::{
    Json, Router, body,
    extract::{Path, Request, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use time::{Duration, OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

use crate::{
    domain::model::{EntityName, EntityType, IdempotencyKey, RunId, RunTrigger, Selector},
    ports::{EnqueueOutcome, EnqueueRequest, IdempotencyRequest, McpMemoryPort, StateRepository},
    runtime::shutdown::Shutdown,
};

const MAX_REQUEST_BYTES: usize = 16 * 1024;

/// Read-only values supplied by runtime wiring. Keeping this separate from the
/// queue port means the HTTP boundary can be contract-tested with fakes.
#[async_trait]
pub trait HttpQueryPort: Send + Sync {
    async fn status(&self) -> Result<StatusView, HttpDependencyError>;
    async fn stats(&self) -> Result<StatsView, HttpDependencyError>;
    async fn run(&self, run_id: &RunId) -> Result<Option<RunView>, HttpDependencyError>;
    async fn metrics(&self) -> Result<String, HttpDependencyError>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatusView {
    pub ready: bool,
    pub version: String,
    pub active_generation: Option<GenerationView>,
    pub polling: PollingView,
    pub run: RunSummaryView,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GenerationView {
    pub taxonomy_version: String,
    pub representation_version: String,
    pub embedding_model: String,
    pub dimensions: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PollingView {
    pub enabled: bool,
    pub interval_seconds: u64,
    pub next_run_at: Option<OffsetDateTime>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunSummaryView {
    pub in_progress: bool,
    pub last_run_id: Option<RunId>,
    pub last_run_status: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StatsView {
    pub indexed: u64,
    pub pending: u64,
    pub indexing: u64,
    pub failed: u64,
    pub delete_pending: u64,
    pub deleted: u64,
    pub last_success_at: Option<String>,
    pub last_run_changes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunView {
    pub run_id: String,
    pub status: String,
    pub selector: SelectorResponse,
    pub requested_at: String,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub entities_seen: u64,
    pub entities_indexed: u64,
    pub entities_skipped: u64,
    pub entities_deleted: u64,
    pub entities_failed: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpDependencyError {
    Unavailable,
}

struct AppState<S, M, Q> {
    state: Arc<S>,
    mcp: Arc<M>,
    query: Arc<Q>,
    idempotency_ttl: Duration,
    shutdown: Option<Shutdown>,
}

impl<S, M, Q> Clone for AppState<S, M, Q> {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
            mcp: Arc::clone(&self.mcp),
            query: Arc::clone(&self.query),
            idempotency_ttl: self.idempotency_ttl,
            shutdown: self.shutdown.clone(),
        }
    }
}

pub fn router<S, M, Q>(
    state: Arc<S>,
    mcp: Arc<M>,
    query: Arc<Q>,
    idempotency_ttl: Duration,
) -> Router
where
    S: StateRepository + 'static,
    M: McpMemoryPort + 'static,
    Q: HttpQueryPort + 'static,
{
    router_internal(state, mcp, query, idempotency_ttl, None)
}

/// Production router variant. Once `shutdown.begin()` is called, new mutating
/// requests receive a deterministic 503 while read-only diagnostics remain
/// available until the server drains.
pub fn router_with_shutdown<S, M, Q>(
    state: Arc<S>,
    mcp: Arc<M>,
    query: Arc<Q>,
    idempotency_ttl: Duration,
    shutdown: Shutdown,
) -> Router
where
    S: StateRepository + 'static,
    M: McpMemoryPort + 'static,
    Q: HttpQueryPort + 'static,
{
    router_internal(state, mcp, query, idempotency_ttl, Some(shutdown))
}

fn router_internal<S, M, Q>(
    state: Arc<S>,
    mcp: Arc<M>,
    query: Arc<Q>,
    idempotency_ttl: Duration,
    shutdown: Option<Shutdown>,
) -> Router
where
    S: StateRepository + 'static,
    M: McpMemoryPort + 'static,
    Q: HttpQueryPort + 'static,
{
    let app_state = AppState {
        state,
        mcp,
        query,
        idempotency_ttl,
        shutdown,
    };
    Router::new()
        .route("/indexer/status", get(status::<S, M, Q>))
        .route("/indexer/stats", get(stats::<S, M, Q>))
        .route("/indexer/index", post(index::<S, M, Q>))
        .route("/indexer/fullscan", post(fullscan::<S, M, Q>))
        .route("/indexer/runs/{run_id}", get(run::<S, M, Q>))
        .route("/indexer/metrics", get(metrics::<S, M, Q>))
        .with_state(app_state)
}

async fn status<S, M, Q>(State(app): State<AppState<S, M, Q>>) -> Response
where
    S: StateRepository,
    M: McpMemoryPort,
    Q: HttpQueryPort,
{
    match app.query.status().await {
        Ok(view) => {
            let code = if view.ready {
                StatusCode::OK
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            };
            (code, Json(StatusResponse::from(view))).into_response()
        }
        Err(HttpDependencyError::Unavailable) => unavailable(),
    }
}

async fn stats<S, M, Q>(State(app): State<AppState<S, M, Q>>) -> Response
where
    S: StateRepository,
    M: McpMemoryPort,
    Q: HttpQueryPort,
{
    match app.query.stats().await {
        Ok(view) => Json(view).into_response(),
        Err(HttpDependencyError::Unavailable) => unavailable(),
    }
}

async fn run<S, M, Q>(State(app): State<AppState<S, M, Q>>, Path(run_id): Path<String>) -> Response
where
    S: StateRepository,
    M: McpMemoryPort,
    Q: HttpQueryPort,
{
    let run_id = match RunId::parse(run_id) {
        Ok(run_id) => run_id,
        Err(_) => return error(StatusCode::NOT_FOUND, "run_not_found", "run was not found"),
    };
    match app.query.run(&run_id).await {
        Ok(Some(view)) => Json(view).into_response(),
        Ok(None) => error(StatusCode::NOT_FOUND, "run_not_found", "run was not found"),
        Err(HttpDependencyError::Unavailable) => unavailable(),
    }
}

async fn metrics<S, M, Q>(State(app): State<AppState<S, M, Q>>) -> Response
where
    S: StateRepository,
    M: McpMemoryPort,
    Q: HttpQueryPort,
{
    match app.query.metrics().await {
        Ok(metrics) => (
            [(
                header::CONTENT_TYPE,
                "text/plain; version=0.0.4; charset=utf-8",
            )],
            metrics,
        )
            .into_response(),
        Err(HttpDependencyError::Unavailable) => unavailable(),
    }
}

async fn index<S, M, Q>(State(app): State<AppState<S, M, Q>>, request: Request) -> Response
where
    S: StateRepository,
    M: McpMemoryPort,
    Q: HttpQueryPort,
{
    let headers = request.headers().clone();
    let bytes = match body::to_bytes(request.into_body(), MAX_REQUEST_BYTES).await {
        Ok(value) => value,
        Err(_) => {
            return error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "request_too_large",
                "request body exceeds the size limit",
            );
        }
    };
    let payload = match serde_json::from_slice::<IndexRequest>(&bytes) {
        Ok(value) => value,
        Err(_) => {
            return error(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "request body must be valid JSON",
            );
        }
    };
    enqueue(app, headers, payload.selector, RunTrigger::Api).await
}

async fn fullscan<S, M, Q>(State(app): State<AppState<S, M, Q>>, headers: HeaderMap) -> Response
where
    S: StateRepository,
    M: McpMemoryPort,
    Q: HttpQueryPort,
{
    enqueue(
        app,
        headers,
        RequestSelector {
            full: Some(true),
            entity: None,
            entity_type: None,
        },
        RunTrigger::Fullscan,
    )
    .await
}

async fn enqueue<S, M, Q>(
    app: AppState<S, M, Q>,
    headers: HeaderMap,
    raw_selector: RequestSelector,
    trigger: RunTrigger,
) -> Response
where
    S: StateRepository,
    M: McpMemoryPort,
    Q: HttpQueryPort,
{
    if app
        .shutdown
        .as_ref()
        .is_some_and(|shutdown| !shutdown.is_accepting())
    {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "shutting_down",
            "service is shutting down",
        );
    }
    let selector = match raw_selector.into_domain() {
        Ok(selector) => selector,
        Err(message) => return error(StatusCode::BAD_REQUEST, "invalid_selector", message),
    };
    match target_exists(&*app.mcp, &selector).await {
        Ok(true) => {}
        Ok(false) => {
            return error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "target_not_found",
                "selector does not match the current graph",
            );
        }
        Err(_) => return unavailable(),
    }

    let requested_at = OffsetDateTime::now_utc();
    let run_id = match RunId::parse(Uuid::new_v4().to_string()) {
        Ok(value) => value,
        Err(_) => {
            return error(
                StatusCode::SERVICE_UNAVAILABLE,
                "unavailable",
                "service is unavailable",
            );
        }
    };
    let response = EnqueueResponse::new(run_id.clone(), &selector, requested_at, false);
    let idempotency = match idempotency_request(
        &headers,
        &selector,
        trigger,
        &response,
        requested_at,
        app.idempotency_ttl,
    ) {
        Ok(value) => value,
        Err(message) => return error(StatusCode::BAD_REQUEST, "invalid_idempotency_key", message),
    };
    match app
        .state
        .enqueue(EnqueueRequest {
            run_id,
            trigger,
            selector: selector.clone(),
            requested_at,
            idempotency,
        })
        .await
    {
        Ok(EnqueueOutcome::Queued { .. }) => (StatusCode::ACCEPTED, Json(response)).into_response(),
        Ok(EnqueueOutcome::Coalesced { run_id }) => (
            StatusCode::ACCEPTED,
            Json(EnqueueResponse::new(run_id, &selector, requested_at, true)),
        )
            .into_response(),
        Ok(EnqueueOutcome::IdempotentReplay {
            response_status,
            response_body_json,
            ..
        }) => replay(response_status, response_body_json),
        Ok(EnqueueOutcome::IdempotencyConflict) => error(
            StatusCode::CONFLICT,
            "idempotency_key_reused",
            "idempotency key was reused with a different request",
        ),
        Ok(EnqueueOutcome::Conflict) => error(
            StatusCode::CONFLICT,
            "run_in_progress",
            "request conflicts with the current queue",
        ),
        Err(_) => unavailable(),
    }
}

async fn target_exists<M: McpMemoryPort>(
    mcp: &M,
    selector: &Selector,
) -> Result<bool, crate::ports::McpError> {
    let snapshot = mcp.read_graph().await?;
    Ok(match selector {
        Selector::Full => true,
        Selector::Entity(name) => snapshot.entities.iter().any(|entity| &entity.name == name),
        Selector::EntityType(entity_type) => snapshot
            .entities
            .iter()
            .any(|entity| &entity.entity_type == entity_type),
    })
}

fn idempotency_request(
    headers: &HeaderMap,
    selector: &Selector,
    trigger: RunTrigger,
    response: &EnqueueResponse,
    now: OffsetDateTime,
    ttl: Duration,
) -> Result<Option<IdempotencyRequest>, &'static str> {
    let Some(value) = headers.get("Idempotency-Key") else {
        return Ok(None);
    };
    let value = value
        .to_str()
        .map_err(|_| "idempotency key must be valid text")?;
    let key =
        IdempotencyKey::parse(value.to_owned()).map_err(|_| "idempotency key must not be blank")?;
    let canonical = CanonicalRequest {
        trigger: trigger.as_str(),
        selector: SelectorResponse::from(selector),
    };
    let request_hash = sha256_json(&canonical)?;
    let response_body_json =
        serde_json::to_string(response).map_err(|_| "request could not be encoded")?;
    Ok(Some(IdempotencyRequest {
        key,
        request_hash,
        response_status: StatusCode::ACCEPTED.as_u16(),
        response_body_json,
        expires_at: now + ttl,
    }))
}

fn sha256_json(value: &impl Serialize) -> Result<String, &'static str> {
    let bytes = serde_json::to_vec(value).map_err(|_| "request could not be encoded")?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn replay(status: u16, body: String) -> Response {
    let status = StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    match serde_json::from_str::<serde_json::Value>(&body) {
        Ok(value) => (status, Json(value)).into_response(),
        Err(_) => error(
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable",
            "service is unavailable",
        ),
    }
}

fn unavailable() -> Response {
    error(
        StatusCode::SERVICE_UNAVAILABLE,
        "unavailable",
        "service is unavailable",
    )
}

fn error(status: StatusCode, code: &'static str, message: &'static str) -> Response {
    (
        status,
        Json(ErrorResponse {
            error: ErrorBody { code, message },
        }),
    )
        .into_response()
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IndexRequest {
    selector: RequestSelector,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EntityRequest {
    name: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RequestSelector {
    full: Option<bool>,
    entity: Option<EntityRequest>,
    #[serde(rename = "entityType")]
    entity_type: Option<String>,
}

impl RequestSelector {
    fn into_domain(self) -> Result<Selector, &'static str> {
        match (self.full, self.entity, self.entity_type) {
            (Some(true), None, None) => Ok(Selector::Full),
            (Some(false), None, None) => Err("full selector must be true"),
            (None, Some(entity), None) => EntityName::parse(entity.name)
                .map(Selector::Entity)
                .map_err(|_| "entity name must not be empty"),
            (None, None, Some(entity_type)) => EntityType::parse(entity_type)
                .map(Selector::EntityType)
                .map_err(|_| "entity type must not be blank"),
            _ => Err("selector must contain exactly one variant"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(untagged)]
pub enum SelectorResponse {
    Full {
        full: bool,
    },
    Entity {
        entity: EntityResponse,
    },
    EntityType {
        #[serde(rename = "entityType")]
        entity_type: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct EntityResponse {
    pub name: String,
}

impl From<&Selector> for SelectorResponse {
    fn from(value: &Selector) -> Self {
        match value {
            Selector::Full => Self::Full { full: true },
            Selector::Entity(name) => Self::Entity {
                entity: EntityResponse {
                    name: name.as_str().to_owned(),
                },
            },
            Selector::EntityType(entity_type) => Self::EntityType {
                entity_type: entity_type.as_str().to_owned(),
            },
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EnqueueResponse {
    run_id: String,
    status: &'static str,
    selector: SelectorResponse,
    requested_at: String,
    coalesced: bool,
}

impl EnqueueResponse {
    fn new(
        run_id: RunId,
        selector: &Selector,
        requested_at: OffsetDateTime,
        coalesced: bool,
    ) -> Self {
        Self {
            run_id: run_id.as_str().to_owned(),
            status: "queued",
            selector: SelectorResponse::from(selector),
            requested_at: timestamp(requested_at),
            coalesced,
        }
    }
}

#[derive(Serialize)]
struct CanonicalRequest<'a> {
    trigger: &'a str,
    selector: SelectorResponse,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StatusResponse {
    ready: bool,
    version: String,
    active_generation: Option<GenerationResponse>,
    polling: PollingResponse,
    run: RunSummaryResponse,
}

impl From<StatusView> for StatusResponse {
    fn from(value: StatusView) -> Self {
        Self {
            ready: value.ready,
            version: value.version,
            active_generation: value.active_generation.map(GenerationResponse::from),
            polling: PollingResponse {
                enabled: value.polling.enabled,
                interval_seconds: value.polling.interval_seconds,
                next_run_at: value.polling.next_run_at.map(timestamp),
            },
            run: RunSummaryResponse {
                in_progress: value.run.in_progress,
                last_run_id: value.run.last_run_id.map(|id| id.as_str().to_owned()),
                last_run_status: value.run.last_run_status,
            },
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct GenerationResponse {
    taxonomy_version: String,
    representation_version: String,
    embedding_model: String,
    dimensions: u32,
}
impl From<GenerationView> for GenerationResponse {
    fn from(value: GenerationView) -> Self {
        Self {
            taxonomy_version: value.taxonomy_version,
            representation_version: value.representation_version,
            embedding_model: value.embedding_model,
            dimensions: value.dimensions,
        }
    }
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PollingResponse {
    enabled: bool,
    interval_seconds: u64,
    next_run_at: Option<String>,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RunSummaryResponse {
    in_progress: bool,
    last_run_id: Option<String>,
    last_run_status: Option<String>,
}
#[derive(Serialize)]
struct ErrorResponse {
    error: ErrorBody,
}
#[derive(Serialize)]
struct ErrorBody {
    code: &'static str,
    message: &'static str,
}

fn timestamp(value: OffsetDateTime) -> String {
    value
        .format(&Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_owned())
}

#[cfg(test)]
mod tests {
    use super::{RequestSelector, SelectorResponse, sha256_json};
    use crate::domain::model::Selector;

    #[test]
    fn canonical_request_hash_distinguishes_selector_kinds() {
        let full = sha256_json(&SelectorResponse::from(&Selector::Full));
        let entity = sha256_json(&SelectorResponse::Entity {
            entity: super::EntityResponse {
                name: "full".to_owned(),
            },
        });
        assert!(matches!((full, entity), (Ok(left), Ok(right)) if left != right));
    }

    #[test]
    fn false_full_selector_is_rejected() {
        assert!(
            RequestSelector {
                full: Some(false),
                entity: None,
                entity_type: None
            }
            .into_domain()
            .is_err()
        );
    }
}
