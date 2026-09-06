use async_trait::async_trait;
use thiserror::Error;
use time::OffsetDateTime;

use crate::domain::model::{
    ClaimedRun, DomainError, Embedding, EntityName, GraphSnapshot, IdempotencyKey,
    IndexedEntityState, Lease, RunId, RunTrigger, Selector, WorkAction,
};

#[derive(Clone, Debug)]
pub struct VectorWrite {
    pub entity_name: EntityName,
    pub embedding: Embedding,
    pub model: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BatchWriteResult {
    pub upserted: u32,
    pub failed: Vec<BatchWriteFailure>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BatchWriteFailure {
    pub entity_name: EntityName,
    pub code: String,
}

#[async_trait]
pub trait McpMemoryPort: Send + Sync {
    async fn read_graph(&self) -> Result<GraphSnapshot, McpError>;
    async fn upsert_batch(&self, items: &[VectorWrite]) -> Result<BatchWriteResult, McpError>;
    async fn delete(&self, entity_name: &EntityName) -> Result<(), McpError>;
    async fn vector_dimension(&self) -> Result<u32, McpError>;
}

#[async_trait]
pub trait EmbeddingProvider: Send + Sync {
    async fn embed(&self, inputs: &[String]) -> Result<Vec<Embedding>, EmbeddingError>;
}

#[async_trait]
pub trait StateRepository: Send + Sync {
    async fn enqueue(&self, request: EnqueueRequest) -> Result<EnqueueOutcome, StateError>;
    async fn claim_next(
        &self,
        owner: &str,
        now: OffsetDateTime,
    ) -> Result<Option<ClaimedRun>, StateError>;
    async fn renew_claim(
        &self,
        run_id: &RunId,
        lease: &Lease,
        now: OffsetDateTime,
    ) -> Result<bool, StateError>;
    async fn recover_expired_claims(&self, now: OffsetDateTime) -> Result<(), StateError>;
    async fn recover_after_exclusive_start(&self, now: OffsetDateTime) -> Result<(), StateError>;
    async fn list_indexed_entities(&self) -> Result<Vec<IndexedEntityState>, StateError>;
    async fn record_snapshot(
        &self,
        run_id: &RunId,
        lease: &Lease,
        complete: bool,
        entity_count: u32,
        relation_count: u32,
        now: OffsetDateTime,
    ) -> Result<(), StateError>;
    async fn stage_work(&self, work: StageWork) -> Result<(), StateError>;
    async fn complete_work(&self, work: CompletedWork) -> Result<(), StateError>;
    async fn fail_work(&self, work: FailedWork) -> Result<(), StateError>;
    async fn finish_run(&self, completion: RunCompletion) -> Result<(), StateError>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnqueueRequest {
    pub run_id: RunId,
    pub trigger: RunTrigger,
    pub selector: Selector,
    pub requested_at: OffsetDateTime,
    pub idempotency: Option<IdempotencyRequest>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IdempotencyRequest {
    pub key: IdempotencyKey,
    pub request_hash: String,
    pub response_status: u16,
    pub response_body_json: String,
    pub expires_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EnqueueOutcome {
    Queued {
        run_id: RunId,
    },
    Coalesced {
        run_id: RunId,
    },
    IdempotentReplay {
        run_id: RunId,
        response_status: u16,
        response_body_json: String,
    },
    IdempotencyConflict,
    Conflict,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StageWork {
    pub run_id: RunId,
    pub lease: Lease,
    pub entity_name: EntityName,
    pub action: WorkAction,
    pub content_hash: Option<String>,
    pub vector_address: Option<String>,
    pub staged_at: OffsetDateTime,
}

/// A state transition that is legal only after the corresponding MCP operation
/// has returned success. Embeddings deliberately never cross this boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompletedWork {
    pub run_id: RunId,
    pub lease: Lease,
    pub entity_name: EntityName,
    pub action: WorkAction,
    pub entity_type: Option<String>,
    pub content_hash: Option<String>,
    pub vector_address: Option<String>,
    pub completed_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FailedWork {
    pub run_id: RunId,
    pub lease: Lease,
    pub entity_name: EntityName,
    pub action: WorkAction,
    pub error_code: String,
    pub error_message: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RunCompletionStatus {
    Succeeded,
    Partial,
    Failed,
}

impl RunCompletionStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Partial => "partial",
            Self::Failed => "failed",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunCompletion {
    pub run_id: RunId,
    pub lease: Lease,
    pub status: RunCompletionStatus,
    pub entities_seen: u32,
    pub entities_indexed: u32,
    pub entities_skipped: u32,
    pub entities_deleted: u32,
    pub entities_failed: u32,
    pub error_summary: Option<String>,
    pub finished_at: OffsetDateTime,
}

#[derive(Debug, Error)]
pub enum McpError {
    #[error("MCP transport failed: {0}")]
    Transport(String),
    #[error("MCP authentication failed")]
    Unauthorized,
    #[error("MCP rate limited the request")]
    RateLimited,
    #[error("MCP server failed the request")]
    Server,
    #[error("MCP response was invalid")]
    InvalidResponse,
}

#[derive(Debug, Error)]
pub enum EmbeddingError {
    #[error("embedding provider transport failed")]
    Transport,
    #[error("embedding provider authentication failed")]
    Unauthorized,
    #[error("embedding provider rate limited the request")]
    RateLimited,
    #[error("embedding provider server failed the request")]
    Server,
    #[error("embedding provider returned an invalid response")]
    InvalidResponse,
}

#[derive(Debug, Error)]
pub enum StateError {
    #[error("state storage operation failed: {0}")]
    Storage(String),
    #[error("stored domain value is invalid: {0}")]
    Domain(String),
    #[error("lease owner must not be blank")]
    BlankLeaseOwner,
    #[error("claim changed before it could be leased")]
    ClaimRace,
    #[error("lease is no longer current")]
    LeaseLost,
    #[error("stored selector is invalid")]
    InvalidStoredSelector,
    #[error("delete work requires the stored vector address")]
    MissingVectorAddress,
    #[error("database path must have a parent directory")]
    MissingDatabaseParent,
    #[error("timestamp is out of range")]
    TimestampOutOfRange,
}

impl StateError {
    pub fn storage(error: impl std::fmt::Display) -> Self {
        Self::Storage(error.to_string())
    }

    pub fn domain(error: DomainError) -> Self {
        Self::Domain(error.to_string())
    }
}
