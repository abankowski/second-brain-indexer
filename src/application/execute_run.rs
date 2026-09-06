use std::collections::{BTreeMap, BTreeSet};

use thiserror::Error;
use time::OffsetDateTime;

use crate::{
    application::retry::{RetryClass, classify_embedding, classify_mcp, retry_delay},
    config::{EmbeddingConfig, McpConfig, RepresentationConfig, RetryConfig},
    domain::{
        model::{ClaimedRun, EntityName, GraphEntity, WorkAction},
        planner::{PlannedAction, plan},
    },
    ports::{
        CompletedWork, EmbeddingError, EmbeddingProvider, FailedWork, McpError, McpMemoryPort,
        RunCompletion, RunCompletionStatus, StageWork, StateError, StateRepository, VectorWrite,
    },
};

#[derive(Debug, Error)]
pub enum ExecuteRunError {
    #[error("MCP vector dimension {actual} does not match configured dimension {expected}")]
    DimensionMismatch { expected: u32, actual: u32 },
    #[error("MCP operation failed: {0}")]
    Mcp(#[from] McpError),
    #[error("embedding operation failed: {0}")]
    Embedding(#[from] EmbeddingError),
    #[error("state operation failed: {0}")]
    State(#[from] StateError),
    #[error("planning failed: {0}")]
    Planner(#[from] crate::domain::planner::PlannerError),
    #[error("MCP batch response did not identify every item")]
    InvalidBatchResult,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionReport {
    pub indexed: u32,
    pub skipped: u32,
    pub deleted: u32,
    pub failed: u32,
}

pub struct RunExecutor<'a, M, E, S> {
    mcp: &'a M,
    embedding: &'a E,
    state: &'a S,
    mcp_config: &'a McpConfig,
    embedding_config: &'a EmbeddingConfig,
    representation_config: &'a RepresentationConfig,
    retry_config: &'a RetryConfig,
}

impl<'a, M, E, S> RunExecutor<'a, M, E, S>
where
    M: McpMemoryPort,
    E: EmbeddingProvider,
    S: StateRepository,
{
    pub fn new(
        mcp: &'a M,
        embedding: &'a E,
        state: &'a S,
        mcp_config: &'a McpConfig,
        embedding_config: &'a EmbeddingConfig,
        representation_config: &'a RepresentationConfig,
        retry_config: &'a RetryConfig,
    ) -> Self {
        Self {
            mcp,
            embedding,
            state,
            mcp_config,
            embedding_config,
            representation_config,
            retry_config,
        }
    }

    pub async fn execute(
        &self,
        claimed: ClaimedRun,
        now: OffsetDateTime,
    ) -> Result<ExecutionReport, ExecuteRunError> {
        let actual_dimension = self.mcp_dimension().await?;
        let expected_dimension = self.embedding_config.dimensions.get();
        if actual_dimension != expected_dimension {
            return self
                .finish_failure(
                    &claimed,
                    0,
                    format!(
                        "dimension mismatch: expected {expected_dimension}, got {actual_dimension}"
                    ),
                    now,
                    ExecuteRunError::DimensionMismatch {
                        expected: expected_dimension,
                        actual: actual_dimension,
                    },
                )
                .await;
        }

        let snapshot = self.read_graph().await?;
        let indexed = self.state.list_indexed_entities().await?;
        self.state
            .record_snapshot(
                &claimed.id,
                &claimed.lease,
                matches!(
                    snapshot.deletion_proof,
                    crate::domain::model::DeletionProof::Complete
                ),
                u32::try_from(snapshot.entities.len())
                    .map_err(|_| ExecuteRunError::InvalidBatchResult)?,
                u32::try_from(snapshot.relations.len())
                    .map_err(|_| ExecuteRunError::InvalidBatchResult)?,
                now,
            )
            .await?;
        let reconciliation = plan(
            &snapshot,
            &claimed.selector,
            &indexed,
            &crate::domain::canonical::RepresentationSettings {
                version: self.representation_config.version.clone(),
                max_input_chars: usize::try_from(self.embedding_config.max_input_chars.get())
                    .map_err(|_| ExecuteRunError::InvalidBatchResult)?,
            },
        )?;
        self.execute_plan(claimed, now, &snapshot.entities, reconciliation.actions)
            .await
    }

    async fn execute_plan(
        &self,
        claimed: ClaimedRun,
        now: OffsetDateTime,
        entities: &[GraphEntity],
        actions: Vec<PlannedAction>,
    ) -> Result<ExecutionReport, ExecuteRunError> {
        let entity_types = entities
            .iter()
            .map(|entity| (entity.name.clone(), entity.entity_type.as_str().to_owned()))
            .collect::<BTreeMap<_, _>>();
        let mut report = ExecutionReport {
            indexed: 0,
            skipped: 0,
            deleted: 0,
            failed: 0,
        };
        let mut upserts = Vec::new();
        for action in actions {
            match action {
                PlannedAction::Skip { .. } => report.skipped += 1,
                PlannedAction::Upsert {
                    entity_name,
                    document,
                } => {
                    self.state
                        .stage_work(StageWork {
                            run_id: claimed.id.clone(),
                            lease: claimed.lease.clone(),
                            entity_name: entity_name.clone(),
                            action: WorkAction::Upsert,
                            content_hash: Some(document.sha256.clone()),
                            vector_address: None,
                            staged_at: now,
                        })
                        .await?;
                    upserts.push(UpsertInput {
                        entity_name: entity_name.clone(),
                        entity_type: entity_types
                            .get(&entity_name)
                            .cloned()
                            .ok_or(ExecuteRunError::InvalidBatchResult)?,
                        content_hash: document.sha256,
                        text: document.text,
                    });
                }
                PlannedAction::Delete {
                    entity_name,
                    vector_address,
                } => {
                    self.state
                        .stage_work(StageWork {
                            run_id: claimed.id.clone(),
                            lease: claimed.lease.clone(),
                            entity_name: entity_name.clone(),
                            action: WorkAction::Delete,
                            content_hash: None,
                            vector_address: Some(vector_address.as_str().to_owned()),
                            staged_at: now,
                        })
                        .await?;
                    match self.delete(&vector_address).await {
                        Ok(()) => {
                            self.state
                                .complete_work(CompletedWork {
                                    run_id: claimed.id.clone(),
                                    lease: claimed.lease.clone(),
                                    entity_name,
                                    action: WorkAction::Delete,
                                    entity_type: None,
                                    content_hash: None,
                                    vector_address: Some(vector_address.as_str().to_owned()),
                                    completed_at: now,
                                })
                                .await?;
                            report.deleted += 1;
                        }
                        Err(error) => {
                            self.fail(&claimed, entity_name, WorkAction::Delete, &error)
                                .await?;
                            report.failed += 1;
                        }
                    }
                }
            }
        }
        for batch in upserts.chunks(usize::from(self.mcp_config.batch_size)) {
            let batch_report = self.embed_and_write(&claimed, now, batch.to_vec()).await?;
            report.indexed += batch_report.indexed;
            report.failed += batch_report.failed;
        }
        let status = if report.failed == 0 {
            RunCompletionStatus::Succeeded
        } else if report.indexed + report.deleted + report.skipped == 0 {
            RunCompletionStatus::Failed
        } else {
            RunCompletionStatus::Partial
        };
        self.state
            .finish_run(RunCompletion {
                run_id: claimed.id,
                lease: claimed.lease,
                status,
                entities_seen: u32::try_from(entities.len())
                    .map_err(|_| ExecuteRunError::InvalidBatchResult)?,
                entities_indexed: report.indexed,
                entities_skipped: report.skipped,
                entities_deleted: report.deleted,
                entities_failed: report.failed,
                error_summary: (report.failed != 0)
                    .then(|| "one or more work items failed".to_owned()),
                finished_at: now,
            })
            .await?;
        Ok(report)
    }

    async fn embed_and_write(
        &self,
        claimed: &ClaimedRun,
        now: OffsetDateTime,
        inputs: Vec<UpsertInput>,
    ) -> Result<ExecutionReport, ExecuteRunError> {
        let texts = inputs
            .iter()
            .map(|input| input.text.clone())
            .collect::<Vec<_>>();
        let embeddings = match self.embed(&texts).await {
            Ok(value) if value.len() == inputs.len() => value,
            Ok(_) => return Err(ExecuteRunError::Embedding(EmbeddingError::InvalidResponse)),
            Err(error) => {
                for input in &inputs {
                    self.fail(
                        claimed,
                        input.entity_name.clone(),
                        WorkAction::Upsert,
                        &ExecuteRunError::Embedding(error_kind(&error)),
                    )
                    .await?;
                }
                return Ok(ExecutionReport {
                    indexed: 0,
                    skipped: 0,
                    deleted: 0,
                    failed: u32::try_from(inputs.len())
                        .map_err(|_| ExecuteRunError::InvalidBatchResult)?,
                });
            }
        };
        let writes = inputs
            .iter()
            .zip(embeddings)
            .map(|(input, embedding)| VectorWrite {
                entity_name: input.entity_name.clone(),
                embedding,
                model: self.embedding_config.model.clone(),
            })
            .collect::<Vec<_>>();
        self.write_batch(claimed, now, inputs, writes).await
    }

    async fn write_batch(
        &self,
        claimed: &ClaimedRun,
        now: OffsetDateTime,
        inputs: Vec<UpsertInput>,
        writes: Vec<VectorWrite>,
    ) -> Result<ExecutionReport, ExecuteRunError> {
        let result = match self.upsert(&writes).await {
            Ok(value) => value,
            Err(error) => {
                for input in &inputs {
                    self.fail(
                        claimed,
                        input.entity_name.clone(),
                        WorkAction::Upsert,
                        &ExecuteRunError::Mcp(error_kind_mcp(&error)),
                    )
                    .await?;
                }
                return Ok(ExecutionReport {
                    indexed: 0,
                    skipped: 0,
                    deleted: 0,
                    failed: u32::try_from(inputs.len())
                        .map_err(|_| ExecuteRunError::InvalidBatchResult)?,
                });
            }
        };
        let failed = result
            .failed
            .iter()
            .map(|failure| failure.entity_name.clone())
            .collect::<BTreeSet<_>>();
        let known = inputs
            .iter()
            .map(|input| input.entity_name.clone())
            .collect::<BTreeSet<_>>();
        if !failed.is_subset(&known)
            || result.upserted
                != u32::try_from(inputs.len() - failed.len())
                    .map_err(|_| ExecuteRunError::InvalidBatchResult)?
        {
            return Err(ExecuteRunError::InvalidBatchResult);
        }
        let mut report = ExecutionReport {
            indexed: 0,
            skipped: 0,
            deleted: 0,
            failed: 0,
        };
        for input in inputs
            .iter()
            .filter(|input| !failed.contains(&input.entity_name))
        {
            self.complete_upsert(claimed, now, input).await?;
            report.indexed += 1;
        }
        if failed.is_empty() {
            return Ok(report);
        }
        let failed_inputs = inputs
            .into_iter()
            .zip(writes)
            .filter(|(input, _)| failed.contains(&input.entity_name))
            .collect::<Vec<_>>();
        if failed_inputs.len() == 1 {
            let (input, _) = failed_inputs
                .into_iter()
                .next()
                .ok_or(ExecuteRunError::InvalidBatchResult)?;
            let failure = result
                .failed
                .first()
                .ok_or(ExecuteRunError::InvalidBatchResult)?;
            self.fail(
                claimed,
                input.entity_name,
                WorkAction::Upsert,
                &ExecuteRunError::Mcp(McpError::InvalidResponse),
            )
            .await?;
            let _ = failure;
            report.failed += 1;
            return Ok(report);
        }
        let split_at = failed_inputs.len() / 2;
        let (left, right) = failed_inputs.split_at(split_at);
        let (left_inputs, left_writes): (Vec<_>, Vec<_>) = left.iter().cloned().unzip();
        let (right_inputs, right_writes): (Vec<_>, Vec<_>) = right.iter().cloned().unzip();
        let left_report =
            Box::pin(self.write_batch(claimed, now, left_inputs, left_writes)).await?;
        let right_report =
            Box::pin(self.write_batch(claimed, now, right_inputs, right_writes)).await?;
        report.indexed += left_report.indexed + right_report.indexed;
        report.failed += left_report.failed + right_report.failed;
        Ok(report)
    }

    async fn complete_upsert(
        &self,
        claimed: &ClaimedRun,
        now: OffsetDateTime,
        input: &UpsertInput,
    ) -> Result<(), ExecuteRunError> {
        self.state
            .complete_work(CompletedWork {
                run_id: claimed.id.clone(),
                lease: claimed.lease.clone(),
                entity_name: input.entity_name.clone(),
                action: WorkAction::Upsert,
                entity_type: Some(input.entity_type.clone()),
                content_hash: Some(input.content_hash.clone()),
                vector_address: Some(input.entity_name.as_str().to_owned()),
                completed_at: now,
            })
            .await?;
        Ok(())
    }

    async fn fail(
        &self,
        claimed: &ClaimedRun,
        entity_name: EntityName,
        action: WorkAction,
        error: &ExecuteRunError,
    ) -> Result<(), ExecuteRunError> {
        self.state
            .fail_work(FailedWork {
                run_id: claimed.id.clone(),
                lease: claimed.lease.clone(),
                entity_name,
                action,
                error_code: error_code(error).to_owned(),
                error_message: error.to_string(),
            })
            .await?;
        Ok(())
    }

    async fn read_graph(&self) -> Result<crate::domain::model::GraphSnapshot, ExecuteRunError> {
        retry_mcp(self.retry_config, 1, || self.mcp.read_graph())
            .await
            .map_err(ExecuteRunError::Mcp)
    }
    async fn mcp_dimension(&self) -> Result<u32, ExecuteRunError> {
        retry_mcp(self.retry_config, 2, || self.mcp.vector_dimension())
            .await
            .map_err(ExecuteRunError::Mcp)
    }
    async fn upsert(
        &self,
        writes: &[VectorWrite],
    ) -> Result<crate::ports::BatchWriteResult, McpError> {
        retry_mcp(
            self.retry_config,
            u64::try_from(writes.len()).unwrap_or(u64::MAX),
            || self.mcp.upsert_batch(writes),
        )
        .await
    }
    async fn delete(&self, name: &EntityName) -> Result<(), ExecuteRunError> {
        retry_mcp(self.retry_config, 3, || self.mcp.delete(name))
            .await
            .map_err(ExecuteRunError::Mcp)
    }
    async fn embed(
        &self,
        texts: &[String],
    ) -> Result<Vec<crate::domain::model::Embedding>, EmbeddingError> {
        retry_embedding(
            self.retry_config,
            u64::try_from(texts.len()).unwrap_or(u64::MAX),
            || self.embedding.embed(texts),
        )
        .await
    }

    async fn finish_failure<T>(
        &self,
        claimed: &ClaimedRun,
        seen: u32,
        message: String,
        now: OffsetDateTime,
        error: ExecuteRunError,
    ) -> Result<T, ExecuteRunError> {
        self.state
            .finish_run(RunCompletion {
                run_id: claimed.id.clone(),
                lease: claimed.lease.clone(),
                status: RunCompletionStatus::Failed,
                entities_seen: seen,
                entities_indexed: 0,
                entities_skipped: 0,
                entities_deleted: 0,
                entities_failed: 0,
                error_summary: Some(message),
                finished_at: now,
            })
            .await?;
        Err(error)
    }
}

#[derive(Clone)]
struct UpsertInput {
    entity_name: EntityName,
    entity_type: String,
    content_hash: String,
    text: String,
}

async fn retry_mcp<T, F, Fut>(
    config: &RetryConfig,
    basis: u64,
    mut operation: F,
) -> Result<T, McpError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, McpError>>,
{
    let attempts = config.max_attempts.get();
    for attempt in 1..=attempts {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(error) if classify_mcp(&error) == RetryClass::Retryable && attempt < attempts => {
                tokio::time::sleep(retry_delay(config, attempt, basis)).await
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("non-zero retry attempts always return")
}

async fn retry_embedding<T, F, Fut>(
    config: &RetryConfig,
    basis: u64,
    mut operation: F,
) -> Result<T, EmbeddingError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, EmbeddingError>>,
{
    let attempts = config.max_attempts.get();
    for attempt in 1..=attempts {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(error)
                if classify_embedding(&error) == RetryClass::Retryable && attempt < attempts =>
            {
                tokio::time::sleep(retry_delay(config, attempt, basis)).await
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("non-zero retry attempts always return")
}

fn error_kind(error: &EmbeddingError) -> EmbeddingError {
    match error {
        EmbeddingError::Transport => EmbeddingError::Transport,
        EmbeddingError::Unauthorized => EmbeddingError::Unauthorized,
        EmbeddingError::RateLimited => EmbeddingError::RateLimited,
        EmbeddingError::Server => EmbeddingError::Server,
        EmbeddingError::InvalidResponse => EmbeddingError::InvalidResponse,
    }
}
fn error_kind_mcp(error: &McpError) -> McpError {
    match error {
        McpError::Transport(detail) => McpError::Transport(detail.clone()),
        McpError::Unauthorized => McpError::Unauthorized,
        McpError::RateLimited => McpError::RateLimited,
        McpError::Server => McpError::Server,
        McpError::InvalidResponse => McpError::InvalidResponse,
    }
}
fn error_code(error: &ExecuteRunError) -> &'static str {
    match error {
        ExecuteRunError::DimensionMismatch { .. } => "dimension_mismatch",
        ExecuteRunError::Mcp(McpError::Unauthorized)
        | ExecuteRunError::Embedding(EmbeddingError::Unauthorized) => "unauthorized",
        ExecuteRunError::Mcp(McpError::RateLimited)
        | ExecuteRunError::Embedding(EmbeddingError::RateLimited) => "rate_limited",
        ExecuteRunError::Mcp(McpError::Transport(_))
        | ExecuteRunError::Embedding(EmbeddingError::Transport) => "transport",
        ExecuteRunError::Mcp(McpError::Server)
        | ExecuteRunError::Embedding(EmbeddingError::Server) => "server",
        _ => "invalid_response",
    }
}
