use std::{path::Path, str::FromStr, time::Duration};

use async_trait::async_trait;
use sqlx::{
    Row, SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use crate::{
    domain::model::{
        ClaimedRun, EntityName, EntityType, IndexedEntityState, Lease, RunId, Selector, WorkAction,
    },
    ports::{
        CompletedWork, EnqueueOutcome, EnqueueRequest, FailedWork, RunCompletion, StageWork,
        StateError, StateRepository,
    },
};

pub struct SqliteStateRepository {
    pool: SqlitePool,
    lease_duration: Duration,
}

impl SqliteStateRepository {
    pub async fn connect(path: &Path, lease_duration: Duration) -> Result<Self, StateError> {
        create_private_parent(path)?;
        let options = SqliteConnectOptions::from_str(&format!("sqlite://{}", path.display()))
            .map_err(StateError::storage)?
            .create_if_missing(true)
            .foreign_keys(true)
            .busy_timeout(Duration::from_secs(5))
            .journal_mode(SqliteJournalMode::Wal);
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(options)
            .await
            .map_err(StateError::storage)?;
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .map_err(StateError::storage)?;
        set_private_file_permissions(path)?;
        Ok(Self {
            pool,
            lease_duration,
        })
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    async fn begin_immediate(
        &self,
    ) -> Result<sqlx::pool::PoolConnection<sqlx::Sqlite>, StateError> {
        let mut connection = self.pool.acquire().await.map_err(StateError::storage)?;
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *connection)
            .await
            .map_err(StateError::storage)?;
        Ok(connection)
    }
}

#[async_trait]
impl StateRepository for SqliteStateRepository {
    async fn enqueue(&self, request: EnqueueRequest) -> Result<EnqueueOutcome, StateError> {
        let mut connection = self.begin_immediate().await?;
        let now = timestamp(request.requested_at)?;
        sqlx::query("DELETE FROM idempotency_key WHERE expires_at <= ?")
            .bind(&now)
            .execute(&mut *connection)
            .await
            .map_err(StateError::storage)?;

        if let Some(idempotency) = &request.idempotency {
            let existing = sqlx::query(
                "SELECT request_hash, run_id, response_status, response_body_json FROM idempotency_key WHERE key = ?",
            )
            .bind(idempotency.key.as_str())
            .fetch_optional(&mut *connection)
            .await
            .map_err(StateError::storage)?;
            if let Some(row) = existing {
                let outcome = if row.get::<String, _>("request_hash") == idempotency.request_hash {
                    EnqueueOutcome::IdempotentReplay {
                        run_id: parse_run_id(row.get("run_id"))?,
                        response_status: row.get::<i64, _>("response_status") as u16,
                        response_body_json: row.get("response_body_json"),
                    }
                } else {
                    EnqueueOutcome::IdempotencyConflict
                };
                commit(&mut connection).await?;
                return Ok(outcome);
            }
        }

        let selector_kind = request.selector.kind().as_str();
        let selector_value = request.selector.value();
        let coalesced = sqlx::query(
            "SELECT id FROM run WHERE queue_state = 'queued' AND selector_kind = ? AND selector_value IS ? ORDER BY requested_at LIMIT 1",
        )
        .bind(selector_kind)
        .bind(selector_value)
        .fetch_optional(&mut *connection)
        .await
        .map_err(StateError::storage)?;

        let outcome = if let Some(row) = coalesced {
            EnqueueOutcome::Coalesced {
                run_id: parse_run_id(row.get("id"))?,
            }
        } else {
            let queued_run_exists =
                sqlx::query("SELECT 1 FROM run WHERE queue_state = 'queued' LIMIT 1")
                    .fetch_optional(&mut *connection)
                    .await
                    .map_err(StateError::storage)?
                    .is_some();
            if queued_run_exists {
                EnqueueOutcome::Conflict
            } else {
                insert_run(&mut connection, &request, &now).await?;
                EnqueueOutcome::Queued {
                    run_id: request.run_id.clone(),
                }
            }
        };

        if let Some(idempotency) = &request.idempotency {
            if let EnqueueOutcome::Queued { run_id } | EnqueueOutcome::Coalesced { run_id } =
                &outcome
            {
                sqlx::query(
                    "INSERT INTO idempotency_key (key, request_hash, run_id, response_status, response_body_json, created_at, expires_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                )
                .bind(idempotency.key.as_str())
                .bind(&idempotency.request_hash)
                .bind(run_id.as_str())
                .bind(i64::from(idempotency.response_status))
                .bind(&idempotency.response_body_json)
                .bind(&now)
                .bind(timestamp(idempotency.expires_at)?)
                .execute(&mut *connection)
                .await
                .map_err(StateError::storage)?;
            }
        }
        commit(&mut connection).await?;
        Ok(outcome)
    }

    async fn claim_next(
        &self,
        owner: &str,
        now: OffsetDateTime,
    ) -> Result<Option<ClaimedRun>, StateError> {
        if owner.trim().is_empty() {
            return Err(StateError::BlankLeaseOwner);
        }
        let mut connection = self.begin_immediate().await?;
        let now_text = timestamp(now)?;
        let row = sqlx::query(
            "SELECT id, selector_kind, selector_value, lease_epoch FROM run WHERE queue_state = 'queued' AND not_before <= ? ORDER BY requested_at, id LIMIT 1",
        )
        .bind(&now_text)
        .fetch_optional(&mut *connection)
        .await
        .map_err(StateError::storage)?;
        let Some(row) = row else {
            commit(&mut connection).await?;
            return Ok(None);
        };
        let id = parse_run_id(row.get("id"))?;
        let epoch = row.get::<i64, _>("lease_epoch") + 1;
        let expires_at = now
            .checked_add(
                time::Duration::try_from(self.lease_duration).map_err(StateError::storage)?,
            )
            .ok_or(StateError::TimestampOutOfRange)?;
        let changed = sqlx::query(
            "UPDATE run SET queue_state = 'leased', status = 'running', started_at = COALESCE(started_at, ?), lease_owner = ?, lease_epoch = ?, lease_expires_at = ?, claim_count = claim_count + 1 WHERE id = ? AND queue_state = 'queued'",
        )
        .bind(&now_text)
        .bind(owner)
        .bind(epoch)
        .bind(timestamp(expires_at)?)
        .bind(id.as_str())
        .execute(&mut *connection)
        .await
        .map_err(StateError::storage)?
        .rows_affected();
        if changed != 1 {
            rollback(&mut connection).await;
            return Err(StateError::ClaimRace);
        }
        let selector = selector_from_row(&row)?;
        commit(&mut connection).await?;
        Ok(Some(ClaimedRun {
            id,
            selector,
            lease: Lease {
                owner: owner.to_owned(),
                epoch,
            },
        }))
    }

    async fn renew_claim(
        &self,
        run_id: &RunId,
        lease: &Lease,
        now: OffsetDateTime,
    ) -> Result<bool, StateError> {
        let expires_at = now
            .checked_add(
                time::Duration::try_from(self.lease_duration).map_err(StateError::storage)?,
            )
            .ok_or(StateError::TimestampOutOfRange)?;
        let changed = sqlx::query(
            "UPDATE run SET lease_expires_at = ? WHERE id = ? AND queue_state = 'leased' AND lease_owner = ? AND lease_epoch = ?",
        )
        .bind(timestamp(expires_at)?)
        .bind(run_id.as_str())
        .bind(&lease.owner)
        .bind(lease.epoch)
        .execute(&self.pool)
        .await
        .map_err(StateError::storage)?
        .rows_affected();
        Ok(changed == 1)
    }

    async fn recover_expired_claims(&self, now: OffsetDateTime) -> Result<(), StateError> {
        self.recover_leases(now, true).await
    }

    async fn recover_after_exclusive_start(&self, now: OffsetDateTime) -> Result<(), StateError> {
        self.recover_leases(now, false).await
    }

    async fn list_indexed_entities(&self) -> Result<Vec<IndexedEntityState>, StateError> {
        let rows = sqlx::query(
            "SELECT entity_name, content_hash, vector_address FROM entity_index_state WHERE status = 'indexed' ORDER BY entity_name",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(StateError::storage)?;
        rows.into_iter()
            .map(|row| {
                Ok(IndexedEntityState {
                    entity_name: EntityName::parse(row.get("entity_name"))
                        .map_err(StateError::domain)?,
                    content_hash: row.get("content_hash"),
                    vector_address: EntityName::parse(row.get("vector_address"))
                        .map_err(StateError::domain)?,
                })
            })
            .collect()
    }

    async fn record_snapshot(
        &self,
        run_id: &RunId,
        lease: &Lease,
        complete: bool,
        entity_count: u32,
        relation_count: u32,
        now: OffsetDateTime,
    ) -> Result<(), StateError> {
        let mut connection = self.begin_immediate().await?;
        ensure_lease(&mut connection, run_id, lease).await?;
        sqlx::query(
            "INSERT INTO run_snapshot (run_id, is_complete, entity_count, relation_count, verified_at) VALUES (?, ?, ?, ?, ?) ON CONFLICT(run_id) DO UPDATE SET is_complete = excluded.is_complete, entity_count = excluded.entity_count, relation_count = excluded.relation_count, verified_at = excluded.verified_at",
        )
        .bind(run_id.as_str())
        .bind(if complete { 1_i64 } else { 0_i64 })
        .bind(i64::from(entity_count))
        .bind(i64::from(relation_count))
        .bind(timestamp(now)?)
        .execute(&mut *connection)
        .await
        .map_err(StateError::storage)?;
        commit(&mut connection).await
    }

    async fn stage_work(&self, work: StageWork) -> Result<(), StateError> {
        let mut connection = self.begin_immediate().await?;
        ensure_lease(&mut connection, &work.run_id, &work.lease).await?;
        let staged = sqlx::query(
            "INSERT INTO run_work (run_id, entity_name, action, content_hash, vector_address, status) VALUES (?, ?, ?, ?, ?, 'pending') ON CONFLICT(run_id, entity_name, action) DO UPDATE SET content_hash = excluded.content_hash, vector_address = excluded.vector_address, status = CASE WHEN run_work.status = 'succeeded' THEN run_work.status ELSE 'pending' END",
        )
        .bind(work.run_id.as_str())
        .bind(work.entity_name.as_str())
        .bind(work.action.as_str())
        .bind(&work.content_hash)
        .bind(&work.vector_address)
        .execute(&mut *connection)
        .await;
        if let Err(error) = staged {
            rollback(&mut connection).await;
            return Err(StateError::storage(error));
        }
        if work.action == WorkAction::Delete {
            let Some(address) = &work.vector_address else {
                rollback(&mut connection).await;
                return Err(StateError::MissingVectorAddress);
            };
            sqlx::query(
                "INSERT INTO deletion_audit (run_id, entity_name, vector_address, requested_at, result) VALUES (?, ?, ?, ?, 'pending') ON CONFLICT(run_id, entity_name) DO NOTHING",
            )
            .bind(work.run_id.as_str())
            .bind(work.entity_name.as_str())
            .bind(address)
            .bind(timestamp(work.staged_at)?)
            .execute(&mut *connection)
            .await
            .map_err(StateError::storage)?;
        }
        commit(&mut connection).await
    }

    async fn complete_work(&self, work: CompletedWork) -> Result<(), StateError> {
        let mut connection = self.begin_immediate().await?;
        ensure_lease(&mut connection, &work.run_id, &work.lease).await?;
        match work.action {
            WorkAction::Upsert => {
                let entity_type = work.entity_type.as_deref().ok_or_else(|| {
                    StateError::Storage("upsert completion requires entity type".to_owned())
                })?;
                let content_hash = work.content_hash.as_deref().ok_or_else(|| {
                    StateError::Storage("upsert completion requires content hash".to_owned())
                })?;
                let vector_address = work.vector_address.as_deref().ok_or_else(|| {
                    StateError::Storage("upsert completion requires vector address".to_owned())
                })?;
                sqlx::query(
                    "INSERT INTO entity_index_state (entity_name, entity_type, vector_address, content_hash, status, last_indexed_at, last_seen_at) VALUES (?, ?, ?, ?, 'indexed', ?, ?) ON CONFLICT(entity_name) DO UPDATE SET entity_type = excluded.entity_type, vector_address = excluded.vector_address, content_hash = excluded.content_hash, status = 'indexed', last_indexed_at = excluded.last_indexed_at, last_seen_at = excluded.last_seen_at, last_error_code = NULL, last_error_message = NULL",
                )
                .bind(work.entity_name.as_str())
                .bind(entity_type)
                .bind(vector_address)
                .bind(content_hash)
                .bind(timestamp(work.completed_at)?)
                .bind(timestamp(work.completed_at)?)
                .execute(&mut *connection)
                .await
                .map_err(StateError::storage)?;
            }
            WorkAction::Delete => {
                sqlx::query(
                    "UPDATE entity_index_state SET status = 'deleted', last_error_code = NULL, last_error_message = NULL WHERE entity_name = ?",
                )
                .bind(work.entity_name.as_str())
                .execute(&mut *connection)
                .await
                .map_err(StateError::storage)?;
                sqlx::query(
                    "UPDATE deletion_audit SET result = 'deleted', completed_at = ?, error_code = NULL, error_message = NULL WHERE run_id = ? AND entity_name = ?",
                )
                .bind(timestamp(work.completed_at)?)
                .bind(work.run_id.as_str())
                .bind(work.entity_name.as_str())
                .execute(&mut *connection)
                .await
                .map_err(StateError::storage)?;
            }
        }
        let changed = sqlx::query(
            "UPDATE run_work SET status = 'succeeded', attempt_count = attempt_count + 1, last_error_code = NULL, last_error_message = NULL WHERE run_id = ? AND entity_name = ? AND action = ?",
        )
        .bind(work.run_id.as_str())
        .bind(work.entity_name.as_str())
        .bind(work.action.as_str())
        .execute(&mut *connection)
        .await
        .map_err(StateError::storage)?
        .rows_affected();
        if changed != 1 {
            rollback(&mut connection).await;
            return Err(StateError::Storage(
                "completed work was not staged".to_owned(),
            ));
        }
        commit(&mut connection).await
    }

    async fn fail_work(&self, work: FailedWork) -> Result<(), StateError> {
        let mut connection = self.begin_immediate().await?;
        ensure_lease(&mut connection, &work.run_id, &work.lease).await?;
        let changed = sqlx::query(
            "UPDATE run_work SET status = 'failed', attempt_count = attempt_count + 1, last_error_code = ?, last_error_message = ? WHERE run_id = ? AND entity_name = ? AND action = ?",
        )
        .bind(&work.error_code)
        .bind(&work.error_message)
        .bind(work.run_id.as_str())
        .bind(work.entity_name.as_str())
        .bind(work.action.as_str())
        .execute(&mut *connection)
        .await
        .map_err(StateError::storage)?
        .rows_affected();
        if changed != 1 {
            rollback(&mut connection).await;
            return Err(StateError::Storage("failed work was not staged".to_owned()));
        }
        if work.action == WorkAction::Delete {
            sqlx::query(
                "UPDATE deletion_audit SET result = 'failed', error_code = ?, error_message = ? WHERE run_id = ? AND entity_name = ?",
            )
            .bind(&work.error_code)
            .bind(&work.error_message)
            .bind(work.run_id.as_str())
            .bind(work.entity_name.as_str())
            .execute(&mut *connection)
            .await
            .map_err(StateError::storage)?;
        }
        commit(&mut connection).await
    }

    async fn finish_run(&self, completion: RunCompletion) -> Result<(), StateError> {
        let mut connection = self.begin_immediate().await?;
        ensure_lease(&mut connection, &completion.run_id, &completion.lease).await?;
        let changed = sqlx::query(
            "UPDATE run SET status = ?, queue_state = 'finished', finished_at = ?, lease_owner = NULL, lease_expires_at = NULL, entities_seen = ?, entities_indexed = ?, entities_skipped = ?, entities_deleted = ?, entities_failed = ?, error_summary = ? WHERE id = ? AND queue_state = 'leased' AND lease_owner = ? AND lease_epoch = ?",
        )
        .bind(completion.status.as_str())
        .bind(timestamp(completion.finished_at)?)
        .bind(i64::from(completion.entities_seen))
        .bind(i64::from(completion.entities_indexed))
        .bind(i64::from(completion.entities_skipped))
        .bind(i64::from(completion.entities_deleted))
        .bind(i64::from(completion.entities_failed))
        .bind(&completion.error_summary)
        .bind(completion.run_id.as_str())
        .bind(&completion.lease.owner)
        .bind(completion.lease.epoch)
        .execute(&mut *connection)
        .await
        .map_err(StateError::storage)?
        .rows_affected();
        if changed != 1 {
            rollback(&mut connection).await;
            return Err(StateError::LeaseLost);
        }
        commit(&mut connection).await
    }
}

impl SqliteStateRepository {
    async fn recover_leases(
        &self,
        now: OffsetDateTime,
        expired_only: bool,
    ) -> Result<(), StateError> {
        let mut connection = self.begin_immediate().await?;
        let condition = if expired_only {
            " AND lease_expires_at <= ?"
        } else {
            ""
        };
        let sql = format!(
            "UPDATE run_work SET status = 'pending' WHERE status = 'indexing' AND run_id IN (SELECT id FROM run WHERE queue_state = 'leased'{condition})"
        );
        let mut work_query = sqlx::query(&sql);
        if expired_only {
            work_query = work_query.bind(timestamp(now)?);
        }
        work_query
            .execute(&mut *connection)
            .await
            .map_err(StateError::storage)?;
        let sql = format!(
            "UPDATE run SET queue_state = 'queued', status = 'queued', lease_owner = NULL, lease_expires_at = NULL, not_before = ? WHERE queue_state = 'leased'{condition}"
        );
        let mut run_query = sqlx::query(&sql).bind(timestamp(now)?);
        if expired_only {
            run_query = run_query.bind(timestamp(now)?);
        }
        run_query
            .execute(&mut *connection)
            .await
            .map_err(StateError::storage)?;
        commit(&mut connection).await
    }
}

async fn insert_run(
    connection: &mut sqlx::pool::PoolConnection<sqlx::Sqlite>,
    request: &EnqueueRequest,
    now: &str,
) -> Result<(), StateError> {
    let selector_json = selector_json(&request.selector)?;
    sqlx::query(
        "INSERT INTO run (id, trigger, selector_kind, selector_value, selector_json, status, queue_state, requested_at, not_before) VALUES (?, ?, ?, ?, ?, 'queued', 'queued', ?, ?)",
    )
    .bind(request.run_id.as_str())
    .bind(request.trigger.as_str())
    .bind(request.selector.kind().as_str())
    .bind(request.selector.value())
    .bind(selector_json)
    .bind(now)
    .bind(now)
    .execute(&mut **connection)
    .await
    .map_err(StateError::storage)?;
    Ok(())
}

async fn ensure_lease(
    connection: &mut sqlx::pool::PoolConnection<sqlx::Sqlite>,
    run_id: &RunId,
    lease: &Lease,
) -> Result<(), StateError> {
    let found = sqlx::query(
        "SELECT 1 FROM run WHERE id = ? AND queue_state = 'leased' AND lease_owner = ? AND lease_epoch = ?",
    )
    .bind(run_id.as_str())
    .bind(&lease.owner)
    .bind(lease.epoch)
    .fetch_optional(&mut **connection)
    .await
    .map_err(StateError::storage)?;
    if found.is_some() {
        Ok(())
    } else {
        rollback(connection).await;
        Err(StateError::LeaseLost)
    }
}

async fn commit(
    connection: &mut sqlx::pool::PoolConnection<sqlx::Sqlite>,
) -> Result<(), StateError> {
    sqlx::query("COMMIT")
        .execute(&mut **connection)
        .await
        .map_err(StateError::storage)?;
    Ok(())
}

async fn rollback(connection: &mut sqlx::pool::PoolConnection<sqlx::Sqlite>) {
    let _ = sqlx::query("ROLLBACK").execute(&mut **connection).await;
}

fn selector_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<Selector, StateError> {
    let kind: String = row.get("selector_kind");
    let value: Option<String> = row.get("selector_value");
    match (kind.as_str(), value) {
        ("full", None) => Ok(Selector::Full),
        ("entity", Some(value)) => EntityName::parse(value)
            .map(Selector::Entity)
            .map_err(StateError::domain),
        ("entity_type", Some(value)) => EntityType::parse(value)
            .map(Selector::EntityType)
            .map_err(StateError::domain),
        _ => Err(StateError::InvalidStoredSelector),
    }
}

fn selector_json(selector: &Selector) -> Result<String, StateError> {
    match selector {
        Selector::Full => Ok(r#"{"full":true}"#.to_owned()),
        Selector::Entity(name) => {
            serde_json::to_string(&serde_json::json!({ "entity": name.as_str() }))
                .map_err(StateError::storage)
        }
        Selector::EntityType(entity_type) => {
            serde_json::to_string(&serde_json::json!({ "entityType": entity_type.as_str() }))
                .map_err(StateError::storage)
        }
    }
}

fn parse_run_id(value: String) -> Result<RunId, StateError> {
    RunId::parse(value).map_err(StateError::domain)
}

fn timestamp(value: OffsetDateTime) -> Result<String, StateError> {
    value.format(&Rfc3339).map_err(StateError::storage)
}

fn create_private_parent(path: &Path) -> Result<(), StateError> {
    let parent = path.parent().ok_or(StateError::MissingDatabaseParent)?;
    std::fs::create_dir_all(parent).map_err(StateError::storage)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))
            .map_err(StateError::storage)?;
    }
    Ok(())
}

fn set_private_file_permissions(path: &Path) -> Result<(), StateError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .map_err(StateError::storage)?;
    }
    Ok(())
}
