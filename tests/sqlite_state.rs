use std::time::Duration;

use second_brain_indexer::{
    adapters::sqlite::SqliteStateRepository,
    domain::model::{
        EntityName, EntityType, IdempotencyKey, RunId, RunTrigger, Selector, WorkAction,
    },
    ports::{
        CompletedWork, EnqueueOutcome, EnqueueRequest, IdempotencyRequest, StageWork,
        StateRepository,
    },
};
use sqlx::Row;
use tempfile::TempDir;
use time::OffsetDateTime;

fn run_id(value: &str) -> RunId {
    match RunId::parse(value.to_owned()) {
        Ok(value) => value,
        Err(error) => panic!("test run id rejected: {error}"),
    }
}

fn entity_name(value: &str) -> EntityName {
    match EntityName::parse(value.to_owned()) {
        Ok(value) => value,
        Err(error) => panic!("test entity name rejected: {error}"),
    }
}

fn entity_type(value: &str) -> EntityType {
    match EntityType::parse(value.to_owned()) {
        Ok(value) => value,
        Err(error) => panic!("test entity type rejected: {error}"),
    }
}

async fn repository() -> (TempDir, SqliteStateRepository) {
    let directory = match tempfile::tempdir() {
        Ok(value) => value,
        Err(error) => panic!("temporary directory unavailable: {error}"),
    };
    let database = directory.path().join("state.db");
    let repository = match SqliteStateRepository::connect(&database, Duration::from_secs(60)).await
    {
        Ok(value) => value,
        Err(error) => panic!("repository unavailable: {error}"),
    };
    (directory, repository)
}

fn request(id: &str, selector: Selector, now: OffsetDateTime) -> EnqueueRequest {
    EnqueueRequest {
        run_id: run_id(id),
        trigger: RunTrigger::Api,
        selector,
        requested_at: now,
        idempotency: None,
    }
}

#[tokio::test]
async fn competing_claims_yield_exactly_one_lease() {
    let (_directory, repository) = repository().await;
    let now = OffsetDateTime::UNIX_EPOCH;
    let queued = repository
        .enqueue(request("run-1", Selector::Full, now))
        .await;
    assert!(matches!(queued, Ok(EnqueueOutcome::Queued { .. })));

    let (left, right) = tokio::join!(
        repository.claim_next("worker-left", now),
        repository.claim_next("worker-right", now),
    );
    let claims = [left, right]
        .into_iter()
        .filter(|claim| matches!(claim, Ok(Some(_))))
        .count();
    assert_eq!(claims, 1);
}

#[tokio::test]
async fn stale_lease_cannot_stage_work_after_recovery() {
    let (_directory, repository) = repository().await;
    let now = OffsetDateTime::UNIX_EPOCH;
    let queued = repository
        .enqueue(request("run-1", Selector::Full, now))
        .await;
    assert!(matches!(queued, Ok(EnqueueOutcome::Queued { .. })));
    let first = match repository.claim_next("old-worker", now).await {
        Ok(Some(value)) => value,
        Ok(None) => panic!("queued run was not claimed"),
        Err(error) => panic!("claim failed: {error}"),
    };
    let recovered = repository.recover_after_exclusive_start(now).await;
    assert!(recovered.is_ok());
    let second = match repository.claim_next("new-worker", now).await {
        Ok(Some(value)) => value,
        Ok(None) => panic!("recovered run was not claimed"),
        Err(error) => panic!("second claim failed: {error}"),
    };

    let stale = repository
        .record_snapshot(&first.id, &first.lease, true, 1, 0, now)
        .await;
    assert!(stale.is_err());
    let current = repository
        .record_snapshot(&second.id, &second.lease, true, 1, 0, now)
        .await;
    assert!(current.is_ok());
}

#[tokio::test]
async fn startup_recovery_resets_unexpired_indexing_work() {
    let (_directory, repository) = repository().await;
    let now = OffsetDateTime::UNIX_EPOCH;
    let enqueued = repository
        .enqueue(request("run-1", Selector::Full, now))
        .await;
    assert!(matches!(enqueued, Ok(EnqueueOutcome::Queued { .. })));
    let claimed = match repository.claim_next("worker", now).await {
        Ok(Some(value)) => value,
        Ok(None) => panic!("queued run was not claimed"),
        Err(error) => panic!("claim failed: {error}"),
    };
    let snapshot = repository
        .record_snapshot(&claimed.id, &claimed.lease, true, 1, 0, now)
        .await;
    assert!(snapshot.is_ok());
    let staged = repository
        .stage_work(StageWork {
            run_id: claimed.id.clone(),
            lease: claimed.lease.clone(),
            entity_name: entity_name("A"),
            action: WorkAction::Upsert,
            content_hash: Some("hash".to_owned()),
            vector_address: None,
            staged_at: now,
        })
        .await;
    assert!(staged.is_ok());
    let changed = sqlx::query("UPDATE run_work SET status = 'indexing'")
        .execute(repository.pool())
        .await;
    assert!(changed.is_ok());
    let recovered = repository.recover_after_exclusive_start(now).await;
    assert!(recovered.is_ok());
    let row = match sqlx::query("SELECT queue_state FROM run WHERE id = 'run-1'")
        .fetch_one(repository.pool())
        .await
    {
        Ok(value) => value,
        Err(error) => panic!("run was not persisted: {error}"),
    };
    assert_eq!(row.get::<String, _>("queue_state"), "queued");
    let row = match sqlx::query("SELECT status FROM run_work WHERE run_id = 'run-1'")
        .fetch_one(repository.pool())
        .await
    {
        Ok(value) => value,
        Err(error) => panic!("work was not persisted: {error}"),
    };
    assert_eq!(row.get::<String, _>("status"), "pending");
}

#[tokio::test]
async fn only_exact_queued_selector_coalesces() {
    let (_directory, repository) = repository().await;
    let now = OffsetDateTime::UNIX_EPOCH;
    let first = repository
        .enqueue(request("run-1", Selector::Entity(entity_name("A")), now))
        .await;
    assert!(matches!(first, Ok(EnqueueOutcome::Queued { .. })));
    let same = repository
        .enqueue(request("run-2", Selector::Entity(entity_name("A")), now))
        .await;
    assert!(matches!(
        same,
        Ok(EnqueueOutcome::Coalesced { run_id: ref coalesced_id }) if coalesced_id == &run_id("run-1")
    ));
    let incompatible = repository
        .enqueue(request(
            "run-3",
            Selector::EntityType(entity_type("Projekt")),
            now,
        ))
        .await;
    assert!(matches!(incompatible, Ok(EnqueueOutcome::Conflict)));
}

#[tokio::test]
async fn idempotency_replays_before_ttl_and_reuses_after_expiry() {
    let (_directory, repository) = repository().await;
    let now = OffsetDateTime::UNIX_EPOCH;
    let key = match IdempotencyKey::parse("key".to_owned()) {
        Ok(value) => value,
        Err(error) => panic!("test idempotency key rejected: {error}"),
    };
    let idempotency = IdempotencyRequest {
        key: key.clone(),
        request_hash: "request-a".to_owned(),
        response_status: 202,
        response_body_json: r#"{"runId":"run-1"}"#.to_owned(),
        expires_at: now + time::Duration::hours(24),
    };
    let first = repository
        .enqueue(EnqueueRequest {
            idempotency: Some(idempotency.clone()),
            ..request("run-1", Selector::Full, now)
        })
        .await;
    assert!(matches!(first, Ok(EnqueueOutcome::Queued { .. })));
    let replay = repository
        .enqueue(EnqueueRequest {
            idempotency: Some(idempotency),
            ..request("run-2", Selector::Full, now)
        })
        .await;
    assert!(matches!(
        replay,
        Ok(EnqueueOutcome::IdempotentReplay { .. })
    ));
    let conflicting = IdempotencyRequest {
        key,
        request_hash: "request-b".to_owned(),
        response_status: 202,
        response_body_json: "{}".to_owned(),
        expires_at: now + time::Duration::hours(24),
    };
    let conflict = repository
        .enqueue(EnqueueRequest {
            idempotency: Some(conflicting),
            ..request("run-3", Selector::Full, now)
        })
        .await;
    assert!(matches!(conflict, Ok(EnqueueOutcome::IdempotencyConflict)));
    let claimed = repository.claim_next("worker", now).await;
    assert!(matches!(claimed, Ok(Some(_))));
    let expired = now + time::Duration::hours(25);
    let fresh_key = match IdempotencyKey::parse("key".to_owned()) {
        Ok(value) => value,
        Err(error) => panic!("test idempotency key rejected: {error}"),
    };
    let fresh = IdempotencyRequest {
        key: fresh_key,
        request_hash: "request-c".to_owned(),
        response_status: 202,
        response_body_json: r#"{"runId":"run-4"}"#.to_owned(),
        expires_at: expired + time::Duration::hours(24),
    };
    let after_expiry = repository
        .enqueue(EnqueueRequest {
            idempotency: Some(fresh),
            ..request("run-4", Selector::Full, expired)
        })
        .await;
    assert!(matches!(after_expiry, Ok(EnqueueOutcome::Queued { .. })));
}

#[tokio::test]
async fn delete_work_requires_a_complete_full_snapshot_and_is_audited() {
    let (_directory, repository) = repository().await;
    let now = OffsetDateTime::UNIX_EPOCH;
    let enqueued = repository
        .enqueue(request("run-1", Selector::Full, now))
        .await;
    assert!(matches!(enqueued, Ok(EnqueueOutcome::Queued { .. })));
    let claimed = match repository.claim_next("worker", now).await {
        Ok(Some(value)) => value,
        Ok(None) => panic!("queued run was not claimed"),
        Err(error) => panic!("claim failed: {error}"),
    };
    let snapshot = repository
        .record_snapshot(&claimed.id, &claimed.lease, false, 1, 0, now)
        .await;
    assert!(snapshot.is_ok());
    let rejected = repository
        .stage_work(StageWork {
            run_id: claimed.id.clone(),
            lease: claimed.lease.clone(),
            entity_name: entity_name("A"),
            action: WorkAction::Delete,
            content_hash: None,
            vector_address: Some("A".to_owned()),
            staged_at: now,
        })
        .await;
    assert!(rejected.is_err());
    let complete = repository
        .record_snapshot(&claimed.id, &claimed.lease, true, 1, 0, now)
        .await;
    assert!(complete.is_ok());
    let staged = repository
        .stage_work(StageWork {
            run_id: claimed.id.clone(),
            lease: claimed.lease.clone(),
            entity_name: entity_name("A"),
            action: WorkAction::Delete,
            content_hash: None,
            vector_address: Some("A".to_owned()),
            staged_at: now,
        })
        .await;
    assert!(staged.is_ok());
    let audit_count = match sqlx::query("SELECT COUNT(*) AS count FROM deletion_audit")
        .fetch_one(repository.pool())
        .await
    {
        Ok(value) => value.get::<i64, _>("count"),
        Err(error) => panic!("deletion audit missing: {error}"),
    };
    assert_eq!(audit_count, 1);
}

#[tokio::test]
async fn schema_has_no_embedding_column_or_secret_storage() {
    let (_directory, repository) = repository().await;
    let schemas = match sqlx::query("SELECT name FROM sqlite_schema WHERE type = 'table'")
        .fetch_all(repository.pool())
        .await
    {
        Ok(value) => value,
        Err(error) => panic!("schema unavailable: {error}"),
    };
    for table in schemas {
        let table_name: String = table.get("name");
        let columns = match sqlx::query(&format!("PRAGMA table_info({table_name})"))
            .fetch_all(repository.pool())
            .await
        {
            Ok(value) => value,
            Err(error) => panic!("columns unavailable: {error}"),
        };
        for column in columns {
            let name: String = column.get("name");
            assert_ne!(name, "embedding");
            assert_ne!(name, "api_key");
            assert_ne!(name, "bearer_token");
        }
    }
}

#[tokio::test]
async fn upsert_state_is_indexed_only_by_the_post_mcp_completion_transition() {
    let (_directory, repository) = repository().await;
    let now = OffsetDateTime::UNIX_EPOCH;
    let enqueued = repository
        .enqueue(request("run-1", Selector::Full, now))
        .await;
    assert!(matches!(enqueued, Ok(EnqueueOutcome::Queued { .. })));
    let claimed = match repository.claim_next("worker", now).await {
        Ok(Some(value)) => value,
        Ok(None) => panic!("queued run was not claimed"),
        Err(error) => panic!("claim failed: {error}"),
    };
    let staged = repository
        .stage_work(StageWork {
            run_id: claimed.id.clone(),
            lease: claimed.lease.clone(),
            entity_name: entity_name("A"),
            action: WorkAction::Upsert,
            content_hash: Some("hash".to_owned()),
            vector_address: None,
            staged_at: now,
        })
        .await;
    assert!(staged.is_ok());
    let before_completion = sqlx::query("SELECT COUNT(*) AS count FROM entity_index_state")
        .fetch_one(repository.pool())
        .await;
    let before_completion = match before_completion {
        Ok(row) => row.get::<i64, _>("count"),
        Err(error) => panic!("state query failed: {error}"),
    };
    assert_eq!(before_completion, 0);

    let completed = repository
        .complete_work(CompletedWork {
            run_id: claimed.id,
            lease: claimed.lease,
            entity_name: entity_name("A"),
            action: WorkAction::Upsert,
            entity_type: Some("Projekt".to_owned()),
            content_hash: Some("hash".to_owned()),
            vector_address: Some("A".to_owned()),
            completed_at: now,
        })
        .await;
    assert!(completed.is_ok());
    let state = sqlx::query("SELECT status FROM entity_index_state WHERE entity_name = 'A'")
        .fetch_one(repository.pool())
        .await;
    let state = match state {
        Ok(row) => row.get::<String, _>("status"),
        Err(error) => panic!("indexed state query failed: {error}"),
    };
    assert_eq!(state, "indexed");
}
