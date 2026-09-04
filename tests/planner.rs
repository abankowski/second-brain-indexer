use std::time::Duration;

use second_brain_indexer::{
    adapters::sqlite::SqliteStateRepository,
    domain::{
        canonical::{RepresentationSettings, canonicalize},
        model::{
            DeletionProof, EntityName, EntityType, GraphEntity, GraphRelation, GraphSnapshot,
            IncompleteSnapshotReason, IndexedEntityState, Selector,
        },
        planner::{PlannedAction, PlannerError, plan},
    },
    ports::StateRepository,
};
use sqlx::query;
use tempfile::TempDir;

fn name(value: &str) -> EntityName {
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

fn entity(name_value: &str, type_value: &str) -> GraphEntity {
    GraphEntity {
        name: name(name_value),
        entity_type: entity_type(type_value),
        observations: vec![format!("observation for {name_value}")],
    }
}

fn complete(entities: Vec<GraphEntity>) -> GraphSnapshot {
    match GraphSnapshot::complete(entities, Vec::<GraphRelation>::new()) {
        Ok(snapshot) => snapshot,
        Err(error) => panic!("test snapshot rejected: {error:?}"),
    }
}

fn settings() -> RepresentationSettings {
    RepresentationSettings {
        version: "entity-v1".to_owned(),
        max_input_chars: 10_000,
    }
}

fn current_hash(entity: &GraphEntity, all_entities: &[GraphEntity]) -> String {
    match canonicalize(entity, all_entities, &[], &settings()) {
        Ok(document) => document.sha256,
        Err(error) => panic!("canonical document unavailable: {error}"),
    }
}

fn indexed(name_value: &str, content_hash: &str, vector_address: &str) -> IndexedEntityState {
    IndexedEntityState {
        entity_name: name(name_value),
        content_hash: content_hash.to_owned(),
        vector_address: name(vector_address),
    }
}

#[derive(Debug, Eq, PartialEq)]
enum ActionSummary {
    Upsert(String),
    Skip(String),
    Delete {
        entity_name: String,
        vector_address: String,
    },
}

fn summarize(actions: &[PlannedAction]) -> Vec<ActionSummary> {
    actions
        .iter()
        .map(|action| match action {
            PlannedAction::Upsert { entity_name, .. } => {
                ActionSummary::Upsert(entity_name.as_str().to_owned())
            }
            PlannedAction::Skip { entity_name } => {
                ActionSummary::Skip(entity_name.as_str().to_owned())
            }
            PlannedAction::Delete {
                entity_name,
                vector_address,
            } => ActionSummary::Delete {
                entity_name: entity_name.as_str().to_owned(),
                vector_address: vector_address.as_str().to_owned(),
            },
        })
        .collect()
}

#[test]
fn full_snapshot_maps_new_changed_same_and_missing_names_to_actions() {
    let entities = vec![
        entity("A", "Projekt"),
        entity("B", "Projekt"),
        entity("C", "Projekt"),
    ];
    let cases = [
        (
            "new",
            Vec::new(),
            vec![
                ActionSummary::Upsert("A".to_owned()),
                ActionSummary::Upsert("B".to_owned()),
                ActionSummary::Upsert("C".to_owned()),
            ],
        ),
        (
            "changed",
            vec![indexed("A", "old-hash", "A")],
            vec![
                ActionSummary::Upsert("A".to_owned()),
                ActionSummary::Upsert("B".to_owned()),
                ActionSummary::Upsert("C".to_owned()),
            ],
        ),
        (
            "same",
            vec![
                indexed("A", &current_hash(&entities[0], &entities), "A"),
                indexed("B", &current_hash(&entities[1], &entities), "B"),
                indexed("C", &current_hash(&entities[2], &entities), "C"),
            ],
            vec![
                ActionSummary::Skip("A".to_owned()),
                ActionSummary::Skip("B".to_owned()),
                ActionSummary::Skip("C".to_owned()),
            ],
        ),
        (
            "missing",
            vec![indexed("missing", "old-hash", "stored-vector-address")],
            vec![
                ActionSummary::Upsert("A".to_owned()),
                ActionSummary::Upsert("B".to_owned()),
                ActionSummary::Upsert("C".to_owned()),
                ActionSummary::Delete {
                    entity_name: "missing".to_owned(),
                    vector_address: "stored-vector-address".to_owned(),
                },
            ],
        ),
    ];

    for (case_name, indexed_entities, expected) in cases {
        let result = plan(
            &complete(entities.clone()),
            &Selector::Full,
            &indexed_entities,
            &settings(),
        );
        let actual = match result {
            Ok(value) => summarize(&value.actions),
            Err(error) => panic!("{case_name} plan failed: {error}"),
        };
        assert_eq!(actual, expected, "{case_name}");
    }
}

#[test]
fn non_full_selectors_never_emit_deletes() {
    let entities = vec![entity("A", "Projekt"), entity("B", "Osoba")];
    let indexed_entities = vec![indexed("missing", "old-hash", "stored-vector-address")];
    let cases = [
        (
            Selector::Entity(name("A")),
            vec![ActionSummary::Upsert("A".to_owned())],
        ),
        (
            Selector::EntityType(entity_type("Osoba")),
            vec![ActionSummary::Upsert("B".to_owned())],
        ),
    ];

    for (selector, expected) in cases {
        let result = plan(
            &complete(entities.clone()),
            &selector,
            &indexed_entities,
            &settings(),
        );
        let actual = match result {
            Ok(value) => summarize(&value.actions),
            Err(error) => panic!("non-full plan failed: {error}"),
        };
        assert_eq!(actual, expected);
    }
}

#[test]
fn unproven_snapshot_still_upserts_but_cannot_produce_delete_work() {
    let result = plan(
        &GraphSnapshot {
            entities: vec![entity("A", "Projekt")],
            relations: Vec::new(),
            deletion_proof: DeletionProof::Unproven(
                IncompleteSnapshotReason::PaginationNotExhausted,
            ),
        },
        &Selector::Full,
        &[indexed("missing", "old-hash", "stored-vector-address")],
        &settings(),
    );
    let plan = match result {
        Ok(value) => value,
        Err(error) => panic!("unproven snapshot should still plan upserts: {error}"),
    };
    assert_eq!(
        summarize(&plan.actions),
        vec![ActionSummary::Upsert("A".to_owned())]
    );
}

#[test]
fn duplicate_indexed_state_is_rejected_instead_of_selecting_an_arbitrary_hash() {
    let result = plan(
        &complete(vec![entity("A", "Projekt")]),
        &Selector::Full,
        &[indexed("A", "first", "A"), indexed("A", "second", "A")],
        &settings(),
    );
    assert!(matches!(
        result,
        Err(PlannerError::DuplicateIndexedEntity(_))
    ));
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

#[tokio::test]
async fn state_read_returns_only_indexed_rows_in_name_order_with_stored_address() {
    let (_directory, repository) = repository().await;
    for (entity_name, content_hash, vector_address, status) in [
        ("B", "hash-b", "vector-b", "indexed"),
        ("A", "hash-a", "vector-a", "indexed"),
        ("failed", "hash-failed", "vector-failed", "failed"),
    ] {
        let inserted = query(
            "INSERT INTO entity_index_state (entity_name, entity_type, vector_address, content_hash, status, last_seen_at) VALUES (?, 'Projekt', ?, ?, ?, '2026-01-01T00:00:00Z')",
        )
        .bind(entity_name)
        .bind(vector_address)
        .bind(content_hash)
        .bind(status)
        .execute(repository.pool())
        .await;
        assert!(inserted.is_ok());
    }

    let read = repository.list_indexed_entities().await;
    let states = match read {
        Ok(value) => value,
        Err(error) => panic!("indexed state read failed: {error}"),
    };
    let actual = states
        .iter()
        .map(|state| {
            (
                state.entity_name.as_str(),
                state.content_hash.as_str(),
                state.vector_address.as_str(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        actual,
        [("A", "hash-a", "vector-a"), ("B", "hash-b", "vector-b")]
    );
}
