use std::{collections::BTreeSet, num::NonZeroU32};

use thiserror::Error;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EntityName(String);

impl EntityName {
    pub fn parse(value: String) -> Result<Self, DomainError> {
        if value.is_empty() {
            return Err(DomainError::EmptyEntityName);
        }

        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EntityType(String);

impl EntityType {
    pub fn parse(value: String) -> Result<Self, DomainError> {
        if value.trim().is_empty() {
            return Err(DomainError::EmptyEntityType);
        }

        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Dimension(NonZeroU32);

impl Dimension {
    pub fn parse(value: u32) -> Result<Self, DomainError> {
        NonZeroU32::new(value)
            .map(Self)
            .ok_or(DomainError::ZeroDimension)
    }

    pub fn get(self) -> u32 {
        self.0.get()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Embedding(Vec<f32>);

impl Embedding {
    pub fn new(values: Vec<f32>, dimension: Dimension) -> Result<Self, DomainError> {
        let actual = u32::try_from(values.len()).map_err(|_| DomainError::EmbeddingTooLong)?;
        if actual != dimension.get() {
            return Err(DomainError::EmbeddingDimensionMismatch {
                expected: dimension.get(),
                actual,
            });
        }

        Ok(Self(values))
    }

    pub fn values(&self) -> &[f32] {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphEntity {
    pub name: EntityName,
    pub entity_type: EntityType,
    pub observations: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphRelation {
    pub from: EntityName,
    pub relation_type: String,
    pub to: EntityName,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphSnapshot {
    pub entities: Vec<GraphEntity>,
    pub relations: Vec<GraphRelation>,
    pub deletion_proof: DeletionProof,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeletionProof {
    Complete,
    Unproven(IncompleteSnapshotReason),
}

impl GraphSnapshot {
    pub fn complete(
        entities: Vec<GraphEntity>,
        relations: Vec<GraphRelation>,
    ) -> Result<Self, IncompleteSnapshotReason> {
        let mut names = BTreeSet::new();
        for entity in &entities {
            if !names.insert(entity.name.clone()) {
                return Err(IncompleteSnapshotReason::DuplicateEntityName(
                    entity.name.clone(),
                ));
            }
        }

        Ok(Self {
            entities,
            relations,
            deletion_proof: DeletionProof::Complete,
        })
    }

    pub fn permits_deletion(&self, selector: &Selector) -> bool {
        matches!(self.deletion_proof, DeletionProof::Complete) && matches!(selector, Selector::Full)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IncompleteSnapshotReason {
    PaginationNotExhausted,
    SnapshotChangedDuringRead,
    DuplicateEntityName(EntityName),
    TransportFailure,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Selector {
    Full,
    Entity(EntityName),
    EntityType(EntityType),
}

impl Selector {
    pub fn kind(&self) -> SelectorKind {
        match self {
            Self::Full => SelectorKind::Full,
            Self::Entity(_) => SelectorKind::Entity,
            Self::EntityType(_) => SelectorKind::EntityType,
        }
    }

    pub fn value(&self) -> Option<&str> {
        match self {
            Self::Full => None,
            Self::Entity(name) => Some(name.as_str()),
            Self::EntityType(entity_type) => Some(entity_type.as_str()),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SelectorKind {
    Full,
    Entity,
    EntityType,
}

impl SelectorKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Entity => "entity",
            Self::EntityType => "entity_type",
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct RunId(String);

impl RunId {
    pub fn parse(value: String) -> Result<Self, DomainError> {
        (!value.trim().is_empty())
            .then_some(Self(value))
            .ok_or(DomainError::BlankRunId)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct IdempotencyKey(String);

impl IdempotencyKey {
    pub fn parse(value: String) -> Result<Self, DomainError> {
        (!value.trim().is_empty())
            .then_some(Self(value))
            .ok_or(DomainError::BlankIdempotencyKey)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RunTrigger {
    Poll,
    Api,
    Fullscan,
    Startup,
}

impl RunTrigger {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Poll => "poll",
            Self::Api => "api",
            Self::Fullscan => "fullscan",
            Self::Startup => "startup",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkAction {
    Upsert,
    Delete,
}

impl WorkAction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Upsert => "upsert",
            Self::Delete => "delete",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Lease {
    pub owner: String,
    pub epoch: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClaimedRun {
    pub id: RunId,
    pub selector: Selector,
    pub lease: Lease,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexedEntityState {
    pub entity_name: EntityName,
    pub content_hash: String,
    pub vector_address: EntityName,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigFingerprint {
    pub taxonomy_version: String,
    pub representation_version: String,
    pub embedding_model: String,
    pub dimensions: Dimension,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum DomainError {
    #[error("entity name must not be empty")]
    EmptyEntityName,
    #[error("entity type must not be blank")]
    EmptyEntityType,
    #[error("embedding dimension must be greater than zero")]
    ZeroDimension,
    #[error("embedding has {actual} values, expected {expected}")]
    EmbeddingDimensionMismatch { expected: u32, actual: u32 },
    #[error("embedding contains too many values")]
    EmbeddingTooLong,
    #[error("run id must not be blank")]
    BlankRunId,
    #[error("idempotency key must not be blank")]
    BlankIdempotencyKey,
}

#[cfg(test)]
mod tests {
    use super::{
        DeletionProof, Dimension, Embedding, EntityName, EntityType, GraphEntity, GraphSnapshot,
        IncompleteSnapshotReason, Selector,
    };

    #[test]
    fn entity_name_preserves_case_and_unicode() {
        let name = EntityName::parse("Źródło".to_owned());
        assert_eq!(name.as_ref().map(EntityName::as_str), Ok("Źródło"));
        assert_ne!(
            EntityName::parse("Źródło".to_owned()),
            EntityName::parse("źródło".to_owned())
        );
    }

    #[test]
    fn entity_name_rejects_empty_value() {
        assert!(EntityName::parse(String::new()).is_err());
    }

    #[test]
    fn embedding_requires_the_configured_dimension() {
        let dimension = Dimension::parse(384);
        assert!(dimension.is_ok());
        let dimension = match dimension {
            Ok(value) => value,
            Err(error) => panic!("valid dimension rejected: {error}"),
        };

        assert!(Embedding::new(vec![0.0; 383], dimension).is_err());
        assert!(Embedding::new(vec![0.0; 384], dimension).is_ok());
        assert!(Embedding::new(vec![0.0; 385], dimension).is_err());
    }

    #[test]
    fn only_complete_full_snapshots_permit_deletion() {
        let name = match EntityName::parse("A".to_owned()) {
            Ok(value) => value,
            Err(error) => panic!("valid entity name rejected: {error}"),
        };
        let entity_type = match EntityType::parse("Projekt".to_owned()) {
            Ok(value) => value,
            Err(error) => panic!("valid entity type rejected: {error}"),
        };
        let snapshot = GraphSnapshot {
            entities: Vec::new(),
            relations: Vec::new(),
            deletion_proof: DeletionProof::Complete,
        };

        assert!(snapshot.permits_deletion(&Selector::Full));
        assert!(!snapshot.permits_deletion(&Selector::Entity(name)));
        assert!(!snapshot.permits_deletion(&Selector::EntityType(entity_type)));

        let unproven = GraphSnapshot {
            entities: Vec::new(),
            relations: Vec::new(),
            deletion_proof: DeletionProof::Unproven(
                IncompleteSnapshotReason::PaginationNotExhausted,
            ),
        };
        assert!(!unproven.permits_deletion(&Selector::Full));
    }

    #[test]
    fn complete_snapshot_rejects_duplicate_exact_names_but_not_case_variants() {
        let project = match EntityType::parse("Projekt".to_owned()) {
            Ok(value) => value,
            Err(error) => panic!("valid entity type rejected: {error}"),
        };
        let entity = |name: &str| GraphEntity {
            name: match EntityName::parse(name.to_owned()) {
                Ok(value) => value,
                Err(error) => panic!("valid entity name rejected: {error}"),
            },
            entity_type: project.clone(),
            observations: Vec::new(),
        };

        assert!(GraphSnapshot::complete(vec![entity("Foo"), entity("Foo")], Vec::new()).is_err());
        assert!(GraphSnapshot::complete(vec![entity("Foo"), entity("foo")], Vec::new()).is_ok());
    }
}
