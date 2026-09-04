use std::collections::{BTreeMap, BTreeSet};

use thiserror::Error;

use crate::domain::{
    canonical::{CanonicalDocument, CanonicalError, RepresentationSettings, canonicalize},
    model::{EntityName, GraphEntity, GraphSnapshot, IndexedEntityState, Selector},
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReconciliationPlan {
    pub actions: Vec<PlannedAction>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PlannedAction {
    Upsert {
        entity_name: EntityName,
        document: CanonicalDocument,
    },
    Skip {
        entity_name: EntityName,
    },
    Delete {
        entity_name: EntityName,
        vector_address: EntityName,
    },
}

pub fn plan(
    snapshot: &GraphSnapshot,
    selector: &Selector,
    indexed_entities: &[IndexedEntityState],
    settings: &RepresentationSettings,
) -> Result<ReconciliationPlan, PlannerError> {
    let (entities, relations) = (&snapshot.entities, &snapshot.relations);
    let entities_by_name = unique_entities(entities)?;
    let indexed_by_name = unique_indexed_entities(indexed_entities)?;
    let mut actions = selected_entities(&entities_by_name, selector)
        .into_iter()
        .map(|entity| {
            let document = canonicalize(entity, entities, relations, settings)?;
            let action = indexed_by_name
                .get(&entity.name)
                .filter(|state| state.content_hash == document.sha256)
                .map_or_else(
                    || PlannedAction::Upsert {
                        entity_name: entity.name.clone(),
                        document,
                    },
                    |_| PlannedAction::Skip {
                        entity_name: entity.name.clone(),
                    },
                );
            Ok(action)
        })
        .collect::<Result<Vec<_>, PlannerError>>()?;

    if snapshot.permits_deletion(selector) {
        let observed_names = entities_by_name.keys().cloned().collect::<BTreeSet<_>>();
        actions.extend(indexed_by_name.into_iter().filter_map(|(name, state)| {
            (!observed_names.contains(&name)).then_some(PlannedAction::Delete {
                entity_name: name,
                vector_address: state.vector_address,
            })
        }));
    }

    Ok(ReconciliationPlan { actions })
}

fn unique_entities(
    entities: &[GraphEntity],
) -> Result<BTreeMap<EntityName, &GraphEntity>, PlannerError> {
    entities
        .iter()
        .try_fold(BTreeMap::new(), |mut by_name, entity| {
            if by_name.insert(entity.name.clone(), entity).is_some() {
                return Err(PlannerError::DuplicateSnapshotEntity(entity.name.clone()));
            }
            Ok(by_name)
        })
}

fn unique_indexed_entities(
    indexed_entities: &[IndexedEntityState],
) -> Result<BTreeMap<EntityName, IndexedEntityState>, PlannerError> {
    indexed_entities
        .iter()
        .cloned()
        .try_fold(BTreeMap::new(), |mut by_name, state| {
            if by_name
                .insert(state.entity_name.clone(), state.clone())
                .is_some()
            {
                return Err(PlannerError::DuplicateIndexedEntity(state.entity_name));
            }
            Ok(by_name)
        })
}

fn selected_entities<'a>(
    entities_by_name: &'a BTreeMap<EntityName, &'a GraphEntity>,
    selector: &Selector,
) -> Vec<&'a GraphEntity> {
    entities_by_name
        .values()
        .copied()
        .filter(|entity| match selector {
            Selector::Full => true,
            Selector::Entity(name) => entity.name == *name,
            Selector::EntityType(entity_type) => entity.entity_type == *entity_type,
        })
        .collect()
}

#[derive(Debug, Error)]
pub enum PlannerError {
    #[error("snapshot contains duplicate entity name: {0:?}")]
    DuplicateSnapshotEntity(EntityName),
    #[error("indexed state contains duplicate entity name: {0:?}")]
    DuplicateIndexedEntity(EntityName),
    #[error(transparent)]
    Canonical(#[from] CanonicalError),
}
