use std::collections::{BTreeMap, BTreeSet};

use sha2::{Digest, Sha256};
use thiserror::Error;
use unicode_normalization::UnicodeNormalization;

use crate::domain::model::{EntityName, GraphEntity, GraphRelation};

const TRUNCATION_SUFFIX: &str = "\n[truncated]\n";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepresentationSettings {
    pub version: String,
    pub max_input_chars: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalDocument {
    pub text: String,
    pub sha256: String,
}

pub fn canonicalize(
    entity: &GraphEntity,
    all_entities: &[GraphEntity],
    relations: &[GraphRelation],
    settings: &RepresentationSettings,
) -> Result<CanonicalDocument, CanonicalError> {
    if settings.version.trim().is_empty() {
        return Err(CanonicalError::BlankRepresentationVersion);
    }
    if settings.max_input_chars <= TRUNCATION_SUFFIX.chars().count() {
        return Err(CanonicalError::MaxInputCharsTooSmall);
    }

    let entity_types = all_entities
        .iter()
        .map(|candidate| (candidate.name.as_str(), candidate.entity_type.as_str()))
        .collect::<BTreeMap<_, _>>();
    let observations = entity
        .observations
        .iter()
        .map(|value| normalize(value))
        .filter(|value| !value.is_empty())
        .collect::<BTreeSet<_>>();
    let relations = canonical_relations(entity, relations, &entity_types)?;
    let representation_version = quote(&normalize(&settings.version))?;
    let entity_type = quote(&normalize(entity.entity_type.as_str()))?;
    let entity_name = quote(&normalize(entity.name.as_str()))?;
    let mut text = format!(
        "representation: {}\nentity_type: {}\nentity_name: {}\n\nobservations:\n",
        representation_version, entity_type, entity_name,
    );
    for observation in observations {
        text.push_str("- ");
        text.push_str(&quote(&observation)?);
        text.push('\n');
    }
    text.push_str("\nrelations:\n");
    for relation in relations {
        text.push_str("- ");
        text.push_str(&relation);
        text.push('\n');
    }

    let text = truncate(text, settings.max_input_chars);
    let sha256 = format!("{:x}", Sha256::digest(text.as_bytes()));
    Ok(CanonicalDocument { text, sha256 })
}

fn canonical_relations(
    entity: &GraphEntity,
    relations: &[GraphRelation],
    entity_types: &BTreeMap<&str, &str>,
) -> Result<BTreeSet<String>, CanonicalError> {
    relations
        .iter()
        .filter_map(|relation| {
            let direction = if relation.from == entity.name {
                Some(("outgoing", &relation.to))
            } else if relation.to == entity.name {
                Some(("incoming", &relation.from))
            } else {
                None
            };
            direction.map(|(direction, counterpart)| (direction, counterpart, relation))
        })
        .filter(|(_, _, relation)| !normalize(&relation.relation_type).is_empty())
        .map(|(direction, counterpart, relation)| {
            let counterpart_type = entity_types
                .get(counterpart.as_str())
                .ok_or_else(|| CanonicalError::DanglingRelation(counterpart.clone()))?;
            Ok(format!(
                "{direction} | {} | {}: {}",
                quote(&normalize(&relation.relation_type))?,
                quote(&normalize(counterpart_type))?,
                quote(&normalize(counterpart.as_str()))?,
            ))
        })
        .collect()
}

fn normalize(value: &str) -> String {
    value
        .replace("\r\n", "\n")
        .replace('\r', "\n")
        .nfc()
        .collect()
}

fn quote(value: &str) -> Result<String, CanonicalError> {
    serde_json::to_string(value).map_err(CanonicalError::JsonEncoding)
}

fn truncate(text: String, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text;
    }

    let prefix_len = max_chars - TRUNCATION_SUFFIX.chars().count();
    let prefix = text.chars().take(prefix_len).collect::<String>();
    format!("{prefix}{TRUNCATION_SUFFIX}")
}

#[derive(Debug, Error)]
pub enum CanonicalError {
    #[error("could not JSON-encode canonical text")]
    JsonEncoding(serde_json::Error),
    #[error("representation version must not be blank")]
    BlankRepresentationVersion,
    #[error("max_input_chars must exceed the truncation suffix length")]
    MaxInputCharsTooSmall,
    #[error("relation references an entity absent from the graph: {0:?}")]
    DanglingRelation(EntityName),
}
