use second_brain_indexer::domain::{
    canonical::{RepresentationSettings, canonicalize},
    model::{EntityName, EntityType, GraphEntity, GraphRelation},
};

fn entity(name: &str, entity_type: &str, observations: &[&str]) -> GraphEntity {
    GraphEntity {
        name: EntityName::parse(name.to_owned()).expect("test entity name is valid"),
        entity_type: EntityType::parse(entity_type.to_owned()).expect("test entity type is valid"),
        observations: observations.iter().map(ToString::to_string).collect(),
    }
}

fn settings() -> RepresentationSettings {
    RepresentationSettings {
        version: "entity-v1".to_owned(),
        max_input_chars: 1_000,
    }
}

#[test]
fn normalizes_unicode_newlines_sorts_and_deduplicates_observations() {
    let current = entity("Second Brain", "Projekt", &["Żółw\r\n", "Żółw\n", "Żółw\n"]);
    let document = canonicalize(&current, &[current.clone()], &[], &settings())
        .expect("canonicalization succeeds");
    assert_eq!(document.text.matches("Żółw").count(), 1);
    assert!(!document.text.contains('\r'));
}

#[test]
fn escapes_newlines_to_prevent_observation_structure_collisions() {
    let first = entity("A", "Projekt", &["a\n- b"]);
    let second = entity("A", "Projekt", &["a", "b"]);
    let first_document =
        canonicalize(&first, &[first.clone()], &[], &settings()).expect("first document is valid");
    let second_document = canonicalize(&second, &[second.clone()], &[], &settings())
        .expect("second document is valid");
    assert_ne!(first_document.sha256, second_document.sha256);
    assert!(first_document.text.contains("\\n"));
}

#[test]
fn renders_incoming_and_outgoing_relations_by_type_and_name() {
    let current = entity("Current", "Projekt", &[]);
    let person = entity("Artur", "Osoba", &[]);
    let technology = entity("mcp-memory", "Technologia", &[]);
    let relations = vec![
        GraphRelation {
            from: person.name.clone(),
            relation_type: "prowadzi".to_owned(),
            to: current.name.clone(),
        },
        GraphRelation {
            from: current.name.clone(),
            relation_type: "wykorzystuje".to_owned(),
            to: technology.name.clone(),
        },
    ];
    let document = canonicalize(
        &current,
        &[current.clone(), person, technology],
        &relations,
        &settings(),
    )
    .expect("relations are complete");
    assert!(
        document
            .text
            .contains("incoming | \"prowadzi\" | \"Osoba\": \"Artur\"")
    );
    assert!(
        document
            .text
            .contains("outgoing | \"wykorzystuje\" | \"Technologia\": \"mcp-memory\"")
    );
}

#[test]
fn truncates_on_a_unicode_boundary_and_ends_with_one_newline() {
    let current = entity("Żółw", "Projekt", &["😀😀😀😀😀😀😀😀"]);
    let document = canonicalize(
        &current,
        &[current.clone()],
        &[],
        &RepresentationSettings {
            version: "entity-v1".to_owned(),
            max_input_chars: 40,
        },
    )
    .expect("document can be truncated");
    assert!(document.text.ends_with("\n[truncated]\n"));
    assert!(document.text.is_char_boundary(document.text.len()));
    assert_eq!(document.text.chars().count(), 40);
}

#[test]
fn rejects_a_relation_whose_counterpart_is_not_in_the_graph() {
    let current = entity("Current", "Projekt", &[]);
    let missing = EntityName::parse("Missing".to_owned()).expect("test name is valid");
    let relation = GraphRelation {
        from: current.name.clone(),
        relation_type: "uses".to_owned(),
        to: missing,
    };
    assert!(canonicalize(&current, &[current.clone()], &[relation], &settings()).is_err());
}

#[test]
fn pins_the_entity_v1_golden_hash() {
    let current = entity("A", "Projekt", &["x"]);
    let document = canonicalize(&current, &[current.clone()], &[], &settings())
        .expect("golden document is valid");
    assert_eq!(
        document.sha256,
        "96766a464422dcae5556d3ffa6ea50ab36d2aa3f738b173906f9218df9241ad3"
    );
}
