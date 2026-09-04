//! Structured runtime events deliberately accept only bounded, non-sensitive
//! values. Entity names, observations, request bodies and credentials never
//! enter these functions.

use crate::runtime::metrics::{RunOutcome, RuntimeErrorClass};

pub fn run_finished(outcome: RunOutcome, indexed: u32, skipped: u32, deleted: u32, failed: u32) {
    tracing::info!(
        event = "index_run_finished",
        outcome = outcome_label(outcome),
        indexed,
        skipped,
        deleted,
        failed,
        "index run finished"
    );
}

pub fn runtime_error(class: RuntimeErrorClass) {
    tracing::warn!(
        event = "indexer_runtime_error",
        class = error_label(class),
        "indexer runtime error"
    );
}

const fn outcome_label(outcome: RunOutcome) -> &'static str {
    match outcome {
        RunOutcome::Succeeded => "succeeded",
        RunOutcome::Partial => "partial",
        RunOutcome::Failed => "failed",
    }
}

const fn error_label(class: RuntimeErrorClass) -> &'static str {
    match class {
        RuntimeErrorClass::McpTransport => "mcp_transport",
        RuntimeErrorClass::EmbeddingTransport => "embedding_transport",
        RuntimeErrorClass::State => "state",
        RuntimeErrorClass::ShutdownDeadline => "shutdown_deadline",
    }
}
