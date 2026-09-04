use std::sync::atomic::{AtomicU64, Ordering};

/// Fixed label values prevent user-controlled names, observations and secrets
/// from becoming Prometheus labels.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RunOutcome {
    Succeeded,
    Partial,
    Failed,
}

impl RunOutcome {
    const fn label(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Partial => "partial",
            Self::Failed => "failed",
        }
    }
    const fn index(self) -> usize {
        match self {
            Self::Succeeded => 0,
            Self::Partial => 1,
            Self::Failed => 2,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeErrorClass {
    McpTransport,
    EmbeddingTransport,
    State,
    ShutdownDeadline,
}

impl RuntimeErrorClass {
    const fn label(self) -> &'static str {
        match self {
            Self::McpTransport => "mcp_transport",
            Self::EmbeddingTransport => "embedding_transport",
            Self::State => "state",
            Self::ShutdownDeadline => "shutdown_deadline",
        }
    }
    const fn index(self) -> usize {
        match self {
            Self::McpTransport => 0,
            Self::EmbeddingTransport => 1,
            Self::State => 2,
            Self::ShutdownDeadline => 3,
        }
    }
}

#[derive(Default)]
pub struct Metrics {
    runs: [AtomicU64; 3],
    errors: [AtomicU64; 4],
}

impl Metrics {
    pub fn record_run(&self, outcome: RunOutcome) {
        self.runs[outcome.index()].fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_error(&self, class: RuntimeErrorClass) {
        self.errors[class.index()].fetch_add(1, Ordering::Relaxed);
    }

    pub fn render(&self) -> String {
        let outcomes = [
            RunOutcome::Succeeded,
            RunOutcome::Partial,
            RunOutcome::Failed,
        ];
        let errors = [
            RuntimeErrorClass::McpTransport,
            RuntimeErrorClass::EmbeddingTransport,
            RuntimeErrorClass::State,
            RuntimeErrorClass::ShutdownDeadline,
        ];
        let mut output = String::from("# TYPE second_brain_indexer_runs_total counter\n");
        for outcome in outcomes {
            output.push_str(&format!(
                "second_brain_indexer_runs_total{{outcome=\"{}\"}} {}\n",
                outcome.label(),
                self.runs[outcome.index()].load(Ordering::Relaxed)
            ));
        }
        output.push_str("# TYPE second_brain_indexer_errors_total counter\n");
        for class in errors {
            output.push_str(&format!(
                "second_brain_indexer_errors_total{{class=\"{}\"}} {}\n",
                class.label(),
                self.errors[class.index()].load(Ordering::Relaxed)
            ));
        }
        output
    }
}
