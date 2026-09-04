use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use async_trait::async_trait;
use second_brain_indexer::{
    adapters::{
        mcp::StreamableHttpMcpAdapter, openai::OpenAiEmbeddingAdapter,
        sqlite::SqliteStateRepository,
    },
    application::execute_run::RunExecutor,
    config::{AppConfig, EnvironmentSecretLookup, parse_toml},
    domain::model::{ClaimedRun, RunId},
    http::{
        GenerationView, HttpDependencyError, HttpQueryPort, PollingView, RunSummaryView, RunView,
        StatsView, StatusView, router_with_shutdown,
    },
    ports::McpMemoryPort,
    runtime::{
        bootstrap::Bootstrap,
        metrics::Metrics,
        scheduler::PollScheduler,
        shutdown::Shutdown,
        worker::{RunProcessor, Worker},
    },
};
use sqlx::Row;
use time::OffsetDateTime;
use tokio::net::TcpListener;

const VERSION: &str = env!("CARGO_PKG_VERSION");
const IDLE_WORKER_DELAY: Duration = Duration::from_millis(100);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let config_path = config_path()?;
    let config = load_config(&config_path)?;
    run(config).await
}

fn config_path() -> Result<PathBuf, String> {
    let mut arguments = std::env::args_os().skip(1);
    match (arguments.next(), arguments.next()) {
        (Some(flag), Some(path)) if flag == "--config" && arguments.next().is_none() => {
            Ok(PathBuf::from(path))
        }
        _ => Err("usage: second-brain-indexer --config <path>".to_owned()),
    }
}

fn load_config(path: &PathBuf) -> Result<AppConfig, String> {
    let source = std::fs::read_to_string(path)
        .map_err(|error| format!("could not read configuration {}: {error}", path.display()))?;
    parse_toml(&source, &EnvironmentSecretLookup).map_err(|error| error.to_string())
}

async fn run(config: AppConfig) -> Result<(), Box<dyn std::error::Error>> {
    let state = Arc::new(
        SqliteStateRepository::connect(&config.state.database_path, config.state.lease_duration)
            .await?,
    );
    let _bootstrap = Bootstrap::start(
        Arc::clone(&state),
        config.state.lock_path.clone(),
        OffsetDateTime::now_utc(),
    )
    .await?;
    let mcp = Arc::new(StreamableHttpMcpAdapter::new(&config.mcp)?);
    let actual_dimension = mcp.vector_dimension().await?;
    if actual_dimension != config.embedding.dimensions.get() {
        return Err(format!(
            "MCP vector dimension {actual_dimension} does not match configured dimension {}",
            config.embedding.dimensions.get()
        )
        .into());
    }
    let embedding = Arc::new(OpenAiEmbeddingAdapter::new(&config.embedding)?);
    let shutdown = Shutdown::new();
    let metrics = Arc::new(Metrics::default());
    let query = Arc::new(RuntimeQuery::new(
        Arc::clone(&state),
        Arc::clone(&metrics),
        config.clone(),
        shutdown.clone(),
    ));
    let app = router_with_shutdown(
        Arc::clone(&state),
        Arc::clone(&mcp),
        query,
        time::Duration::try_from(config.api.idempotency_ttl)
            .map_err(|_| "idempotency TTL is outside the supported range")?,
        shutdown.clone(),
    );
    let address: SocketAddr = config.server.bind.parse()?;
    let listener = TcpListener::bind(address).await?;
    let processor = Arc::new(ProductionProcessor {
        state: Arc::clone(&state),
        mcp,
        embedding,
        config: config.clone(),
    });
    let worker_shutdown = shutdown.clone();
    let worker = Worker::new("second-brain-indexer", state.clone(), processor);
    let worker_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                () = worker_shutdown.cancelled() => return,
                result = worker.process_once(OffsetDateTime::now_utc()) => match result {
                    Ok(true) => {},
                    Ok(false) => tokio::time::sleep(IDLE_WORKER_DELAY).await,
                    Err(error) => {
                        tracing::warn!(event = "worker_error", error = %error, "worker iteration failed");
                        tokio::time::sleep(IDLE_WORKER_DELAY).await;
                    }
                },
            }
        }
    });
    let scheduler_task = config.polling.enabled.then(|| {
        let scheduler = PollScheduler::new(state, config.polling.interval, shutdown.clone());
        tokio::spawn(async move {
            if let Err(error) = scheduler.run(config.polling.run_on_start).await {
                tracing::warn!(event = "scheduler_error", error = %error, "scheduler stopped");
            }
        })
    });
    tracing::info!(event = "indexer_started", %address, "second brain indexer started");
    axum::serve(listener, app)
        .with_graceful_shutdown(wait_for_shutdown(shutdown.clone()))
        .await?;
    shutdown.begin();
    worker_task.abort();
    if let Some(task) = scheduler_task {
        task.abort();
    }
    Ok(())
}

async fn wait_for_shutdown(shutdown: Shutdown) {
    #[cfg(unix)]
    {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut terminate) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {},
                    _ = terminate.recv() => {},
                }
            }
            Err(error) => {
                tracing::warn!(event = "signal_setup_error", error = %error, "SIGTERM handler unavailable; waiting for SIGINT");
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
    shutdown.begin();
}

struct ProductionProcessor {
    state: Arc<SqliteStateRepository>,
    mcp: Arc<StreamableHttpMcpAdapter>,
    embedding: Arc<OpenAiEmbeddingAdapter>,
    config: AppConfig,
}

#[async_trait]
impl RunProcessor for ProductionProcessor {
    async fn execute(&self, claimed: ClaimedRun) -> Result<(), String> {
        RunExecutor::new(
            &*self.mcp,
            &*self.embedding,
            &*self.state,
            &self.config.mcp,
            &self.config.embedding,
            &self.config.representation,
            &self.config.retry,
        )
        .execute(claimed, OffsetDateTime::now_utc())
        .await
        .map(|_| ())
        .map_err(|error| error.to_string())
    }
}

struct RuntimeQuery {
    state: Arc<SqliteStateRepository>,
    metrics: Arc<Metrics>,
    config: AppConfig,
    shutdown: Shutdown,
}

impl RuntimeQuery {
    fn new(
        state: Arc<SqliteStateRepository>,
        metrics: Arc<Metrics>,
        config: AppConfig,
        shutdown: Shutdown,
    ) -> Self {
        Self {
            state,
            metrics,
            config,
            shutdown,
        }
    }
}

#[async_trait]
impl HttpQueryPort for RuntimeQuery {
    async fn status(&self) -> Result<StatusView, HttpDependencyError> {
        let row =
            sqlx::query("SELECT id, status FROM run ORDER BY requested_at DESC, id DESC LIMIT 1")
                .fetch_optional(self.state.pool())
                .await
                .map_err(|_| HttpDependencyError::Unavailable)?;
        Ok(StatusView {
            ready: self.shutdown.is_accepting(),
            version: VERSION.to_owned(),
            active_generation: Some(GenerationView {
                taxonomy_version: self.config.representation.taxonomy_version.clone(),
                representation_version: self.config.representation.version.clone(),
                embedding_model: self.config.embedding.model.clone(),
                dimensions: self.config.embedding.dimensions.get(),
            }),
            polling: PollingView {
                enabled: self.config.polling.enabled,
                interval_seconds: self.config.polling.interval.as_secs(),
                next_run_at: None,
            },
            run: RunSummaryView {
                in_progress: row
                    .as_ref()
                    .is_some_and(|row| row.get::<String, _>("status") == "running"),
                last_run_id: row
                    .as_ref()
                    .and_then(|row| RunId::parse(row.get("id")).ok()),
                last_run_status: row.map(|row| row.get("status")),
            },
        })
    }

    async fn stats(&self) -> Result<StatsView, HttpDependencyError> {
        let rows =
            sqlx::query("SELECT status, COUNT(*) AS count FROM entity_index_state GROUP BY status")
                .fetch_all(self.state.pool())
                .await
                .map_err(|_| HttpDependencyError::Unavailable)?;
        let count = |status: &str| -> Result<u64, HttpDependencyError> {
            rows.iter()
                .find(|row| row.get::<String, _>("status") == status)
                .map_or(Ok(0), |row| {
                    u64::try_from(row.get::<i64, _>("count"))
                        .map_err(|_| HttpDependencyError::Unavailable)
                })
        };
        Ok(StatsView {
            indexed: count("indexed")?,
            pending: count("pending")?,
            indexing: count("indexing")?,
            failed: count("failed")?,
            delete_pending: count("delete_pending")?,
            deleted: count("deleted")?,
            last_success_at: None,
            last_run_changes: 0,
        })
    }

    async fn run(&self, run_id: &RunId) -> Result<Option<RunView>, HttpDependencyError> {
        let row = sqlx::query("SELECT id, status, selector_kind, selector_value, requested_at, started_at, finished_at, entities_seen, entities_indexed, entities_skipped, entities_deleted, entities_failed FROM run WHERE id = ?")
            .bind(run_id.as_str()).fetch_optional(self.state.pool()).await
            .map_err(|_| HttpDependencyError::Unavailable)?;
        row.map(run_view).transpose()
    }

    async fn metrics(&self) -> Result<String, HttpDependencyError> {
        Ok(self.metrics.render())
    }
}

fn run_view(row: sqlx::sqlite::SqliteRow) -> Result<RunView, HttpDependencyError> {
    let selector = match (
        row.get::<String, _>("selector_kind").as_str(),
        row.get::<Option<String>, _>("selector_value"),
    ) {
        ("full", None) => second_brain_indexer::http::SelectorResponse::Full { full: true },
        ("entity", Some(name)) => second_brain_indexer::http::SelectorResponse::Entity {
            entity: second_brain_indexer::http::EntityResponse { name },
        },
        ("entity_type", Some(entity_type)) => {
            second_brain_indexer::http::SelectorResponse::EntityType { entity_type }
        }
        _ => return Err(HttpDependencyError::Unavailable),
    };
    let timestamp = |column: &str| -> Result<Option<String>, HttpDependencyError> {
        row.try_get::<Option<String>, _>(column)
            .map_err(|_| HttpDependencyError::Unavailable)
    };
    let count = |column: &str| -> Result<u64, HttpDependencyError> {
        u64::try_from(
            row.try_get::<i64, _>(column)
                .map_err(|_| HttpDependencyError::Unavailable)?,
        )
        .map_err(|_| HttpDependencyError::Unavailable)
    };
    Ok(RunView {
        run_id: row.get("id"),
        status: row.get("status"),
        selector,
        requested_at: row.get("requested_at"),
        started_at: timestamp("started_at")?,
        finished_at: timestamp("finished_at")?,
        entities_seen: count("entities_seen")?,
        entities_indexed: count("entities_indexed")?,
        entities_skipped: count("entities_skipped")?,
        entities_deleted: count("entities_deleted")?,
        entities_failed: count("entities_failed")?,
    })
}
