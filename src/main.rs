use std::{path::PathBuf, sync::Arc, time::Duration};

use async_trait::async_trait;
use second_brain_indexer::{
    adapters::{
        mcp::StreamableHttpMcpAdapter, openai::OpenAiEmbeddingAdapter,
        sqlite::SqliteStateRepository,
    },
    application::execute_run::RunExecutor,
    config::{AppConfig, EmbeddingConfig, EmbeddingEngine, EnvironmentSecretLookup, parse_toml},
    domain::model::{ClaimedRun, RunId},
    http::{
        BearerAuth, GenerationView, HttpDependencyError, HttpQueryPort, PollingView,
        RunSummaryView, RunView, StatsView, StatusView, router_with_shutdown_and_auth,
    },
    ports::{EmbeddingError, EmbeddingProvider, McpMemoryPort},
    runtime::{
        bootstrap::Bootstrap,
        logging::record_execution_report,
        metrics::Metrics,
        scheduler::PollScheduler,
        shutdown::Shutdown,
        worker::{RunProcessor, Worker},
    },
};
use sqlx::Row;
use time::OffsetDateTime;
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

const VERSION: &str = env!("CARGO_PKG_VERSION");
const IDLE_WORKER_DELAY: Duration = Duration::from_millis(100);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    initialise_logging();
    let config_path = config_path()?;
    let config = load_config(&config_path)?;
    tracing::info!(
        event = "configuration_loaded",
        config_path = %config_path.display(),
        "configuration accepted"
    );
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

fn build_embedding_provider(
    config: &EmbeddingConfig,
) -> Result<Arc<dyn EmbeddingProvider>, EmbeddingError> {
    match config.engine {
        EmbeddingEngine::OpenAiCompatible { .. } => {
            OpenAiEmbeddingAdapter::new(config).map(|provider| Arc::new(provider) as _)
        }
        EmbeddingEngine::Ollama { .. } => Err(EmbeddingError::InvalidResponse),
    }
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
    tracing::info!(
        event = "mcp_dimension_verified",
        mcp_vector_dimension = actual_dimension,
        configured_dimension = config.embedding.dimensions.get(),
        "MCP session established and vector-store dimension verified"
    );
    let embedding = build_embedding_provider(&config.embedding)?;
    let shutdown = Shutdown::new();
    let metrics = Arc::new(Metrics::default());
    let query = Arc::new(RuntimeQuery::new(
        Arc::clone(&state),
        Arc::clone(&metrics),
        config.clone(),
        shutdown.clone(),
    ));
    let app = router_with_shutdown_and_auth(
        Arc::clone(&state),
        Arc::clone(&mcp),
        query,
        time::Duration::try_from(config.api.idempotency_ttl)
            .map_err(|_| "idempotency TTL is outside the supported range")?,
        shutdown.clone(),
        config
            .api
            .bearer_token
            .clone()
            .map_or_else(BearerAuth::disabled, BearerAuth::enabled),
    );
    let address = config.server.bind;
    let listener = TcpListener::bind(address).await?;
    let processor = Arc::new(ProductionProcessor {
        state: Arc::clone(&state),
        mcp,
        embedding,
        metrics: Arc::clone(&metrics),
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
    tracing::info!(
        event = "indexer_ready",
        %address,
        mcp_vector_dimension = actual_dimension,
        polling_enabled = config.polling.enabled,
        polling_interval_seconds = config.polling.interval.as_secs(),
        message = %startup_ready_message(
            address,
            actual_dimension,
            config.polling.enabled,
            config.polling.interval,
        ),
    );
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

fn initialise_logging() {
    let filter = std::env::var("RUST_LOG")
        .ok()
        .and_then(|value| EnvFilter::try_new(value).ok())
        .unwrap_or_else(|| EnvFilter::new("info"));
    match log_format(std::env::var("INDEXER_LOG_FORMAT").ok().as_deref()) {
        LogFormat::Text => tracing_subscriber::fmt().with_env_filter(filter).init(),
        LogFormat::Json => tracing_subscriber::fmt()
            .json()
            .with_env_filter(filter)
            .init(),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LogFormat {
    Text,
    Json,
}

fn log_format(value: Option<&str>) -> LogFormat {
    match value {
        Some("json") => LogFormat::Json,
        _ => LogFormat::Text,
    }
}

fn startup_ready_message(
    address: std::net::SocketAddr,
    dimension: u32,
    polling_enabled: bool,
    polling_interval: Duration,
) -> String {
    let polling = if polling_enabled {
        format!("polling enabled every {}s", polling_interval.as_secs())
    } else {
        "polling disabled".to_owned()
    };
    format!(
        "indexer ready: listening on {address}; MCP vector store dimension {dimension}; {polling}"
    )
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
    embedding: Arc<dyn EmbeddingProvider>,
    metrics: Arc<Metrics>,
    config: AppConfig,
}

#[async_trait]
impl RunProcessor for ProductionProcessor {
    async fn execute(&self, claimed: ClaimedRun) -> Result<(), String> {
        let embedding = ProviderRef(&*self.embedding);
        RunExecutor::new(
            &*self.mcp,
            &embedding,
            &*self.state,
            &self.config.mcp,
            &self.config.embedding,
            &self.config.representation,
            &self.config.retry,
        )
        .execute(claimed, OffsetDateTime::now_utc())
        .await
        .map(|report| record_execution_report(&self.metrics, &report))
        .map_err(|error| error.to_string())
    }
}

struct ProviderRef<'a>(&'a dyn EmbeddingProvider);

#[async_trait]
impl EmbeddingProvider for ProviderRef<'_> {
    async fn embed(
        &self,
        inputs: &[String],
    ) -> Result<Vec<second_brain_indexer::domain::model::Embedding>, EmbeddingError> {
        self.0.embed(inputs).await
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

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, num::NonZeroU32, time::Duration};

    use second_brain_indexer::{
        config::{EmbeddingConfig, EmbeddingEngine},
        domain::model::Dimension,
        ports::EmbeddingError,
    };
    use secrecy::SecretString;
    use url::Url;

    use super::{LogFormat, build_embedding_provider, log_format, startup_ready_message};

    #[test]
    fn startup_message_reports_listener_dimension_and_polling_state() {
        let address: SocketAddr = "127.0.0.1:9184".parse().expect("test address is valid");

        assert_eq!(
            startup_ready_message(address, 384, true, Duration::from_secs(900)),
            "indexer ready: listening on 127.0.0.1:9184; MCP vector store dimension 384; polling enabled every 900s"
        );
    }

    #[test]
    fn direct_execution_defaults_to_text_and_json_is_opt_in() {
        assert_eq!(log_format(None), LogFormat::Text);
        assert_eq!(log_format(Some("json")), LogFormat::Json);
    }

    #[test]
    fn provider_factory_rejects_ollama_until_its_native_adapter_is_available() {
        let config = EmbeddingConfig {
            engine: EmbeddingEngine::Ollama {
                base_url: Url::parse("http://127.0.0.1:11434").expect("test URL is valid"),
            },
            model: "bge-m3".to_owned(),
            dimensions: Dimension::parse(1024).expect("test dimension is valid"),
            max_input_chars: NonZeroU32::new(24_000).expect("non-zero input limit"),
            max_input_tokens: NonZeroU32::new(8_192).expect("non-zero token limit"),
        };

        assert!(matches!(
            build_embedding_provider(&config),
            Err(EmbeddingError::InvalidResponse)
        ));
    }

    #[test]
    fn provider_factory_constructs_the_openai_compatible_engine() {
        let config = EmbeddingConfig {
            engine: EmbeddingEngine::OpenAiCompatible {
                base_url: Url::parse("https://api.openai.com/v1").expect("test URL is valid"),
                api_key: SecretString::from("test-secret"),
            },
            model: "text-embedding-3-small".to_owned(),
            dimensions: Dimension::parse(384).expect("test dimension is valid"),
            max_input_chars: NonZeroU32::new(24_000).expect("non-zero input limit"),
            max_input_tokens: NonZeroU32::new(8_192).expect("non-zero token limit"),
        };

        assert!(build_embedding_provider(&config).is_ok());
    }
}
