use std::{num::NonZeroU32, path::PathBuf, time::Duration};

use secrecy::SecretString;
use serde::Deserialize;
use thiserror::Error;
use url::Url;

use crate::domain::model::{ConfigFingerprint, Dimension, DomainError};

const MAX_MCP_BATCH_SIZE: u16 = 1024;

pub trait SecretLookup {
    fn get(&self, variable: &str) -> Option<SecretString>;
}

pub struct EnvironmentSecretLookup;

impl SecretLookup for EnvironmentSecretLookup {
    fn get(&self, variable: &str) -> Option<SecretString> {
        std::env::var(variable)
            .ok()
            .and_then(|value| (!value.is_empty()).then(|| SecretString::from(value)))
    }
}

#[derive(Clone, Debug)]
pub struct AppConfig {
    pub server: ServerConfig,
    pub polling: PollingConfig,
    pub state: StateConfig,
    pub mcp: McpConfig,
    pub embedding: EmbeddingConfig,
    pub representation: RepresentationConfig,
    pub retry: RetryConfig,
    pub api: ApiConfig,
}

impl AppConfig {
    pub fn fingerprint(&self) -> ConfigFingerprint {
        ConfigFingerprint {
            taxonomy_version: self.representation.taxonomy_version.clone(),
            representation_version: self.representation.version.clone(),
            embedding_model: self.embedding.model.clone(),
            dimensions: self.embedding.dimensions,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub bind: String,
    pub request_timeout: Duration,
}
#[derive(Clone, Debug)]
pub struct PollingConfig {
    pub enabled: bool,
    pub interval: Duration,
    pub run_on_start: bool,
}
#[derive(Clone, Debug)]
pub struct StateConfig {
    pub database_path: PathBuf,
    pub lock_path: PathBuf,
    pub lease_duration: Duration,
}
#[derive(Clone, Debug)]
pub struct McpConfig {
    pub transport: McpTransport,
    pub endpoint: Url,
    pub request_timeout: Duration,
    pub batch_size: u16,
    pub bearer_token: SecretString,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum McpTransport {
    StreamableHttp,
}
#[derive(Clone, Debug)]
pub struct EmbeddingConfig {
    pub model: String,
    pub dimensions: Dimension,
    pub max_input_chars: NonZeroU32,
    pub max_input_tokens: NonZeroU32,
    pub openai_base_url: Url,
    pub api_key: SecretString,
}
#[derive(Clone, Debug)]
pub struct RepresentationConfig {
    pub version: String,
    pub taxonomy_version: String,
}
#[derive(Clone, Debug)]
pub struct RetryConfig {
    pub max_attempts: NonZeroU32,
    pub base_delay: Duration,
    pub max_delay: Duration,
}
#[derive(Clone, Debug)]
pub struct ApiConfig {
    pub idempotency_ttl: Duration,
}

pub fn parse_toml(input: &str, secrets: &impl SecretLookup) -> Result<AppConfig, ConfigError> {
    let raw: RawConfig =
        toml::from_str(input).map_err(|error| ConfigError::Toml(error.to_string()))?;
    raw.validate(secrets)
}

#[derive(Deserialize)]
struct RawConfig {
    server: RawServerConfig,
    polling: RawPollingConfig,
    state: RawStateConfig,
    mcp: RawMcpConfig,
    embedding: RawEmbeddingConfig,
    representation: RawRepresentationConfig,
    retry: RawRetryConfig,
    api: RawApiConfig,
}

#[derive(Deserialize)]
struct RawServerConfig {
    bind: String,
    request_timeout_seconds: u64,
}
#[derive(Deserialize)]
struct RawPollingConfig {
    enabled: bool,
    interval_seconds: u64,
    run_on_start: bool,
}
#[derive(Deserialize)]
struct RawStateConfig {
    database_path: PathBuf,
    lock_path: PathBuf,
    lease_seconds: u64,
}
#[derive(Deserialize)]
struct RawMcpConfig {
    transport: String,
    endpoint: String,
    request_timeout_seconds: u64,
    batch_size: u16,
    bearer_token_env: String,
}
#[derive(Deserialize)]
struct RawEmbeddingConfig {
    model: String,
    dimensions: u32,
    max_input_chars: u32,
    max_input_tokens: u32,
    openai_base_url: String,
    api_key_env: String,
}
#[derive(Deserialize)]
struct RawRepresentationConfig {
    version: String,
    taxonomy_version: String,
}
#[derive(Deserialize)]
struct RawRetryConfig {
    max_attempts: u32,
    base_delay_ms: u64,
    max_delay_ms: u64,
}
#[derive(Deserialize)]
struct RawApiConfig {
    idempotency_ttl_hours: u64,
}

impl RawConfig {
    fn validate(self, secrets: &impl SecretLookup) -> Result<AppConfig, ConfigError> {
        let transport = match self.mcp.transport.as_str() {
            "streamable-http" => McpTransport::StreamableHttp,
            value => return Err(ConfigError::UnsupportedMcpTransport(value.to_owned())),
        };
        let dimensions = Dimension::parse(self.embedding.dimensions)?;
        let max_input_chars =
            non_zero(self.embedding.max_input_chars, "embedding.max_input_chars")?;
        let max_input_tokens = non_zero(
            self.embedding.max_input_tokens,
            "embedding.max_input_tokens",
        )?;
        let max_attempts = non_zero(self.retry.max_attempts, "retry.max_attempts")?;
        let endpoint = parse_http_url("mcp.endpoint", &self.mcp.endpoint)?;
        let openai_base_url =
            parse_http_url("embedding.openai_base_url", &self.embedding.openai_base_url)?;
        if self.mcp.batch_size == 0 || self.mcp.batch_size > MAX_MCP_BATCH_SIZE {
            return Err(ConfigError::InvalidBatchSize(self.mcp.batch_size));
        }

        Ok(AppConfig {
            server: ServerConfig {
                bind: required("server.bind", self.server.bind)?,
                request_timeout: non_zero_duration(
                    self.server.request_timeout_seconds,
                    "server.request_timeout_seconds",
                )?,
            },
            polling: PollingConfig {
                enabled: self.polling.enabled,
                interval: non_zero_duration(
                    self.polling.interval_seconds,
                    "polling.interval_seconds",
                )?,
                run_on_start: self.polling.run_on_start,
            },
            state: StateConfig {
                database_path: required_path("state.database_path", self.state.database_path)?,
                lock_path: required_path("state.lock_path", self.state.lock_path)?,
                lease_duration: non_zero_duration(self.state.lease_seconds, "state.lease_seconds")?,
            },
            mcp: McpConfig {
                transport,
                endpoint,
                request_timeout: non_zero_duration(
                    self.mcp.request_timeout_seconds,
                    "mcp.request_timeout_seconds",
                )?,
                batch_size: self.mcp.batch_size,
                bearer_token: required_secret(secrets, &self.mcp.bearer_token_env)?,
            },
            embedding: EmbeddingConfig {
                model: required("embedding.model", self.embedding.model)?,
                dimensions,
                max_input_chars,
                max_input_tokens,
                openai_base_url,
                api_key: required_secret(secrets, &self.embedding.api_key_env)?,
            },
            representation: RepresentationConfig {
                version: required("representation.version", self.representation.version)?,
                taxonomy_version: required(
                    "representation.taxonomy_version",
                    self.representation.taxonomy_version,
                )?,
            },
            retry: RetryConfig {
                max_attempts,
                base_delay: non_zero_millis(self.retry.base_delay_ms, "retry.base_delay_ms")?,
                max_delay: non_zero_millis(self.retry.max_delay_ms, "retry.max_delay_ms")?,
            },
            api: ApiConfig {
                idempotency_ttl: non_zero_duration(
                    self.api.idempotency_ttl_hours,
                    "api.idempotency_ttl_hours",
                )?,
            },
        })
    }
}

fn required(field: &'static str, value: String) -> Result<String, ConfigError> {
    (!value.trim().is_empty())
        .then_some(value)
        .ok_or(ConfigError::Blank(field))
}
fn required_path(field: &'static str, value: PathBuf) -> Result<PathBuf, ConfigError> {
    (!value.as_os_str().is_empty())
        .then_some(value)
        .ok_or(ConfigError::Blank(field))
}
fn non_zero(value: u32, field: &'static str) -> Result<NonZeroU32, ConfigError> {
    NonZeroU32::new(value).ok_or(ConfigError::Zero(field))
}
fn non_zero_duration(value: u64, field: &'static str) -> Result<Duration, ConfigError> {
    (value > 0)
        .then(|| Duration::from_secs(value))
        .ok_or(ConfigError::Zero(field))
}
fn non_zero_millis(value: u64, field: &'static str) -> Result<Duration, ConfigError> {
    (value > 0)
        .then(|| Duration::from_millis(value))
        .ok_or(ConfigError::Zero(field))
}
fn parse_http_url(field: &'static str, value: &str) -> Result<Url, ConfigError> {
    let url = Url::parse(value).map_err(|_| ConfigError::InvalidUrl(field))?;
    matches!(url.scheme(), "http" | "https")
        .then_some(url)
        .ok_or(ConfigError::InvalidUrl(field))
}
fn required_secret(
    secrets: &impl SecretLookup,
    variable: &str,
) -> Result<SecretString, ConfigError> {
    required("secret environment variable name", variable.to_owned())?;
    secrets
        .get(variable)
        .ok_or_else(|| ConfigError::MissingSecret(variable.to_owned()))
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("invalid TOML: {0}")]
    Toml(String),
    #[error("{0} must not be blank")]
    Blank(&'static str),
    #[error("{0} must be greater than zero")]
    Zero(&'static str),
    #[error("{0} must be an absolute HTTP(S) URL")]
    InvalidUrl(&'static str),
    #[error("unsupported MCP transport: {0}")]
    UnsupportedMcpTransport(String),
    #[error("mcp.batch_size must be in 1..=1024, got {0}")]
    InvalidBatchSize(u16),
    #[error("required secret is unavailable: {0}")]
    MissingSecret(String),
    #[error(transparent)]
    Domain(#[from] DomainError),
}

#[cfg(test)]
mod tests {
    use super::{SecretLookup, parse_toml};
    use secrecy::SecretString;
    use std::collections::BTreeMap;

    struct TestSecrets(BTreeMap<String, SecretString>);
    impl SecretLookup for TestSecrets {
        fn get(&self, variable: &str) -> Option<SecretString> {
            self.0.get(variable).cloned()
        }
    }
    fn secrets() -> TestSecrets {
        TestSecrets(BTreeMap::from([
            (
                "MCP_MEMORY_TOKEN".to_owned(),
                SecretString::from("mcp-secret"),
            ),
            (
                "OPENAI_API_KEY".to_owned(),
                SecretString::from("openai-secret"),
            ),
        ]))
    }
    fn config() -> String {
        r#"
[server]
bind = "127.0.0.1:9184"
request_timeout_seconds = 10
[polling]
enabled = true
interval_seconds = 900
run_on_start = false
[state]
database_path = "/tmp/state.db"
lock_path = "/tmp/state.lock"
lease_seconds = 60
[mcp]
transport = "streamable-http"
endpoint = "https://brain-1.tandk.pl/mcp"
request_timeout_seconds = 30
batch_size = 128
bearer_token_env = "MCP_MEMORY_TOKEN"
[embedding]
model = "target-compatible-model"
dimensions = 384
max_input_chars = 24000
max_input_tokens = 8192
openai_base_url = "https://api.openai.com/v1"
api_key_env = "OPENAI_API_KEY"
[representation]
version = "entity-v1"
taxonomy_version = "1.0"
[retry]
max_attempts = 5
base_delay_ms = 500
max_delay_ms = 30000
[api]
idempotency_ttl_hours = 24
"#
        .to_owned()
    }

    #[test]
    fn parses_target_compatible_configuration() {
        assert!(parse_toml(&config(), &secrets()).is_ok());
    }
    #[test]
    fn rejects_invalid_dimension_before_any_network_call() {
        assert!(
            parse_toml(
                &config().replace("dimensions = 384", "dimensions = 0"),
                &secrets()
            )
            .is_err()
        );
    }
    #[test]
    fn rejects_invalid_batch_sizes() {
        for batch in ["0", "1025"] {
            assert!(
                parse_toml(
                    &config().replace("batch_size = 128", &format!("batch_size = {batch}")),
                    &secrets()
                )
                .is_err()
            );
        }
    }
    #[test]
    fn rejects_invalid_urls_and_blank_versions() {
        assert!(
            parse_toml(
                &config().replace("https://brain-1.tandk.pl/mcp", "not-a-url"),
                &secrets()
            )
            .is_err()
        );
        assert!(
            parse_toml(
                &config().replace("version = \"entity-v1\"", "version = \" \""),
                &secrets()
            )
            .is_err()
        );
    }
    #[test]
    fn rejects_missing_secrets_without_exposing_their_values() {
        let error = parse_toml(&config(), &TestSecrets(BTreeMap::new()));
        assert!(error.is_err());
        assert!(!format!("{error:?}").contains("mcp-secret"));
    }

    #[test]
    fn fingerprint_changes_with_each_index_configuration_field() {
        let base = match parse_toml(&config(), &secrets()) {
            Ok(value) => value.fingerprint(),
            Err(error) => panic!("valid configuration rejected: {error}"),
        };
        for (from, to) in [
            ("taxonomy_version = \"1.0\"", "taxonomy_version = \"2.0\""),
            ("version = \"entity-v1\"", "version = \"entity-v2\""),
            (
                "model = \"target-compatible-model\"",
                "model = \"other-model\"",
            ),
            ("dimensions = 384", "dimensions = 385"),
        ] {
            let changed = match parse_toml(&config().replace(from, to), &secrets()) {
                Ok(value) => value.fingerprint(),
                Err(error) => panic!("changed configuration rejected: {error}"),
            };
            assert_ne!(base, changed);
        }
    }
}
