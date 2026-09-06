use std::time::Duration;

use crate::config::RetryConfig;
use crate::ports::{EmbeddingError, McpError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryClass {
    Retryable,
    Permanent,
}

pub fn classify_mcp(error: &McpError) -> RetryClass {
    match error {
        McpError::Transport(_) | McpError::RateLimited | McpError::Server => RetryClass::Retryable,
        McpError::Unauthorized | McpError::InvalidResponse => RetryClass::Permanent,
    }
}

pub fn classify_embedding(error: &EmbeddingError) -> RetryClass {
    match error {
        EmbeddingError::Transport | EmbeddingError::RateLimited | EmbeddingError::Server => {
            RetryClass::Retryable
        }
        EmbeddingError::Unauthorized | EmbeddingError::InvalidResponse => RetryClass::Permanent,
    }
}

/// The deterministic jitter is deliberately bounded in `[75%, 100%]` of the
/// capped exponential backoff. A deterministic basis makes the policy testable
/// while distributing different batches without a random-number dependency.
pub fn retry_delay(config: &RetryConfig, failed_attempt: u32, jitter_basis: u64) -> Duration {
    let exponent = failed_attempt.saturating_sub(1).min(63);
    let multiplier = 1_u128 << exponent;
    let capped_ms = u128::min(
        config.base_delay.as_millis().saturating_mul(multiplier),
        config.max_delay.as_millis(),
    );
    let jitter_percent = 75_u128 + u128::from(jitter_basis % 26);
    let millis = capped_ms.saturating_mul(jitter_percent) / 100;
    Duration::from_millis(u64::try_from(millis).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroU32, time::Duration};

    use super::{RetryClass, classify_embedding, classify_mcp, retry_delay};
    use crate::{
        config::RetryConfig,
        ports::{EmbeddingError, McpError},
    };

    fn config() -> RetryConfig {
        RetryConfig {
            max_attempts: NonZeroU32::new(3).expect("non-zero attempts"),
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_millis(1_000),
        }
    }

    #[test]
    fn rate_limits_retry_but_authentication_does_not() {
        assert_eq!(classify_mcp(&McpError::RateLimited), RetryClass::Retryable);
        assert_eq!(classify_mcp(&McpError::Unauthorized), RetryClass::Permanent);
        assert_eq!(
            classify_embedding(&EmbeddingError::RateLimited),
            RetryClass::Retryable
        );
        assert_eq!(
            classify_embedding(&EmbeddingError::Unauthorized),
            RetryClass::Permanent
        );
    }

    #[test]
    fn jitter_stays_inside_the_configured_bounds() {
        let delay = retry_delay(&config(), 2, 7);
        assert!((Duration::from_millis(150)..=Duration::from_millis(200)).contains(&delay));
    }
}
