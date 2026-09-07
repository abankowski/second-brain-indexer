use std::time::Duration;

use async_trait::async_trait;
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::{
    config::{DEFAULT_EMBEDDING_REQUEST_TIMEOUT, EmbeddingConfig, EmbeddingEngine},
    domain::model::{Dimension, Embedding},
    ports::{EmbeddingError, EmbeddingProvider},
};

pub struct OllamaEmbeddingAdapter {
    client: Client,
    endpoint: Url,
    model: String,
    dimension: Dimension,
}

impl OllamaEmbeddingAdapter {
    pub fn new(config: &EmbeddingConfig) -> Result<Self, EmbeddingError> {
        Self::new_with_timeout(config, DEFAULT_EMBEDDING_REQUEST_TIMEOUT)
    }

    pub fn new_with_timeout(
        config: &EmbeddingConfig,
        request_timeout: Duration,
    ) -> Result<Self, EmbeddingError> {
        if request_timeout.is_zero() {
            return Err(EmbeddingError::InvalidResponse);
        }
        let mut base_url = match &config.engine {
            EmbeddingEngine::Ollama { base_url } => base_url.clone(),
            EmbeddingEngine::OpenAiCompatible { .. } => {
                return Err(EmbeddingError::InvalidResponse);
            }
        };
        if !base_url.username().is_empty() || base_url.password().is_some() {
            return Err(EmbeddingError::InvalidResponse);
        }
        let client = Client::builder()
            .timeout(request_timeout)
            .build()
            .map_err(|_| EmbeddingError::Transport)?;
        if !base_url.path().ends_with('/') {
            base_url.set_path(&format!("{}/", base_url.path()));
        }
        let endpoint = base_url
            .join("api/embed")
            .map_err(|_| EmbeddingError::InvalidResponse)?;
        Ok(Self {
            client,
            endpoint,
            model: config.model.clone(),
            dimension: config.dimensions,
        })
    }
}

#[async_trait]
impl EmbeddingProvider for OllamaEmbeddingAdapter {
    async fn embed(&self, inputs: &[String]) -> Result<Vec<Embedding>, EmbeddingError> {
        let response = self
            .client
            .post(self.endpoint.clone())
            .json(&EmbeddingRequest {
                model: &self.model,
                input: inputs,
                truncate: false,
            })
            .send()
            .await
            .map_err(|_| EmbeddingError::Transport)?;
        classify_status(response.status())?;
        let payload: EmbeddingResponse = response
            .json()
            .await
            .map_err(|_| EmbeddingError::InvalidResponse)?;
        if payload.embeddings.len() != inputs.len() {
            return Err(EmbeddingError::InvalidResponse);
        }
        payload
            .embeddings
            .into_iter()
            .map(|values| {
                Embedding::new(values, self.dimension).map_err(|_| EmbeddingError::InvalidResponse)
            })
            .collect()
    }
}

fn classify_status(status: StatusCode) -> Result<(), EmbeddingError> {
    if status.is_success() {
        return Ok(());
    }
    match status {
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => Err(EmbeddingError::Unauthorized),
        StatusCode::TOO_MANY_REQUESTS => Err(EmbeddingError::RateLimited),
        value if value.is_server_error() => Err(EmbeddingError::Server),
        _ => Err(EmbeddingError::InvalidResponse),
    }
}

#[derive(Serialize)]
struct EmbeddingRequest<'a> {
    model: &'a str,
    input: &'a [String],
    truncate: bool,
}

#[derive(Deserialize)]
struct EmbeddingResponse {
    embeddings: Vec<Vec<f32>>,
}
