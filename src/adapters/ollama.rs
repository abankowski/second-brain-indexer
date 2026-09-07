use async_trait::async_trait;
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::{
    config::{EmbeddingConfig, EmbeddingEngine},
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
        let client = Client::builder()
            .build()
            .map_err(|_| EmbeddingError::Transport)?;
        let mut base_url = match &config.engine {
            EmbeddingEngine::Ollama { base_url } => base_url.clone(),
            EmbeddingEngine::OpenAiCompatible { .. } => {
                return Err(EmbeddingError::InvalidResponse);
            }
        };
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
