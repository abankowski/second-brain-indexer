use std::time::Duration;

use async_trait::async_trait;
use reqwest::{Client, StatusCode, header};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::{
    config::{DEFAULT_EMBEDDING_REQUEST_TIMEOUT, EmbeddingConfig, EmbeddingEngine},
    domain::model::{Dimension, Embedding},
    ports::{EmbeddingError, EmbeddingProvider},
};

pub struct OpenAiEmbeddingAdapter {
    client: Client,
    endpoint: Url,
    api_key: SecretString,
    model: String,
    dimension: Dimension,
}

impl OpenAiEmbeddingAdapter {
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
        let client = Client::builder()
            .timeout(request_timeout)
            .build()
            .map_err(|_| EmbeddingError::Transport)?;
        let (mut base_url, api_key) = match &config.engine {
            EmbeddingEngine::OpenAiCompatible { base_url, api_key } => {
                (base_url.clone(), api_key.clone())
            }
            EmbeddingEngine::Ollama { .. } => return Err(EmbeddingError::InvalidResponse),
        };
        if !base_url.path().ends_with('/') {
            base_url.set_path(&format!("{}/", base_url.path()));
        }
        let endpoint = base_url
            .join("embeddings")
            .map_err(|_| EmbeddingError::InvalidResponse)?;
        Ok(Self {
            client,
            endpoint,
            api_key,
            model: config.model.clone(),
            dimension: config.dimensions,
        })
    }
}

#[async_trait]
impl EmbeddingProvider for OpenAiEmbeddingAdapter {
    async fn embed(&self, inputs: &[String]) -> Result<Vec<Embedding>, EmbeddingError> {
        let response = self
            .client
            .post(self.endpoint.clone())
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", self.api_key.expose_secret()),
            )
            .json(&EmbeddingRequest {
                model: &self.model,
                input: inputs,
                encoding_format: "float",
                dimensions: self.dimension.get(),
            })
            .send()
            .await
            .map_err(|_| EmbeddingError::Transport)?;
        classify_status(response.status())?;
        let payload: EmbeddingResponse = response
            .json()
            .await
            .map_err(|_| EmbeddingError::InvalidResponse)?;
        ordered_embeddings(payload.data, inputs.len(), self.dimension)
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

fn ordered_embeddings(
    values: Vec<EmbeddingResponseItem>,
    expected_count: usize,
    dimension: Dimension,
) -> Result<Vec<Embedding>, EmbeddingError> {
    if values.len() != expected_count {
        return Err(EmbeddingError::InvalidResponse);
    }
    let mut ordered = values
        .into_iter()
        .map(|item| {
            let embedding = Embedding::new(item.embedding, dimension)
                .map_err(|_| EmbeddingError::InvalidResponse)?;
            Ok((item.index, embedding))
        })
        .collect::<Result<Vec<_>, EmbeddingError>>()?;
    ordered.sort_by_key(|(index, _)| *index);
    if ordered
        .iter()
        .enumerate()
        .any(|(expected, (actual, _))| expected != *actual)
    {
        return Err(EmbeddingError::InvalidResponse);
    }
    Ok(ordered
        .into_iter()
        .map(|(_, embedding)| embedding)
        .collect())
}

#[derive(Serialize)]
struct EmbeddingRequest<'a> {
    model: &'a str,
    input: &'a [String],
    encoding_format: &'static str,
    dimensions: u32,
}

#[derive(Deserialize)]
struct EmbeddingResponse {
    data: Vec<EmbeddingResponseItem>,
}

#[derive(Deserialize)]
struct EmbeddingResponseItem {
    index: usize,
    embedding: Vec<f32>,
}

#[cfg(test)]
mod tests {
    use super::{EmbeddingResponseItem, OpenAiEmbeddingAdapter, ordered_embeddings};
    use crate::{
        config::{EmbeddingConfig, EmbeddingEngine},
        domain::model::Dimension,
        ports::EmbeddingError,
    };
    use std::num::NonZeroU32;
    use url::Url;

    #[test]
    fn restores_response_index_order() {
        let dimension = Dimension::parse(2).expect("test dimension is valid");
        let embeddings = ordered_embeddings(
            vec![
                EmbeddingResponseItem {
                    index: 1,
                    embedding: vec![2.0, 2.1],
                },
                EmbeddingResponseItem {
                    index: 0,
                    embedding: vec![1.0, 1.1],
                },
            ],
            2,
            dimension,
        )
        .expect("response has all indexed vectors");
        assert_eq!(embeddings[0].values(), &[1.0, 1.1]);
        assert_eq!(embeddings[1].values(), &[2.0, 2.1]);
    }

    #[test]
    fn rejects_an_ollama_configuration() {
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
            OpenAiEmbeddingAdapter::new(&config),
            Err(EmbeddingError::InvalidResponse)
        ));
    }

    #[test]
    fn rejects_duplicate_or_missing_response_indexes() {
        let dimension = Dimension::parse(2).expect("test dimension is valid");
        let result = ordered_embeddings(
            vec![
                EmbeddingResponseItem {
                    index: 0,
                    embedding: vec![1.0, 1.1],
                },
                EmbeddingResponseItem {
                    index: 0,
                    embedding: vec![2.0, 2.1],
                },
            ],
            2,
            dimension,
        );
        assert!(result.is_err());
    }
}
