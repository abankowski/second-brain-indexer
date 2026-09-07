//! Process lifecycle adapters. These modules own scheduling, exclusive process
//! ownership and observability; the application layer remains transport-free.

use std::net::SocketAddr;

use crate::{
    domain::model::Dimension,
    ports::{EmbeddingError, EmbeddingProvider},
};
use tokio::net::TcpListener;

pub mod bootstrap;
pub mod lock;
pub mod logging;
pub mod metrics;
pub mod scheduler;
pub mod shutdown;
pub mod worker;

const STARTUP_EMBEDDING_PROBE: &str = "second-brain-indexer startup embedding probe";

pub async fn bind_after_embedding_probe(
    provider: &dyn EmbeddingProvider,
    expected: Dimension,
    address: SocketAddr,
) -> Result<TcpListener, EmbeddingError> {
    probe_embedding(provider, expected).await?;
    TcpListener::bind(address)
        .await
        .map_err(|_| EmbeddingError::Transport)
}

pub async fn probe_embedding(
    provider: &dyn EmbeddingProvider,
    expected: Dimension,
) -> Result<(), EmbeddingError> {
    let embeddings = provider
        .embed(&[STARTUP_EMBEDDING_PROBE.to_owned()])
        .await?;
    match embeddings.as_slice() {
        [embedding] if embedding.values().len() == expected.get() as usize => Ok(()),
        _ => Err(EmbeddingError::InvalidResponse),
    }
}
