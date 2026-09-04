use std::{path::PathBuf, sync::Arc};

use thiserror::Error;
use time::OffsetDateTime;

use crate::{
    ports::{StateError, StateRepository},
    runtime::lock::{ProcessLock, ProcessLockError},
};

/// Holds the process lock for the entire lifetime of the running service.
pub struct Bootstrap {
    _lock: ProcessLock,
}

impl Bootstrap {
    pub async fn start<S>(
        state: Arc<S>,
        lock_path: PathBuf,
        now: OffsetDateTime,
    ) -> Result<Self, BootstrapError>
    where
        S: StateRepository + 'static,
    {
        let lock = ProcessLock::acquire(&lock_path)?;
        state.recover_after_exclusive_start(now).await?;
        Ok(Self { _lock: lock })
    }
}

#[derive(Debug, Error)]
pub enum BootstrapError {
    #[error(transparent)]
    Lock(#[from] ProcessLockError),
    #[error("state recovery failed: {0}")]
    State(#[from] StateError),
}
