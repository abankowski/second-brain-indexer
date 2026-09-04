use std::{
    fs::{File, OpenOptions},
    io,
    path::Path,
};

use fs2::FileExt;
use thiserror::Error;

/// An advisory operating-system lock, released automatically if the process
/// exits. The lock file is intentionally retained: a stale pathname does not
/// imply a held lock, whereas deleting it creates a race between processes.
pub struct ProcessLock {
    _file: File,
}

impl ProcessLock {
    pub fn acquire(path: &Path) -> Result<Self, ProcessLockError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(ProcessLockError::Io)?;
        file.try_lock_exclusive()
            .map_err(|error| match error.kind() {
                io::ErrorKind::WouldBlock => ProcessLockError::AlreadyLocked,
                _ => ProcessLockError::Io(error),
            })?;
        Ok(Self { _file: file })
    }
}

#[derive(Debug, Error)]
pub enum ProcessLockError {
    #[error("another second-brain-indexer process already holds the lock")]
    AlreadyLocked,
    #[error("unable to acquire the process lock: {0}")]
    Io(#[source] io::Error),
}
