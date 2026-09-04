use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use tokio::sync::watch;

/// Shared lifecycle state. A shutdown transition is monotonic: once requests
/// are refused, no scheduler or server task may resume accepting work.
#[derive(Clone)]
pub struct Shutdown {
    accepting: Arc<AtomicBool>,
    sender: watch::Sender<bool>,
}

impl Shutdown {
    pub fn new() -> Self {
        let (sender, _) = watch::channel(false);
        Self {
            accepting: Arc::new(AtomicBool::new(true)),
            sender,
        }
    }

    pub fn begin(&self) {
        self.accepting.store(false, Ordering::Release);
        self.sender.send_replace(true);
    }

    pub fn is_accepting(&self) -> bool {
        self.accepting.load(Ordering::Acquire)
    }

    pub async fn cancelled(&self) {
        if !self.is_accepting() {
            return;
        }
        let mut receiver = self.sender.subscribe();
        while !*receiver.borrow() {
            if receiver.changed().await.is_err() {
                return;
            }
        }
    }
}

impl Default for Shutdown {
    fn default() -> Self {
        Self::new()
    }
}
