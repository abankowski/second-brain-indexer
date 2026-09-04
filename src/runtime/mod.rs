//! Process lifecycle adapters. These modules own scheduling, exclusive process
//! ownership and observability; the application layer remains transport-free.

pub mod bootstrap;
pub mod lock;
pub mod logging;
pub mod metrics;
pub mod scheduler;
pub mod shutdown;
pub mod worker;
