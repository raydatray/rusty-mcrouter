//! Frontend protocol handling and proxy-thread orchestration.

mod config;
mod connection;
mod counters;
mod events;
mod handle;
mod message;
#[allow(clippy::module_inception)]
mod proxy;
mod proxy_set;
mod thread;
mod worker;

pub use config::{ListenerConfig, ProxyThreadConfig, ThreadMode};
pub use counters::FrontendCounters;
pub use events::{WorkerEvent, WorkerEventRecord, WorkerEventSink};
pub use handle::ProxyHandle;
pub use message::{ProxyMessage, ProxyRequest};
pub use proxy_set::ProxySet;
pub use thread::proxy_thread_main;
