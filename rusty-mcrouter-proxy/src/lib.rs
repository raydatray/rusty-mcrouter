//! Frontend protocol handling and proxy-thread orchestration.

mod config;
mod connection;
mod error;
mod events;
mod handle;
mod message;
mod metrics;
#[allow(clippy::module_inception)]
mod proxy;
mod proxy_set;
mod server;
mod thread;
mod worker;

pub use config::{ListenerConfig, ProxyThreadConfig, ThreadMode};
pub use error::FrontendError;
pub use events::{WorkerEvent, WorkerEventRecord, WorkerEventSink};
pub use handle::ProxyHandle;
pub use message::{ProxyMessage, ProxyRequest};
pub use metrics::FrontendMetricsShard;
pub use proxy_set::ProxySet;
pub use server::Server;
pub use thread::proxy_thread_main;
