//! Frontend protocol handling and proxy-thread orchestration.

mod config;
mod connection;
mod error;
mod events;
mod handle;
mod message;
mod metrics;
mod proxy_set;
mod routing;
mod runtime;
mod server;
mod thread;

pub use crate::config::{
    ListenerConfig, ProxyInbox, ProxyShards, ProxyShared, ProxyThreadConfig, ThreadMode,
};
pub use crate::error::FrontendError;
pub use crate::events::{WorkerEvent, WorkerEventRecord, WorkerEventSink};
pub use crate::handle::ProxyHandle;
pub use crate::message::{ProxyCommand, ProxyRequest};
pub use crate::metrics::FrontendMetricsShard;
pub use crate::proxy_set::ProxySet;
pub use crate::server::Server;
pub use crate::thread::proxy_thread_main;
