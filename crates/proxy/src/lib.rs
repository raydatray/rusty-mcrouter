//! Frontend protocol handling and proxy-thread orchestration.

mod config;
mod connection;
mod context;
mod error;
mod events;
mod generation;
mod handle;
mod message;
mod metrics;
mod proxy_set;
mod routing;
mod runtime;
mod server;
mod worker;

pub use crate::config::{
    FrontendConnectionOptions, ListenerConfig, ProxyInbox, ProxyShards, ProxyShared,
    ProxyThreadSetup, ThreadMode,
};
pub use crate::error::FrontendError;
pub use crate::events::{WorkerEvent, WorkerEventRecord, WorkerEventSink};
pub use crate::handle::ProxyHandle;
pub use crate::message::{ProxyCommand, ProxyRequest};
pub use crate::metrics::FrontendMetricsShard;
pub use crate::proxy_set::ProxySet;
pub use crate::server::{bind_listener, Server};
pub use crate::worker::ProxyWorker;
