use std::{net::SocketAddr, sync::Arc, time::Duration};

use rusty_mcrouter_backend::{
    destination::{DestinationConfig, DestinationMetricsRegistry},
    metrics::BackendMetricsShard,
    tko::TkoTrackerMap,
};
use rusty_mcrouter_config::ConfigDocument;
use rusty_mcrouter_core::{
    RootRouteOptions, RoutingEventSink, RoutingMetricsLayout, RoutingMetricsShard,
};
use tokio::sync::mpsc;

use crate::{FrontendMetricsShard, ProxyCommand, ProxyRequest, ProxySet, WorkerEventSink};

pub struct ProxyThreadConfig {
    pub proxy_id: usize,
    pub inbox: ProxyInbox,
    pub shards: ProxyShards,
    pub shared: Arc<ProxyShared>,
    pub proxies: ProxySet,
    pub listener: Option<ListenerConfig>,
    pub routing_events: RoutingEventSink,
    /// Worker lifecycle events are emitted through a leaf-owned sink.
    pub events: WorkerEventSink,
}

pub struct ProxyInbox {
    pub work_rx: mpsc::Receiver<std::net::TcpStream>,
    pub request_rx: mpsc::Receiver<ProxyRequest>,
    pub command_rx: mpsc::Receiver<ProxyCommand>,
}

#[derive(Clone)]
pub struct ProxyShards {
    pub backend: Arc<BackendMetricsShard>,
    pub frontend: Arc<FrontendMetricsShard>,
    pub routing: Arc<RoutingMetricsShard>,
}

impl ProxyShards {
    pub fn new(layout: Arc<RoutingMetricsLayout>) -> Self {
        Self {
            backend: BackendMetricsShard::new(),
            frontend: FrontendMetricsShard::new(),
            routing: RoutingMetricsShard::new(layout),
        }
    }
}

pub struct ProxyShared {
    pub config: Arc<ConfigDocument>,
    /// Cross-thread health: same-server destinations on different threads
    /// share health verdicts through it (atomics only).
    pub tko_map: Arc<TkoTrackerMap>,
    /// Cross-thread counters: same-server destinations on different threads
    /// share one scrapeable counter block through it (atomics only).
    pub destinations: Arc<DestinationMetricsRegistry>,
    /// Router-level destination defaults (derived from RouterOptions once in
    /// main); pools override via server_timeout/connect_timeout.
    pub defaults: DestinationConfig,
    pub root_route_options: RootRouteOptions,
    /// Idle-connection sweep interval; zero disables.
    pub sweep_interval: Duration,
    pub thread_mode: ThreadMode,
}

#[derive(Clone, Copy)]
pub enum ThreadMode {
    SameThread,
    // todo - thread modes: constructed once dispatch policy is configurable
    #[allow(dead_code)]
    FixedRemote {
        proxy_id: usize,
    },
    #[allow(dead_code)]
    AffinitizedRemote,
}

pub struct ListenerConfig {
    pub listen_addr: SocketAddr,
    pub use_reuseport: bool,
}
