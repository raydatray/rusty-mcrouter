use std::{sync::Arc, time::Duration};

use rusty_mcrouter_backend::{
    destination::{DestinationConfig, DestinationMetricsRegistry},
    metrics::BackendMetricsShard,
    tko::{DestTokenAllocator, TkoTrackerMap},
};
use rusty_mcrouter_config::ConfigDocument;
use rusty_mcrouter_core::{RootRouteOptions, RoutingEventSink, RoutingMetricsShard};
use rusty_mcrouter_frontend::{FrontendConnectionOptions, FrontendMetricsShard, ListenerConfig};

use crate::{ThreadMode, WorkerEventSink, WorkerInbox, WorkerSet};

pub struct WorkerSetup {
    pub worker_id: usize,
    pub inbox: WorkerInbox,
    pub shards: WorkerShards,
    pub shared: Arc<WorkerShared>,
    pub workers: WorkerSet,
    pub listener: Option<ListenerConfig>,
    pub routing_events: RoutingEventSink,
    /// Worker lifecycle events are emitted through a leaf-owned sink.
    pub events: WorkerEventSink,
}

#[derive(Clone)]
pub struct WorkerShards {
    pub backend: Arc<BackendMetricsShard>,
    pub frontend: Arc<FrontendMetricsShard>,
    pub routing: Arc<RoutingMetricsShard>,
}

impl WorkerShards {
    pub fn new() -> Self {
        Self {
            backend: BackendMetricsShard::new(),
            frontend: FrontendMetricsShard::new(),
            routing: RoutingMetricsShard::new(),
        }
    }
}

impl Default for WorkerShards {
    fn default() -> Self {
        Self::new()
    }
}

pub struct WorkerShared {
    pub tokens: Arc<DestTokenAllocator>,
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
    pub connection_options: FrontendConnectionOptions,
}
