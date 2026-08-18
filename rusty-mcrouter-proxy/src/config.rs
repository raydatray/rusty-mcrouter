use std::{net::SocketAddr, sync::Arc, time::Duration};

use rusty_mcrouter_backend::{counters::ProxyCounters, destination, tko::TkoTrackerMap};
use rusty_mcrouter_config::ConfigDocument;
use tokio::sync::mpsc;

use crate::{message::ProxyMessage, proxy_set::ProxySet, FrontendCounters, WorkerEventSink};

pub struct ProxyThreadConfig {
    pub proxy_id: usize,
    pub config: Arc<ConfigDocument>,
    pub work_rx: mpsc::Receiver<std::net::TcpStream>,
    pub proxy_rx: mpsc::Receiver<ProxyMessage>,
    pub proxies: ProxySet,
    pub thread_mode: ThreadMode,
    pub listener_config: Option<ListenerConfig>,
    /// Cross-thread health: same-server destinations on different threads
    /// share health verdicts through it (atomics only).
    pub tko_map: Arc<TkoTrackerMap>,
    /// Cross-thread counters: same-server destinations on different threads
    /// share one scrapeable counter block through it (atomics only).
    pub counters_registry: Arc<destination::DestinationCountersRegistry>,
    /// This thread's counter shards. Created in main so the scrape
    /// sources hold the same Arcs; this thread is the only writer.
    pub proxy_counters: Arc<ProxyCounters>,
    pub frontend_counters: Arc<FrontendCounters>,
    /// Worker lifecycle events are emitted through a leaf-owned sink.
    pub events: WorkerEventSink,
    /// Router-level destination defaults (derived from RouterOptions once in
    /// main); pools override via server_timeout/connect_timeout.
    pub defaults: destination::Config,
    /// Idle-connection sweep interval; zero disables.
    pub sweep_interval: Duration,
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
    pub listener_txs: Vec<mpsc::Sender<std::net::TcpStream>>,
}
