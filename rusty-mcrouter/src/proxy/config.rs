use std::{net::SocketAddr, sync::Arc, time::Duration};

use rusty_mcrouter_config::ConfigDocument;
use rusty_mcrouter_net::{destination, tko::TkoTrackerMap};
use tokio::sync::mpsc;

use crate::proxy::{message::ProxyMessage, proxy_set::ProxySet};

pub struct ProxyThreadConfig {
    pub proxy_id: usize,
    pub config: Arc<ConfigDocument>,
    pub work_rx: mpsc::Receiver<std::net::TcpStream>,
    pub proxy_rx: mpsc::Receiver<ProxyMessage>,
    pub proxies: ProxySet,
    pub thread_mode: ThreadMode,
    pub listener_config: Option<ListenerConfig>,
    /// The ONE cross-thread object: same-server destinations on different
    /// threads share health verdicts through it (atomics only).
    pub tko_map: Arc<TkoTrackerMap>,
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
