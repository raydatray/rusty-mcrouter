use clap::Parser;
use rusty_mcrouter_backend::{
    destination::{self, DestinationMetricsRegistry},
    metrics::BackendMetricsShard,
    tko::TkoTrackerMap,
};
use rusty_mcrouter_config::parse_file;
use rusty_mcrouter_core::{RoutingMetricsLayout, RoutingMetricsShard};
use rusty_mcrouter_observability::{
    sources::{
        BackendRequestsSource, BackendScalarsSource, DestinationSource, FrontendRequestsSource,
        FrontendScalarsSource, RoutingSource, SelfSource, TkoSource,
    },
    Observability,
};
use rusty_mcrouter_proxy::{
    FrontendMetricsShard, ListenerConfig, ProxyCommand, ProxyHandle, ProxyRequest, ProxySet,
    ProxyThread, ProxyThreadConfig, ThreadMode,
};
use tokio::sync::mpsc;

use std::{
    io::Write,
    net::{SocketAddr, ToSocketAddrs},
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
const WORK_CHANNEL_CAPACITY: usize = 1024;
const PROXY_REQUEST_CAPACITY: usize = 1024;
const PROXY_COMMAND_CAPACITY: usize = 16;

#[derive(Parser)]
struct Args {
    #[arg(
        long,
        value_name = "PATH",
        help = "path to mcrouter-format JSON config file"
    )]
    config: PathBuf,

    #[arg(
        long,
        value_name = "ADDR",
        default_value = "127.0.0.1:5000",
        env = "RUSTY_MCROUTER_LISTEN",
        help = "address to listen on"
    )]
    listen: String,

    #[arg(
        long,
        value_name = "N",
        default_value = "4",
        env = "RUSTY_MCROUTER_NUM_PROXIES",
        help = "num of proxy threads"
    )]
    num_proxies: usize,

    #[arg(
        long,
        value_name = "M",
        env = "RUSTY_MCROUTER_NUM_LISTENING_SOCKETS",
        help = "number of SO_REUSEPORT listening sockets (defaults to num_proxies)"
    )]
    num_listening_sockets: Option<usize>,

    #[arg(
        long,
        value_name = "ADDR",
        env = "RUSTY_MCROUTER_METRICS_ADDR",
        help = "address for the prometheus /metrics endpoint; unset disables it"
    )]
    metrics_addr: Option<String>,

    #[command(flatten)]
    options: RouterOptions,
}

/// Router-level destination/TKO options. CLI flags with mcrouter's names and
/// defaults (mcrouter_options_list.h) — upstream treats these as command-line
/// options, not config-file keys, and so do we.
#[derive(clap::Args, Clone, Debug)]
struct RouterOptions {
    #[arg(
        long,
        default_value_t = 1000,
        help = "per-request reply timeout, ms; also the connect timeout default"
    )]
    server_timeout_ms: u64,

    #[arg(
        long,
        default_value_t = 0,
        help = "extra connect attempts after a connect TIMEOUT (other connect errors never retry)"
    )]
    connect_timeout_retries: usize,

    #[arg(
        long,
        default_value_t = 3,
        help = "consecutive soft failures (timeouts) before a server is marked TKO"
    )]
    failures_until_tko: u64,

    #[arg(
        long,
        default_value_t = 10_000,
        help = "first probe delay after a TKO mark, ms"
    )]
    probe_delay_initial_ms: u64,

    #[arg(long, default_value_t = 60_000, help = "probe backoff ceiling, ms")]
    probe_delay_max_ms: u64,

    #[arg(
        long,
        default_value_t = 60_000,
        help = "idle connections are closed within at most 2x this interval, ms; 0 disables"
    )]
    reset_inactive_connection_interval_ms: u64,

    #[arg(long, help = "disable TKO tracking entirely (no fast-fail, no probes)")]
    disable_tko_tracking: bool,
}

fn destination_defaults(o: &RouterOptions) -> destination::Config {
    destination::Config {
        // connect_timeout defaults to the server timeout, like mcrouter
        // (McRouteHandleProvider-inl.h:197-205); pools may override both
        connect_timeout: Some(Duration::from_millis(o.server_timeout_ms)),
        reply_timeout: Some(Duration::from_millis(o.server_timeout_ms)),
        connect_timeout_retries: o.connect_timeout_retries,
        failures_until_tko: o.failures_until_tko,
        probe_delay_initial: Duration::from_millis(o.probe_delay_initial_ms),
        probe_delay_max: Duration::from_millis(o.probe_delay_max_ms),
        disable_tko_tracking: o.disable_tko_tracking,
    }
}

fn main() -> anyhow::Result<()> {
    std::panic::set_hook(Box::new(|info| {
        eprintln!("FATAL: panic in proxy thread: {info}");
        std::process::exit(1);
    }));

    let args = Args::parse();
    let num_listening_sockets = args.num_listening_sockets.unwrap_or(1);
    if args.num_proxies == 0 {
        anyhow::bail!("num_proxies must be >= 1")
    }
    if num_listening_sockets == 0 {
        anyhow::bail!("num_listening_sockets must be >= 1");
    }
    if num_listening_sockets > args.num_proxies {
        anyhow::bail!(
            "num_listening_sockets ({}) must be <= num_proxies ({})",
            num_listening_sockets,
            args.num_proxies
        );
    }

    let listen_addr = args
        .listen
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| anyhow::anyhow!("could not resolve listen address: {}", args.listen))?;
    let metrics_addr = args
        .metrics_addr
        .as_deref()
        .map(|addr| {
            addr.to_socket_addrs()?
                .next()
                .ok_or_else(|| anyhow::anyhow!("could not resolve metrics address: {addr}"))
        })
        .transpose()?;

    // first: installs the tracing subscriber (logs -> stderr; stdout is
    // the READY/METRICS control channel)
    let mut observability = Observability::new(1024);

    let config = Arc::new(parse_file(&args.config)?);
    let routing_layout = RoutingMetricsLayout::new(config.pools.keys().cloned());

    // the cross-thread objects: per-server health and per-server counters,
    // shared by every proxy thread's destinations, atomics only
    let tko_map = TkoTrackerMap::with_sink(observability.events().sink());
    let metrics_registry = DestinationMetricsRegistry::new();
    let defaults = destination_defaults(&args.options);
    let sweep_interval = Duration::from_millis(args.options.reset_inactive_connection_interval_ms);

    let (work_txs, work_rxs): (Vec<_>, Vec<_>) = (0..args.num_proxies)
        .map(|_| mpsc::channel::<std::net::TcpStream>(WORK_CHANNEL_CAPACITY))
        .unzip();

    let (request_txs, request_rxs): (Vec<_>, Vec<_>) = (0..args.num_proxies)
        .map(|_| mpsc::channel::<ProxyRequest>(PROXY_REQUEST_CAPACITY))
        .unzip();
    let (command_txs, command_rxs): (Vec<_>, Vec<_>) = (0..args.num_proxies)
        .map(|_| mpsc::channel::<ProxyCommand>(PROXY_COMMAND_CAPACITY))
        .unzip();
    let proxy_handles: Vec<_> = request_txs
        .iter()
        .zip(&command_txs)
        .enumerate()
        .map(|(id, (request_tx, command_tx))| {
            ProxyHandle::new(id, request_tx.clone(), command_tx.clone())
        })
        .collect();
    let proxies = ProxySet::new(proxy_handles.clone());

    let use_reuseport = num_listening_sockets > 1;
    let mut proxy_threads = Vec::with_capacity(args.num_proxies);
    let mut bound_addr: Option<SocketAddr> = None;
    let mut work_rxs_iter = work_rxs.into_iter();
    let mut request_rxs_iter = request_rxs.into_iter();
    let mut command_rxs_iter = command_rxs.into_iter();
    // per-thread counter shards, created here so the scrape sources hold
    // the same Arcs the threads write
    let mut backend_metric_shards = Vec::with_capacity(args.num_proxies);
    let mut frontend_metric_shards = Vec::with_capacity(args.num_proxies);
    let mut routing_metric_shards = Vec::with_capacity(args.num_proxies);

    for (proxy_id, proxy_handle) in proxy_handles.into_iter().enumerate() {
        let has_listener = proxy_id < num_listening_sockets;
        let work_rx = work_rxs_iter.next().expect("one work_rx per proxy thread");
        let request_rx = request_rxs_iter
            .next()
            .expect("one request_rx per proxy thread");
        let command_rx = command_rxs_iter
            .next()
            .expect("one command_rx per proxy thread");
        let listener_config = if has_listener {
            Some(ListenerConfig {
                listen_addr,
                use_reuseport,
                listener_txs: work_txs.clone(),
            })
        } else {
            None
        };

        let backend_metrics = BackendMetricsShard::new();
        let frontend_metrics = FrontendMetricsShard::new();
        let routing_metrics = RoutingMetricsShard::new(Arc::clone(&routing_layout));
        backend_metric_shards.push(Arc::clone(&backend_metrics));
        frontend_metric_shards.push(Arc::clone(&frontend_metrics));
        routing_metric_shards.push(Arc::clone(&routing_metrics));

        let cfg = ProxyThreadConfig {
            proxy_id,
            config: Arc::clone(&config),
            work_rx,
            request_rx,
            command_rx,
            proxies: proxies.clone(),
            thread_mode: ThreadMode::SameThread,
            listener_config,
            tko_map: Arc::clone(&tko_map),
            metrics_registry: Arc::clone(&metrics_registry),
            backend_metrics,
            frontend_metrics,
            routing_metrics,
            routing_events: observability.events().sink(),
            events: observability.events().sink(),
            defaults: defaults.clone(),
            sweep_interval,
        };

        let (thread, maybe_addr) = ProxyThread::spawn(proxy_handle, cfg)?;
        if let Some(addr) = maybe_addr {
            bound_addr.get_or_insert(addr);
        }
        proxy_threads.push(thread);
    }

    // drop main's sender copies
    // - each thread keeps its own clones (work_txs inside listener_config, proxy senders inside the ProxySet)
    // - the queues stay open until the threads terminate
    drop(work_txs);
    drop(request_txs);
    drop(command_txs);
    drop(proxies);

    observability.register(Box::new(BackendScalarsSource {
        shards: backend_metric_shards.clone(),
    }));
    observability.register(Box::new(BackendRequestsSource {
        shards: backend_metric_shards,
    }));
    observability.register(Box::new(FrontendScalarsSource {
        shards: frontend_metric_shards.clone(),
    }));
    observability.register(Box::new(FrontendRequestsSource {
        shards: frontend_metric_shards,
    }));
    observability.register(Box::new(RoutingSource {
        shards: routing_metric_shards,
    }));
    observability.register(Box::new(TkoSource {
        map: Arc::clone(&tko_map),
    }));
    observability.register(Box::new(DestinationSource {
        registry: Arc::clone(&metrics_registry),
    }));
    observability.register(Box::new(SelfSource {
        dropped: observability.events().dropped_counter(),
        num_proxies: args.num_proxies,
        start_unix_secs: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
    }));
    let metrics_bound = observability.spawn(metrics_addr)?;

    let addr =
        bound_addr.ok_or_else(|| anyhow::anyhow!("no proxy thread reported a bound address"))?;
    println!("READY {addr}");
    if let Some(metrics) = metrics_bound {
        println!("METRICS {metrics}");
    }
    std::io::stdout().flush().ok();
    eprintln!(
        "rusty-mcrouter listening on {} with {} proxy threads ({} listening sockets) -> {}",
        addr,
        args.num_proxies,
        num_listening_sockets,
        args.config.display()
    );

    for thread in proxy_threads {
        thread.join()?;
    }

    Ok(())
}
