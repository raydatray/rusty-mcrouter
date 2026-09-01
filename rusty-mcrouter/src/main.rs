mod control;
mod proxy;

use clap::Parser;
use rusty_mcrouter_backend::{
    destination::{self, DestinationMetricsRegistry},
    tko::TkoTrackerMap,
};
use rusty_mcrouter_config::{parse_file, RoutingPrefix};
use rusty_mcrouter_core::{RootRouteOptions, RoutingMetricsLayout};
use rusty_mcrouter_observability::{
    logging,
    sources::{
        BackendRequestsSource, BackendScalarsSource, DestinationSource, FrontendRequestsSource,
        FrontendScalarsSource, RoutingSource, SelfSource, TkoSource,
    },
    Observability,
};
use rusty_mcrouter_proxy::{
    ListenerConfig, ProxyHandle, ProxySet, ProxyShards, ProxyShared, ProxyThreadConfig, ThreadMode,
};

use crate::control::{ControlThread, ProcessEvent};
use crate::proxy::ProxyThread;

use std::{
    io::Write,
    net::{SocketAddr, ToSocketAddrs},
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

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
        default_value = "127.0.0.1:5001",
        env = "RUSTY_MCROUTER_METRICS_ADDR",
        help = "address for the prometheus /metrics endpoint"
    )]
    metrics_addr: String,

    #[command(flatten)]
    options: RouterOptions,
}

/// Router-level destination/TKO options. CLI flags with mcrouter's names and
/// defaults (mcrouter_options_list.h) — upstream treats these as command-line
/// options, not config-file keys, and so do we.
#[derive(clap::Args, Clone, Debug)]
struct RouterOptions {
    #[arg(
        short = 'R',
        long = "route-prefix",
        default_value = "/././",
        help = "default routing prefix"
    )]
    route_prefix: RoutingPrefix,

    #[arg(
        long,
        help = "send requests with unknown routing prefixes to the default route"
    )]
    send_invalid_route_to_default: bool,

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

fn destination_defaults(o: &RouterOptions) -> destination::DestinationConfig {
    destination::DestinationConfig {
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
    let metrics_addr = args.metrics_addr.to_socket_addrs()?.next().ok_or_else(|| {
        anyhow::anyhow!("could not resolve metrics address: {}", args.metrics_addr)
    })?;

    logging::init();
    let mut observability = Observability::new(1024);

    let config = Arc::new(parse_file(&args.config)?);
    let routing_layout = RoutingMetricsLayout::new(&config);

    let shared = Arc::new(ProxyShared {
        config,
        tko_map: TkoTrackerMap::new(observability.events().sink()),
        destinations: DestinationMetricsRegistry::new(),
        defaults: destination_defaults(&args.options),
        root_route_options: RootRouteOptions {
            default_route: args.options.route_prefix.clone(),
            send_invalid_to_default: args.options.send_invalid_route_to_default,
        },
        sweep_interval: Duration::from_millis(args.options.reset_inactive_connection_interval_ms),
        thread_mode: ThreadMode::SameThread,
    });

    // both ends of every proxy's channels: the handles go to everyone
    // (as the ProxySet), each inbox to its own thread
    let (handles, inboxes): (Vec<_>, Vec<_>) =
        (0..args.num_proxies).map(ProxyHandle::allocate).unzip();
    let proxies = ProxySet::new(handles.clone());

    let use_reuseport = num_listening_sockets > 1;
    let (process_event_tx, process_event_rx) = std::sync::mpsc::channel();
    let mut proxy_threads = Vec::with_capacity(args.num_proxies);
    let mut bound_addr: Option<SocketAddr> = None;
    // per-thread counter shards, created here so the scrape sources hold
    // the same Arcs the threads write
    let mut proxy_shards = Vec::with_capacity(args.num_proxies);

    for (proxy_id, (handle, inbox)) in handles.into_iter().zip(inboxes).enumerate() {
        let listener = (proxy_id < num_listening_sockets).then_some(ListenerConfig {
            listen_addr,
            use_reuseport,
        });
        let shards = ProxyShards::new(Arc::clone(&routing_layout));
        proxy_shards.push(shards.clone());

        let cfg = ProxyThreadConfig {
            proxy_id,
            inbox,
            shards,
            shared: Arc::clone(&shared),
            proxies: proxies.clone(),
            listener,
            routing_events: observability.events().sink(),
            events: observability.events().sink(),
        };

        let (thread, maybe_addr) = match ProxyThread::spawn(handle, cfg, process_event_tx.clone()) {
            Ok(spawned) => spawned,
            Err(error) => {
                shutdown_proxy_threads(&mut proxy_threads);
                return Err(error);
            }
        };
        if let Some(addr) = maybe_addr {
            bound_addr.get_or_insert(addr);
        }
        proxy_threads.push(thread);
    }

    // drop main's ProxySet: each thread keeps its own clone, so the
    // queues stay open until the threads terminate
    drop(proxies);

    let backend_shards: Vec<_> = proxy_shards
        .iter()
        .map(|s| Arc::clone(&s.backend))
        .collect();
    let frontend_shards: Vec<_> = proxy_shards
        .iter()
        .map(|s| Arc::clone(&s.frontend))
        .collect();
    let routing_shards: Vec<_> = proxy_shards
        .iter()
        .map(|s| Arc::clone(&s.routing))
        .collect();
    observability.register(Box::new(BackendScalarsSource {
        shards: backend_shards.clone(),
    }));
    observability.register(Box::new(BackendRequestsSource {
        shards: backend_shards,
    }));
    observability.register(Box::new(FrontendScalarsSource {
        shards: frontend_shards.clone(),
    }));
    observability.register(Box::new(FrontendRequestsSource {
        shards: frontend_shards,
    }));
    observability.register(Box::new(RoutingSource {
        shards: routing_shards,
    }));
    observability.register(Box::new(TkoSource {
        map: Arc::clone(&shared.tko_map),
    }));
    observability.register(Box::new(DestinationSource {
        registry: Arc::clone(&shared.destinations),
    }));
    observability.register(Box::new(SelfSource {
        metrics: observability.control_metrics(),
        num_proxies: args.num_proxies,
        start_unix_secs: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
    }));
    let (control_thread, metrics_bound) = match ControlThread::spawn(
        observability.into_parts(metrics_addr),
        process_event_tx.clone(),
    ) {
        Ok(spawned) => spawned,
        Err(error) => {
            shutdown_proxy_threads(&mut proxy_threads);
            return Err(error);
        }
    };
    drop(process_event_tx);

    let addr =
        bound_addr.ok_or_else(|| anyhow::anyhow!("no proxy thread reported a bound address"))?;
    println!("READY {addr}");
    println!("METRICS {metrics_bound}");
    std::io::stdout().flush()?;
    tracing::info!(
        listen = %addr,
        proxy_threads = args.num_proxies,
        listening_sockets = num_listening_sockets,
        config = %args.config.display(),
        "rusty-mcrouter ready"
    );

    let mut first_error = match process_event_rx.recv()? {
        ProcessEvent::ShutdownRequested => None,
        ProcessEvent::ProxyExited { id } => Some(anyhow::anyhow!("proxy-{id} exited unexpectedly")),
        ProcessEvent::ControlExited => Some(anyhow::anyhow!("control thread exited unexpectedly")),
    };

    if let Some(error) = shutdown_proxy_threads(&mut proxy_threads) {
        if first_error.is_none() {
            first_error = Some(error);
        }
    }
    if let Err(error) = control_thread.shutdown() {
        if first_error.is_none() {
            first_error = Some(error);
        }
    }
    if let Some(error) = first_error {
        return Err(error);
    }

    Ok(())
}

fn shutdown_proxy_threads(proxy_threads: &mut Vec<ProxyThread>) -> Option<anyhow::Error> {
    let mut first_error = None;
    for thread in proxy_threads.drain(..) {
        if let Err(error) = thread.shutdown() {
            if first_error.is_none() {
                first_error = Some(error);
            }
        }
    }
    first_error
}

#[cfg(test)]
mod args_tests {
    use super::*;

    fn parse_args(extra: &[&str]) -> Args {
        let mut args = vec!["rusty-mcrouter", "--config", "config.json"];
        args.extend_from_slice(extra);
        Args::try_parse_from(args).unwrap()
    }

    #[test]
    fn root_route_options_use_mcrouter_defaults() {
        let args = parse_args(&[]);

        assert_eq!(args.options.route_prefix.as_str(), "/././");
        assert!(!args.options.send_invalid_route_to_default);
    }

    #[test]
    fn root_route_options_accept_short_and_long_flags() {
        let args = parse_args(&["-R", "/a/a/", "--send-invalid-route-to-default"]);

        assert_eq!(args.options.route_prefix.as_str(), "/a/a/");
        assert!(args.options.send_invalid_route_to_default);
    }

    #[test]
    fn route_prefix_rejects_malformed_values() {
        assert!(Args::try_parse_from([
            "rusty-mcrouter",
            "--config",
            "config.json",
            "--route-prefix",
            "/invalid/",
        ])
        .is_err());
    }
}
