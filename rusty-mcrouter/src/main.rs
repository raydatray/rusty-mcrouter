mod control;
mod proxy;

use clap::Parser;
use rusty_mcrouter_backend::{
    destination::{self, DestinationMetricsRegistry},
    tko::TkoTrackerMap,
};
use rusty_mcrouter_config::{parse_file, RoutingPrefix};
use rusty_mcrouter_core::RootRouteOptions;
use rusty_mcrouter_observability::{channel, logging, ControlMetrics, ScrapeInputs};
use rusty_mcrouter_proxy::{ProxyShared, ThreadMode};

use crate::control::{ControlThread, ControlThreadConfig, ProcessEvent, Supervisor};
use crate::proxy::{ProxyFleet, ProxyFleetConfig};

use std::{io::Write, net::ToSocketAddrs, path::PathBuf, sync::Arc, time::Duration};

const EVENT_BUS_CAPACITY: usize = 1024;

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
    let control_metrics = Arc::new(ControlMetrics::default());
    let (events, event_consumer) = channel(EVENT_BUS_CAPACITY, Arc::clone(&control_metrics));

    let shared = Arc::new(ProxyShared {
        config: Arc::new(parse_file(&args.config)?),
        tko_map: TkoTrackerMap::new(events.sink()),
        destinations: DestinationMetricsRegistry::new(),
        defaults: destination_defaults(&args.options),
        root_route_options: RootRouteOptions {
            default_route: args.options.route_prefix.clone(),
            send_invalid_to_default: args.options.send_invalid_route_to_default,
        },
        sweep_interval: Duration::from_millis(args.options.reset_inactive_connection_interval_ms),
        thread_mode: ThreadMode::SameThread,
    });

    let supervisor = Supervisor::new();

    let proxies = ProxyFleet::spawn(
        ProxyFleetConfig {
            num_proxies: args.num_proxies,
            num_listening_sockets,
            listen_addr,
            shared: Arc::clone(&shared),
            events,
        },
        &supervisor,
    )?;

    let registry = ScrapeInputs {
        proxies: proxies.shards(),
        tko_map: Arc::clone(&shared.tko_map),
        destinations: Arc::clone(&shared.destinations),
        control: Arc::clone(&control_metrics),
    }
    .into_registry();

    let (control_thread, metrics_bound) = match ControlThread::spawn(
        ControlThreadConfig {
            events: event_consumer,
            registry: Arc::new(registry),
            metrics_addr,
            metrics: control_metrics,
        },
        &supervisor,
    ) {
        Ok(spawned) => spawned,
        Err(error) => {
            let _ = proxies.shutdown();
            return Err(error);
        }
    };

    println!("READY {}", proxies.bound_addr());
    println!("METRICS {metrics_bound}");
    std::io::stdout().flush()?;
    tracing::info!(
        listen = %proxies.bound_addr(),
        proxy_threads = args.num_proxies,
        listening_sockets = num_listening_sockets,
        config = %args.config.display(),
        "rusty-mcrouter ready"
    );

    let outcome = match supervisor.wait()? {
        ProcessEvent::ShutdownRequested => Ok(()),
        ProcessEvent::ProxyExited { id } => Err(anyhow::anyhow!("proxy-{id} exited unexpectedly")),
        ProcessEvent::ControlExited => Err(anyhow::anyhow!("control thread exited unexpectedly")),
    };

    // proxies first so their Stopped events reach the control runtime
    let stopped_proxies = proxies.shutdown();
    let stopped_control = control_thread.shutdown();
    outcome.and(stopped_proxies).and(stopped_control)
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
