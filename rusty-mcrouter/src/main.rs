mod args;
mod control;
mod proxy;

use rusty_mcrouter_backend::{destination::DestinationMetricsRegistry, tko::TkoTrackerMap};
use rusty_mcrouter_config::parse_file;
use rusty_mcrouter_observability::{channel, logging, ControlMetrics, ScrapeInputs};
use rusty_mcrouter_proxy::{ProxyShared, ThreadMode};

use crate::args::Args;
use crate::control::{ControlThread, ControlThreadConfig, ProcessEvent, Supervisor};
use crate::proxy::{ProxyFleet, ProxyFleetConfig};

use std::{io::Write, sync::Arc};

const EVENT_BUS_CAPACITY: usize = 1024;

fn main() -> anyhow::Result<()> {
    let args = Args::from_cli()?;
    let listen_addr = args.listen_addr()?;
    let metrics_addr = args.metrics_addr()?;

    logging::init();
    let control_metrics = Arc::new(ControlMetrics::default());
    let (events, event_consumer) = channel(EVENT_BUS_CAPACITY, Arc::clone(&control_metrics));

    let shared = Arc::new(ProxyShared {
        config: Arc::new(parse_file(&args.config)?),
        tko_map: TkoTrackerMap::new(events.sink()),
        destinations: DestinationMetricsRegistry::new(),
        defaults: args.destination_defaults(),
        root_route_options: args.root_route_options(),
        sweep_interval: args.sweep_interval(),
        thread_mode: ThreadMode::SameThread,
    });

    let supervisor = Supervisor::new();

    let proxies = ProxyFleet::spawn(
        ProxyFleetConfig {
            num_proxies: args.num_proxies,
            num_listening_sockets: args.num_listening_sockets,
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
        listening_sockets = args.num_listening_sockets,
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
