use rusty_mcrouter_backend::{
    destination::DestinationMetricsRegistry,
    tko::{DestTokenAllocator, TkoTrackerMap},
};
use rusty_mcrouter_observability::{channel, logging, ConfigMetrics, ControlMetrics, ScrapeInputs};
use rusty_mcrouter_proxy::{ProxyHandle, ProxyShards, ProxyShared, ThreadMode};

use crate::args::Args;
use crate::config;
use crate::control::{ControlThread, ControlThreadConfig, ProcessEvent, Supervisor};
use crate::proxy_fleet::{ProxyFleet, ProxyFleetConfig, ProxyWorkerInputs};
use crate::reload::{ConfigReloader, ReloaderConfig};

use std::{io::Write, sync::Arc};

const EVENT_BUS_CAPACITY: usize = 1024;

pub(crate) fn run() -> anyhow::Result<()> {
    let args = Args::from_cli()?;
    logging::init();

    let listen_addr = args.listen_addr()?;
    let metrics_addr = args.metrics_addr()?;

    let control_metrics = Arc::new(ControlMetrics::default());
    let config_metrics = Arc::new(ConfigMetrics::default());
    let (events, event_consumer) = channel(EVENT_BUS_CAPACITY, Arc::clone(&control_metrics));
    let tko_map = TkoTrackerMap::new(events.sink());
    let tokens = Arc::new(DestTokenAllocator::new());
    let destinations = DestinationMetricsRegistry::new();
    let (workers, proxy_handles, proxy_shards): (Vec<_>, Vec<_>, Vec<_>) = (0..args.num_proxies)
        .map(|id| {
            let (handle, inbox) = ProxyHandle::allocate(id);
            let shards = ProxyShards::new();
            let worker = ProxyWorkerInputs {
                handle: handle.clone(),
                inbox,
                shards: shards.clone(),
            };
            (worker, handle, shards)
        })
        .collect();

    let (config_bytes, config) = config::load(&args.config)?;
    let config = Arc::new(config);
    let shared = Arc::new(ProxyShared {
        tokens,
        config: Arc::clone(&config),
        tko_map,
        destinations,
        defaults: args.destination_defaults(),
        root_route_options: args.root_route_options(),
        sweep_interval: args.sweep_interval(),
        thread_mode: ThreadMode::SameThread,
        connection_options: Default::default(),
    });

    let supervisor = Supervisor::new();

    let registry = ScrapeInputs {
        proxies: proxy_shards,
        tko_map: Arc::clone(&shared.tko_map),
        destinations: Arc::clone(&shared.destinations),
        control: Arc::clone(&control_metrics),
        config: Arc::clone(&config_metrics),
    }
    .into_registry();

    let reloader = (!args.disable_reload_configs).then(|| {
        ConfigReloader::new(ReloaderConfig {
            path: args.config.clone(),
            delay: args.reconfiguration_delay(),
            running: (config_bytes, config),
            proxies: proxy_handles,
            defaults: args.destination_defaults(),
            root_options: args.root_route_options(),
            metrics: Arc::clone(&config_metrics),
        })
    });

    let (control_thread, metrics_bound) = ControlThread::spawn(
        ControlThreadConfig {
            events: event_consumer,
            registry: Arc::new(registry),
            metrics_addr,
            metrics: control_metrics,
            reloader,
        },
        &supervisor,
    )?;

    let proxies = match ProxyFleet::spawn(
        ProxyFleetConfig {
            workers,
            num_listening_sockets: args.num_listening_sockets,
            listen_addr,
            shared: Arc::clone(&shared),
            events,
        },
        &supervisor,
    ) {
        Ok(proxies) => proxies,
        Err(error) => {
            let _ = control_thread.shutdown();
            return Err(error);
        }
    };

    config_metrics.applied(1);
    if let Err(error) = control_thread.proxies_ready() {
        let _ = proxies.shutdown();
        let _ = control_thread.shutdown();
        return Err(error);
    }

    println!("READY {}", proxies.bound_addr());
    println!("METRICS {metrics_bound}");
    if let Err(error) = std::io::stdout().flush() {
        let _ = proxies.shutdown();
        let _ = control_thread.shutdown();
        return Err(error.into());
    }
    tracing::info!(
        listen = %proxies.bound_addr(),
        proxy_threads = args.num_proxies,
        listening_sockets = args.num_listening_sockets,
        config = %args.config.display(),
        "rusty-mcrouter ready"
    );

    let outcome = match supervisor.wait() {
        Ok(ProcessEvent::ShutdownRequested) => Ok(()),
        Ok(ProcessEvent::ProxyExited { id }) => {
            Err(anyhow::anyhow!("proxy-{id} exited unexpectedly"))
        }
        Ok(ProcessEvent::ControlExited) => {
            Err(anyhow::anyhow!("control thread exited unexpectedly"))
        }
        Err(error) => Err(error),
    };

    // proxies first so their Stopped events reach the control runtime
    let stopped_proxies = proxies.shutdown();
    let stopped_control = control_thread.shutdown();
    outcome.and(stopped_proxies).and(stopped_control)
}
