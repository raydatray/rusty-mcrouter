use std::{net::SocketAddr, rc::Rc, sync::mpsc::SyncSender};

use rusty_mcrouter_backend::{destination, DestinationFactory};
use rusty_mcrouter_core::{build_route, RoutingState};
use tokio::{runtime::Builder, task::LocalSet};

use crate::{
    config::{ListenerConfig, ProxyThreadConfig},
    runtime::ProxyRuntime,
    server::Server,
    WorkerEvent, WorkerEventRecord,
};

type ReadyEvent = anyhow::Result<Option<SocketAddr>>;

pub struct ProxyThread {
    handle: crate::ProxyHandle,
    join: Option<std::thread::JoinHandle<anyhow::Result<()>>>,
}

impl ProxyThread {
    pub fn spawn(
        handle: crate::ProxyHandle,
        config: ProxyThreadConfig,
    ) -> anyhow::Result<(Self, Option<SocketAddr>)> {
        let proxy_id = config.proxy_id;
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        let join = std::thread::Builder::new()
            .name(format!("proxy-{proxy_id}"))
            .spawn(move || {
                let result = proxy_thread_main(config, ready_tx);
                if let Err(error) = &result {
                    eprintln!("proxy-{proxy_id} terminated: {error}");
                    std::process::exit(1);
                }
                result
            })?;

        let bound_addr = match ready_rx.recv() {
            Ok(result) => result?,
            Err(_) => anyhow::bail!("proxy-{proxy_id} died during startup"),
        };

        Ok((
            Self {
                handle,
                join: Some(join),
            },
            bound_addr,
        ))
    }

    pub fn handle(&self) -> &crate::ProxyHandle {
        &self.handle
    }

    pub fn join(mut self) -> anyhow::Result<()> {
        self.join
            .take()
            .expect("proxy thread exists")
            .join()
            .map_err(|_| anyhow::anyhow!("proxy thread panicked"))?
    }

    pub fn shutdown(mut self) -> anyhow::Result<()> {
        self.handle.shutdown_blocking()?;
        self.join
            .take()
            .expect("proxy thread exists")
            .join()
            .map_err(|_| anyhow::anyhow!("proxy thread panicked"))?
    }
}

pub fn proxy_thread_main(
    cfg: ProxyThreadConfig,
    ready_tx: SyncSender<ReadyEvent>,
) -> anyhow::Result<()> {
    let rt = Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()?;

    // todo - fibers, this LocalSet is our FiberManager analogue
    let local = LocalSet::new();

    local.block_on(&rt, async move {
        let ProxyThreadConfig {
            proxy_id,
            config,
            work_rx,
            request_rx,
            command_rx,
            proxies,
            thread_mode,
            listener_config,
            tko_map,
            metrics_registry,
            backend_metrics,
            frontend_metrics,
            routing_metrics,
            routing_events,
            events,
            defaults,
            sweep_interval,
        } = cfg;

        // bind the listening socket if this thread owns one.
        // - `listener` couples the bound server with the socket queues it dispatches to
        // - worker-only threads carry neither
        let listener = match listener_config {
            Some(ListenerConfig {
                listen_addr,
                use_reuseport,
                listener_txs,
            }) => {
                let bind_result = if use_reuseport {
                    Server::bind_reuseport(listen_addr).await
                } else {
                    Server::bind(listen_addr).await
                };

                let server = match bind_result {
                    Ok(s) => s,
                    Err(e) => {
                        let _ =
                            ready_tx.send(Err(anyhow::anyhow!("bind({listen_addr}) failed: {e}")));
                        anyhow::bail!("bind({listen_addr}) failed: {e}");
                    }
                };

                Some((server, listener_txs))
            }
            None => None,
        };

        let bound_addr = listener
            .as_ref()
            .map(|(server, _)| server.local_addr())
            .transpose()?;

        // each thread builds its own route graph. `Rc<dyn DynRoute>` is
        // thread-local and never shared across threads. Backends are lazy:
        // building over dead servers succeeds, they just start life failing
        // (and TKO via the shared tracker map).
        let dest_map = destination::Map::new(tko_map, backend_metrics, metrics_registry);
        let sweep_task = dest_map.spawn_idle_sweep(sweep_interval);
        let factory = DestinationFactory::new(Rc::clone(&dest_map));
        let routing_state = RoutingState::with_event_sink(routing_metrics, routing_events);
        let route = match build_route(&config, &factory, &defaults, routing_state.layout()) {
            Ok(r) => r,
            Err(e) => {
                let _ = ready_tx.send(Err(anyhow::anyhow!("build_route failed: {e}")));
                anyhow::bail!("build_route failed: {e}");
            }
        };

        let _ = ready_tx.send(Ok(bound_addr));
        drop(ready_tx);
        events.emit(WorkerEventRecord {
            proxy_id,
            event: WorkerEvent::Started,
        });

        let listener_task = listener.map(|(server, listener_txs)| {
            tokio::task::spawn_local(async move {
                server
                    .accept_and_dispatch(listener_txs)
                    .await
                    .map_err(anyhow::Error::from)
            })
        });

        let runtime = ProxyRuntime::new(
            proxy_id,
            route,
            routing_state,
            proxies,
            thread_mode,
            frontend_metrics,
            request_rx,
            command_rx,
            work_rx,
            listener_task,
            sweep_task,
            dest_map,
        );
        let result = runtime.run().await;

        events.emit(WorkerEventRecord {
            proxy_id,
            event: WorkerEvent::Stopped,
        });
        result
    })
}
