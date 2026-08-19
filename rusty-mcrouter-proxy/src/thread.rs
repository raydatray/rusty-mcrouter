use std::{net::SocketAddr, rc::Rc, sync::mpsc::SyncSender};

use rusty_mcrouter_backend::{destination, DestinationFactory};
use rusty_mcrouter_core::{build_route, RoutingState};
use tokio::{runtime::Builder, task::LocalSet};

use crate::{
    config::{ListenerConfig, ProxyThreadConfig},
    proxy::Proxy,
    server::Server,
    worker::ConnectionWorker,
    WorkerEvent, WorkerEventRecord,
};

type ReadyEvent = anyhow::Result<Option<SocketAddr>>;

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
            proxy_rx,
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
        let _sweep = dest_map.spawn_idle_sweep(sweep_interval);
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

        // proxy actor:
        // -  drains this thread's message queue (requests routed here by
        // peer threads)
        // - same-thread requests bypass it inside the connection task
        let proxy = Proxy {
            id: proxy_id,
            route: Rc::clone(&route),
            routing_state: Rc::clone(&routing_state),
            rx: proxy_rx,
        };
        tokio::task::spawn_local(proxy.run());

        // connection worker
        // - drains handed-off sockets and serves each connection.
        let worker = ConnectionWorker::new(
            proxy_id,
            route,
            proxies,
            thread_mode,
            frontend_metrics,
            routing_state,
            work_rx,
        );

        let result = match listener {
            Some((server, listener_txs)) => {
                tokio::select! {
                    result = server.accept_and_dispatch(listener_txs) => result.map_err(Into::into),
                    _ = worker.run() => {
                        Err(anyhow::anyhow!("worker channel closed unexpectedly"))
                    }
                }
            }
            None => {
                worker.run().await;
                Ok(())
            }
        };

        events.emit(WorkerEventRecord {
            proxy_id,
            event: WorkerEvent::Stopped,
        });
        result
    })
}
