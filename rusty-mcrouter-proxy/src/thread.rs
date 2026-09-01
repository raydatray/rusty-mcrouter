use std::{net::SocketAddr, rc::Rc, sync::mpsc::SyncSender, sync::Arc};

use rusty_mcrouter_backend::{destination, DestinationFactory};
use rusty_mcrouter_core::{build_route_with_options, RoutingState};
use tokio::{runtime::Builder, task::LocalSet};

use crate::runtime::ProxyRuntime;
use crate::{
    ListenerConfig, ProxyInbox, ProxyShards, ProxyThreadConfig, Server, WorkerEvent,
    WorkerEventRecord,
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
            inbox,
            shards,
            shared,
            proxies,
            listener,
            routing_events,
            events,
        } = cfg;
        let ProxyInbox {
            work_rx,
            request_rx,
            command_rx,
        } = inbox;
        let ProxyShards {
            backend: backend_metrics,
            frontend: frontend_metrics,
            routing: routing_metrics,
        } = shards;

        // bind the listening socket if this thread owns one
        let server = match listener {
            Some(ListenerConfig {
                listen_addr,
                use_reuseport,
            }) => {
                let bind_result = if use_reuseport {
                    Server::bind_reuseport(listen_addr).await
                } else {
                    Server::bind(listen_addr).await
                };

                match bind_result {
                    Ok(server) => Some(server),
                    Err(e) => {
                        let _ =
                            ready_tx.send(Err(anyhow::anyhow!("bind({listen_addr}) failed: {e}")));
                        anyhow::bail!("bind({listen_addr}) failed: {e}");
                    }
                }
            }
            None => None,
        };

        let bound_addr = server.as_ref().map(Server::local_addr).transpose()?;

        // each thread builds its own route graph. `Rc<dyn DynRoute>` is
        // thread-local and never shared across threads. Backends are lazy:
        // building over dead servers succeeds, they just start life failing
        // (and TKO via the shared tracker map).
        let dest_map = destination::Map::new(
            Arc::clone(&shared.tko_map),
            backend_metrics,
            Arc::clone(&shared.destinations),
        );
        let sweep_task = dest_map.spawn_idle_sweep(shared.sweep_interval);
        let factory = DestinationFactory::new(Rc::clone(&dest_map));
        let routing_state = RoutingState::new(routing_metrics, routing_events);
        let route = match build_route_with_options(
            &shared.config,
            &factory,
            &shared.defaults,
            routing_state.layout(),
            &shared.root_route_options,
        ) {
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

        let listener_task = server.map(|server| {
            let proxies = proxies.clone();
            tokio::task::spawn_local(async move {
                server
                    .accept_and_dispatch(proxies)
                    .await
                    .map_err(anyhow::Error::from)
            })
        });

        let runtime = ProxyRuntime::new(
            proxy_id,
            route,
            routing_state,
            proxies,
            shared.thread_mode,
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
