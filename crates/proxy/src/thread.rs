use std::{net::SocketAddr, sync::mpsc::SyncSender, sync::Arc};

use rusty_mcrouter_backend::destination;
use tokio::{runtime::Builder, task::LocalSet};

use crate::context::ProxyContext;
use crate::generation::{GenerationBuilder, RouteSlot};
use crate::runtime::{BackgroundTasks, ProxyRuntime};
use crate::{
    ListenerConfig, ProxyShards, ProxyThreadConfig, Server, WorkerEvent, WorkerEventRecord,
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
        let dest_map = destination::Map::new(destination::DestinationMapSetup {
            tko_map: Arc::clone(&shared.tko_map),
            assembler: destination::DestinationAssembler::new(
                destination::DestinationAssemblerSetup {
                    tokens: Arc::clone(&shared.tokens),
                    metrics: Arc::clone(&shared.destinations),
                    shard_metrics: backend_metrics,
                },
            ),
        });
        let sweep_task = dest_map.spawn_idle_sweep(shared.sweep_interval);
        let builder = GenerationBuilder::new(
            dest_map,
            shared.defaults.clone(),
            shared.root_route_options.clone(),
            routing_metrics,
            routing_events,
        );
        let routes = match builder.build(1, &shared.config) {
            Ok(initial) => RouteSlot::new(initial),
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

        let context = ProxyContext {
            proxy_id,
            routes,
            proxies,
            thread_mode: shared.thread_mode,
            metrics: frontend_metrics,
        };
        let tasks = BackgroundTasks {
            listener: listener_task,
            sweep: sweep_task,
        };
        let runtime = ProxyRuntime::new(context, builder, inbox, tasks);
        let result = runtime.run().await;

        events.emit(WorkerEventRecord {
            proxy_id,
            event: WorkerEvent::Stopped,
        });
        result
    })
}
