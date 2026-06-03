use std::{net::SocketAddr, rc::Rc, sync::mpsc::SyncSender};

use rusty_mcrouter_core::build_route;
use rusty_mcrouter_net::Server;
use tokio::{runtime::Builder, task::LocalSet};

use crate::proxy::{ConnectionWorker, ListenerConfig, Proxy, ProxyThreadConfig};

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
        // thread-local and never shared across threads.
        let route = match build_route(&config).await {
            Ok(r) => r,
            Err(e) => {
                let _ = ready_tx.send(Err(anyhow::anyhow!("build_route failed: {e}")));
                anyhow::bail!("build_route failed: {e}");
            }
        };

        let _ = ready_tx.send(Ok(bound_addr));
        drop(ready_tx);

        // proxy actor:
        // -  drains this thread's message queue (requests routed here by
        // peer threads)
        // - same-thread requests bypass it inside the connection task
        let proxy = Proxy {
            id: proxy_id,
            route: Rc::clone(&route),
            rx: proxy_rx,
        };
        tokio::task::spawn_local(proxy.run());

        // connection worker
        // - drains handed-off sockets and serves each connection.
        let worker = ConnectionWorker::new(proxy_id, route, proxies, thread_mode, work_rx);

        match listener {
            Some((server, listener_txs)) => {
                tokio::select! {
                    result = server.accept_and_dispatch(listener_txs) => result?,
                    _ = worker.run() => {
                        anyhow::bail!("worker channel closed unexpectedly");
                    }
                }
            }
            None => {
                worker.run().await;
            }
        }

        Ok(())
    })
}
