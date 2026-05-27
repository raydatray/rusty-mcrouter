use std::{
    net::SocketAddr,
    rc::Rc,
    sync::{mpsc::SyncSender, Arc},
};

use bytes::Bytes;
use rusty_mcrouter_config::ConfigDocument;
use rusty_mcrouter_core::build_route;
use rusty_mcrouter_net::{serve_worker, Server};
use rusty_mcrouter_protocol::Reply;
use tokio::{runtime::Builder, sync::mpsc, task::LocalSet};

enum ProxyThreadRole {
    ListenerAndWorker {
        server: Server,
        work_txs: Vec<mpsc::Sender<std::net::TcpStream>>,
    },
    WorkerOnly,
}

type ReadyEvent = anyhow::Result<Option<SocketAddr>>;

pub fn proxy_thread_main(
    listen_addr: SocketAddr,
    use_reuseport: bool,
    config: Arc<ConfigDocument>,
    listener_txs: Option<Vec<mpsc::Sender<std::net::TcpStream>>>,
    work_rx: mpsc::Receiver<std::net::TcpStream>,
    ready_tx: SyncSender<ReadyEvent>,
) -> anyhow::Result<()> {
    let rt = Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()?;

    // todo - fibers, this LocalSet is our FiberManager analogue; route work should be scheduled through a proxy queue, not direct session calls
    let local = LocalSet::new();

    local.block_on(&rt, async move {
        let role = match listener_txs {
            Some(work_txs) => {
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

                ProxyThreadRole::ListenerAndWorker { server, work_txs }
            }
            None => ProxyThreadRole::WorkerOnly,
        };

        let bound_addr = match &role {
            ProxyThreadRole::ListenerAndWorker { server, .. } => Some(server.local_addr()?),
            ProxyThreadRole::WorkerOnly => None,
        };

        let route = match build_route(&config).await {
            Ok(r) => r,
            Err(e) => {
                let _ = ready_tx.send(Err(anyhow::anyhow!("build_route failed: {e}")));
                anyhow::bail!("build_route failed: {e}");
            }
        };

        let _ = ready_tx.send(Ok(bound_addr));
        drop(ready_tx);

        // todo - proxy queue, replace this direct route closure with ProxyMessage::Request handling on this proxy thread
        let handler = move |req| {
            let route = Rc::clone(&route);
            async move {
                route.route_dyn(req).await.unwrap_or_else(|_| {
                    Reply::ServerError(Bytes::from_static(b"backend unavailable"))
                })
            }
        };

        match role {
            ProxyThreadRole::ListenerAndWorker { server, work_txs } => {
                // todo - thread modes, serve_worker should choose SameThread/FixedRemote/AffinitizedRemote before enqueueing requests
                tokio::select! {
                    result = server.accept_and_dispatch(work_txs) => result?,
                    _ = serve_worker(work_rx, handler) => {
                        anyhow::bail!("worker channel closed unexpectedly");
                    }
                }
            }
            ProxyThreadRole::WorkerOnly => {
                serve_worker(work_rx, handler).await;
            }
        }

        Ok(())
    })
}
