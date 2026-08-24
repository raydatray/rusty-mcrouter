use std::{rc::Rc, sync::Arc};

use anyhow::Context;
use rusty_mcrouter_backend::destination;
use rusty_mcrouter_core::{DynRoute, RoutingState};
use tokio::sync::mpsc;
use tokio::task::{JoinHandle, JoinSet};

use crate::connection::Connection;
use crate::routing::route_request;
use crate::{FrontendMetricsShard, ProxyCommand, ProxyRequest, ProxySet, ThreadMode};

pub(crate) struct ProxyRuntime {
    proxy_id: usize,
    route: Rc<dyn DynRoute>,
    routing_state: Rc<RoutingState>,
    proxies: ProxySet,
    thread_mode: ThreadMode,
    frontend_metrics: Arc<FrontendMetricsShard>,
    request_rx: mpsc::Receiver<ProxyRequest>,
    command_rx: mpsc::Receiver<ProxyCommand>,
    work_rx: mpsc::Receiver<std::net::TcpStream>,
    listener_task: Option<JoinHandle<anyhow::Result<()>>>,
    sweep_task: Option<JoinHandle<()>>,
    route_tasks: JoinSet<()>,
    connection_tasks: JoinSet<()>,
    _destination_map: Rc<destination::Map>,
}

impl ProxyRuntime {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        proxy_id: usize,
        route: Rc<dyn DynRoute>,
        routing_state: Rc<RoutingState>,
        proxies: ProxySet,
        thread_mode: ThreadMode,
        frontend_metrics: Arc<FrontendMetricsShard>,
        request_rx: mpsc::Receiver<ProxyRequest>,
        command_rx: mpsc::Receiver<ProxyCommand>,
        work_rx: mpsc::Receiver<std::net::TcpStream>,
        listener_task: Option<JoinHandle<anyhow::Result<()>>>,
        sweep_task: Option<JoinHandle<()>>,
        destination_map: Rc<destination::Map>,
    ) -> Self {
        Self {
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
            route_tasks: JoinSet::new(),
            connection_tasks: JoinSet::new(),
            _destination_map: destination_map,
        }
    }

    pub(crate) async fn run(mut self) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                biased;

                command = self.command_rx.recv() => {
                    match command {
                        Some(ProxyCommand::Shutdown { acknowledged }) => {
                            self.shutdown().await;
                            let _ = acknowledged.send(());
                            return Ok(());
                        }
                        None => anyhow::bail!("proxy command channel closed"),
                    }
                }

                request = self.request_rx.recv() => {
                    let request = request.context("proxy request channel closed")?;
                    self.spawn_request(request);
                }

                stream = self.work_rx.recv() => {
                    let stream = stream.context("proxy work channel closed")?;
                    self.spawn_connection(stream)?;
                }

                Some(result) = self.route_tasks.join_next(), if !self.route_tasks.is_empty() => {
                    result.context("routed request task panicked")?;
                }

                Some(result) = self.connection_tasks.join_next(), if !self.connection_tasks.is_empty() => {
                    result.context("connection task panicked")?;
                }

                result = wait_for_listener(&mut self.listener_task), if self.listener_task.is_some() => {
                    return result;
                }

                result = wait_for_sweep(&mut self.sweep_task), if self.sweep_task.is_some() => {
                    return result;
                }
            }
        }
    }

    fn spawn_request(&mut self, request: ProxyRequest) {
        let route = Rc::clone(&self.route);
        let state = Rc::clone(&self.routing_state);
        self.route_tasks.spawn_local(async move {
            let reply = route_request(route, state, request.request).await;
            let _ = request.reply_tx.send(reply);
        });
    }

    fn spawn_connection(&mut self, stream: std::net::TcpStream) -> anyhow::Result<()> {
        let stream = tokio::net::TcpStream::from_std(stream)
            .context("could not register accepted stream on proxy runtime")?;
        let connection = Connection::new(
            stream,
            self.proxy_id,
            Rc::clone(&self.route),
            Rc::clone(&self.routing_state),
            self.proxies.clone(),
            self.thread_mode,
            Arc::clone(&self.frontend_metrics),
        );
        let metrics = Arc::clone(&self.frontend_metrics);
        metrics.client_connections.inc();
        self.connection_tasks.spawn_local(async move {
            if let Err(error) = connection.run().await {
                tracing::warn!(%error, "connection failed");
            }
            metrics.client_connections.dec();
        });
        Ok(())
    }

    async fn shutdown(&mut self) {
        self.request_rx.close();
        self.work_rx.close();
        if let Some(task) = self.listener_task.take() {
            task.abort();
        }
        if let Some(task) = self.sweep_task.take() {
            task.abort();
        }
        self.route_tasks.shutdown().await;
        self.connection_tasks.shutdown().await;
    }
}

async fn wait_for_listener(
    task: &mut Option<JoinHandle<anyhow::Result<()>>>,
) -> anyhow::Result<()> {
    let result = task.as_mut().expect("guarded by is_some").await;
    result.context("listener task panicked")?
}

async fn wait_for_sweep(task: &mut Option<JoinHandle<()>>) -> anyhow::Result<()> {
    task.as_mut()
        .expect("guarded by is_some")
        .await
        .context("destination sweep task panicked")?;
    anyhow::bail!("destination sweep task exited unexpectedly")
}

#[cfg(test)]
mod tests {
    use rusty_mcrouter_backend::destination::DestinationMetricsRegistry;
    use rusty_mcrouter_backend::metrics::BackendMetricsShard;
    use rusty_mcrouter_backend::tko::TkoTrackerMap;
    use rusty_mcrouter_core::{NullRoute, Route, RoutingMetricsLayout, RoutingMetricsShard};
    use rusty_mcrouter_observability_primitives::test_support::noop_sink;
    use rusty_mcrouter_protocol::test_support::{get, get_miss};

    use super::*;
    use crate::{ProxyHandle, ProxyRequest};

    fn test_runtime() -> (ProxyRuntime, ProxyHandle, mpsc::Sender<std::net::TcpStream>) {
        let (request_tx, request_rx) = mpsc::channel::<ProxyRequest>(8);
        let (command_tx, command_rx) = mpsc::channel::<ProxyCommand>(8);
        let (work_tx, work_rx) = mpsc::channel(8);
        let handle = ProxyHandle::new(0, request_tx, command_tx);
        let proxies = ProxySet::new(vec![handle.clone()]);
        let tko = TkoTrackerMap::with_sink(noop_sink());
        let map = destination::Map::new(
            tko,
            BackendMetricsShard::new(),
            DestinationMetricsRegistry::new(),
        );
        let layout = RoutingMetricsLayout::new(Vec::<String>::new());
        let state = RoutingState::new(RoutingMetricsShard::new(layout), noop_sink());
        let runtime = ProxyRuntime::new(
            0,
            NullRoute.into_dyn(),
            state,
            proxies,
            ThreadMode::SameThread,
            FrontendMetricsShard::new(),
            request_rx,
            command_rx,
            work_rx,
            None,
            None,
            map,
        );
        (runtime, handle, work_tx)
    }

    #[tokio::test]
    async fn routes_requests_and_acknowledges_shutdown() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let (runtime, handle, _work_tx) = test_runtime();
                let task = tokio::task::spawn_local(runtime.run());

                assert_eq!(handle.send_request(get(b"key")).await, get_miss());
                handle.shutdown().await.unwrap();
                task.await.unwrap().unwrap();
            })
            .await;
    }
}
