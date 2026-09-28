use std::{net::TcpStream, rc::Rc, sync::Arc};

use anyhow::Context;
use tokio::sync::mpsc::Receiver;
use tokio::task::{JoinHandle, JoinSet};

use crate::connection::Connection;
use crate::generation::{GenerationBuilder, RouteGeneration};
use crate::routing::route_request;
use crate::{FrontendMetricsShard, ProxyCommand, ProxyRequest, ProxySet, ThreadMode};

pub(crate) struct ProxyRuntime {
    proxy_id: usize,
    generation: Rc<RouteGeneration>,
    _builder: GenerationBuilder,
    proxies: ProxySet,
    thread_mode: ThreadMode,
    frontend_metrics: Arc<FrontendMetricsShard>,
    request_rx: Receiver<ProxyRequest>,
    command_rx: Receiver<ProxyCommand>,
    work_rx: Receiver<TcpStream>,
    listener_task: Option<JoinHandle<anyhow::Result<()>>>,
    sweep_task: Option<JoinHandle<()>>,
    route_tasks: JoinSet<()>,
    connection_tasks: JoinSet<()>,
}

impl ProxyRuntime {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        proxy_id: usize,
        generation: Rc<RouteGeneration>,
        builder: GenerationBuilder,
        proxies: ProxySet,
        thread_mode: ThreadMode,
        frontend_metrics: Arc<FrontendMetricsShard>,
        request_rx: Receiver<ProxyRequest>,
        command_rx: Receiver<ProxyCommand>,
        work_rx: Receiver<TcpStream>,
        listener_task: Option<JoinHandle<anyhow::Result<()>>>,
        sweep_task: Option<JoinHandle<()>>,
    ) -> Self {
        Self {
            proxy_id,
            generation,
            _builder: builder,
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
        let route = Rc::clone(&self.generation.route);
        let state = Rc::clone(&self.generation.state);
        self.route_tasks.spawn_local(async move {
            let reply = route_request(route, state, request.request).await;
            let _ = request.reply_tx.send(reply);
        });
    }

    fn spawn_connection(&mut self, stream: TcpStream) -> anyhow::Result<()> {
        let stream = tokio::net::TcpStream::from_std(stream)
            .context("could not register accepted stream on proxy runtime")?;
        let connection = Connection::new(
            stream,
            self.proxy_id,
            Rc::clone(&self.generation.route),
            Rc::clone(&self.generation.state),
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
    use std::time::Duration;

    use rusty_mcrouter_backend::destination::{
        self, DestinationConfig, DestinationMetricsRegistry,
    };
    use rusty_mcrouter_backend::metrics::BackendMetricsShard;
    use rusty_mcrouter_backend::test_support::run_local;
    use rusty_mcrouter_backend::tko::TkoTrackerMap;
    use rusty_mcrouter_config::parse;
    use rusty_mcrouter_core::{RootRouteOptions, RoutingMetricsShard};
    use rusty_mcrouter_observability_primitives::test_support::noop_sink;
    use rusty_mcrouter_protocol::test_support::{get, get_miss};

    use super::*;
    use crate::{ProxyHandle, ProxyInbox};

    fn test_runtime(config: &str) -> (ProxyRuntime, ProxyHandle) {
        let (handle, inbox) = ProxyHandle::allocate(0);
        let ProxyInbox {
            work_rx,
            request_rx,
            command_rx,
        } = inbox;
        let proxies = ProxySet::new(vec![handle.clone()]);
        let map = destination::Map::new(
            TkoTrackerMap::new(noop_sink()),
            BackendMetricsShard::new(),
            DestinationMetricsRegistry::new(),
        );
        let defaults = DestinationConfig {
            reply_timeout: Duration::from_millis(100),
            ..DestinationConfig::default()
        };
        let builder = GenerationBuilder::new(
            map,
            defaults,
            RootRouteOptions::default(),
            RoutingMetricsShard::new(),
            noop_sink(),
        );
        let initial = builder.build(&parse(config).unwrap()).unwrap();
        let runtime = ProxyRuntime::new(
            0,
            initial,
            builder,
            proxies,
            ThreadMode::SameThread,
            FrontendMetricsShard::new(),
            request_rx,
            command_rx,
            work_rx,
            None,
            None,
        );
        (runtime, handle)
    }

    #[tokio::test]
    async fn routes_requests_and_acknowledges_shutdown() {
        run_local(async {
            let (runtime, handle) = test_runtime(r#"{"route": "NullRoute"}"#);
            let task = tokio::task::spawn_local(runtime.run());

            assert_eq!(handle.send_request(get(b"key")).await, get_miss());
            handle.shutdown().await.unwrap();
            task.await.unwrap().unwrap();
        })
        .await;
    }
}
