use std::{net::TcpStream, sync::Arc};

use anyhow::Context;
use rusty_mcrouter_config::ConfigDocument;
use rusty_mcrouter_core::BuildError;
use tokio::task::{JoinHandle, JoinSet};

use crate::connection::{Connection, FrontendConnectionSetup};
use crate::context::ProxyContext;
use crate::generation::GenerationBuilder;
use crate::routing::route_request;
use crate::{ProxyCommand, ProxyInbox, ProxyRequest};

/// Long-lived tasks the runtime supervises; either one exiting stops the proxy.
pub(crate) struct BackgroundTasks {
    pub(crate) listener: Option<JoinHandle<anyhow::Result<()>>>,
    pub(crate) sweep: Option<JoinHandle<()>>,
}

impl Drop for BackgroundTasks {
    fn drop(&mut self) {
        if let Some(listener) = self.listener.take() {
            listener.abort();
        }
        if let Some(sweep) = self.sweep.take() {
            sweep.abort();
        }
    }
}

pub(crate) struct ProxyRuntime {
    context: ProxyContext,
    builder: GenerationBuilder,
    inbox: ProxyInbox,
    tasks: BackgroundTasks,
    route_tasks: JoinSet<()>,
    connection_tasks: JoinSet<()>,
}

impl ProxyRuntime {
    pub(crate) fn new(
        context: ProxyContext,
        builder: GenerationBuilder,
        inbox: ProxyInbox,
        tasks: BackgroundTasks,
    ) -> Self {
        Self {
            context,
            builder,
            inbox,
            tasks,
            route_tasks: JoinSet::new(),
            connection_tasks: JoinSet::new(),
        }
    }

    pub(crate) async fn run(mut self) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                biased;

                command = self.inbox.command_rx.recv() => {
                    match command {
                        Some(ProxyCommand::Shutdown { acknowledged }) => {
                            self.shutdown().await;
                            let _ = acknowledged.send(());
                            return Ok(());
                        }
                        Some(ProxyCommand::Reconfigure { generation, config, applied }) => {
                            let _ = applied.send(self.reconfigure(generation, &config));
                        }
                        None => anyhow::bail!("proxy command channel closed"),
                    }
                }

                request = self.inbox.request_rx.recv() => {
                    let request = request.context("proxy request channel closed")?;
                    self.spawn_request(request);
                }

                stream = self.inbox.work_rx.recv() => {
                    let stream = stream.context("proxy work channel closed")?;
                    self.spawn_connection(stream);
                }

                Some(result) = self.route_tasks.join_next(), if !self.route_tasks.is_empty() => {
                    result.context("routed request task panicked")?;
                }

                Some(result) = self.connection_tasks.join_next(), if !self.connection_tasks.is_empty() => {
                    result.context("connection task panicked")?;
                }

                result = wait_for_listener(&mut self.tasks.listener), if self.tasks.listener.is_some() => {
                    return result;
                }

                result = wait_for_sweep(&mut self.tasks.sweep), if self.tasks.sweep.is_some() => {
                    return result;
                }
            }
        }
    }

    fn reconfigure(&mut self, generation: u64, config: &ConfigDocument) -> Result<(), BuildError> {
        debug_assert!(
            generation > self.context.routes.current().generation,
            "generations only move forward"
        );
        let next = self.builder.build(generation, config)?;
        drop(self.context.routes.replace(next));
        Ok(())
    }

    fn spawn_request(&mut self, request: ProxyRequest) {
        let generation = self.context.routes.current();
        self.route_tasks.spawn_local(async move {
            let reply = route_request(&generation, request.request).await;
            let _ = request.reply_tx.send(reply);
        });
    }

    fn spawn_connection(&mut self, stream: TcpStream) {
        let stream = match tokio::net::TcpStream::from_std(stream) {
            Ok(stream) => stream,
            Err(_) => return,
        };
        let connection = Connection::new(FrontendConnectionSetup {
            stream,
            request_tx: self.context.request_sender(),
            metrics: Arc::clone(&self.context.metrics),
            options: self.context.connection_options,
        });
        self.connection_tasks.spawn_local(async move {
            let _ = connection.run().await;
        });
    }

    async fn shutdown(&mut self) {
        self.inbox.request_rx.close();
        self.inbox.work_rx.close();
        if let Some(task) = self.tasks.listener.take() {
            task.abort();
        }
        if let Some(task) = self.tasks.sweep.take() {
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
    use std::rc::Rc;
    use std::sync::Arc;
    use std::time::Duration;

    use rusty_mcrouter_backend::destination::{
        self, DestinationConfig, DestinationMetricsRegistry,
    };
    use rusty_mcrouter_backend::metrics::BackendMetricsShard;
    use rusty_mcrouter_backend::test_support::{
        run_local, scripted_backend_serial, MockBackendFactory, Step,
    };
    use rusty_mcrouter_backend::tko::{DestTokenAllocator, TkoTrackerMap};
    use rusty_mcrouter_config::parse;
    use rusty_mcrouter_core::{build_route, RootRouteOptions, RoutingMetricsShard, RoutingState};
    use rusty_mcrouter_observability_primitives::test_support::noop_sink;
    use rusty_mcrouter_protocol::test_support::{get, get_miss, server_error};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::oneshot;
    use tokio::time::timeout;

    use super::*;
    use crate::generation::{GenerationSetup, RouteGeneration, RouteSlot};
    use crate::{FrontendMetricsShard, ProxyHandle};

    fn test_runtime(config: &str) -> (ProxyRuntime, ProxyHandle) {
        let (handle, inbox) = ProxyHandle::allocate(0);
        let map = destination::Map::new(destination::DestinationMapSetup {
            tko_map: TkoTrackerMap::new(noop_sink()),
            assembler: destination::DestinationAssembler::new(
                destination::DestinationAssemblerSetup {
                    tokens: Arc::new(DestTokenAllocator::new()),
                    metrics: DestinationMetricsRegistry::new(),
                    shard_metrics: BackendMetricsShard::new(),
                },
            ),
        });
        let defaults = DestinationConfig {
            reply_timeout: Duration::from_millis(100),
            ..DestinationConfig::default()
        };
        let builder = GenerationBuilder::new(GenerationSetup {
            destinations: map,
            defaults,
            root_options: RootRouteOptions::default(),
            metrics: RoutingMetricsShard::new(),
            events: noop_sink(),
        });
        let initial = builder.build(1, &parse(config).unwrap()).unwrap();
        let context = ProxyContext::solo(
            handle.clone(),
            RouteSlot::new(initial),
            FrontendMetricsShard::new(),
        );
        let tasks = BackgroundTasks {
            listener: None,
            sweep: None,
        };
        let runtime = ProxyRuntime::new(context, builder, inbox, tasks);
        (runtime, handle)
    }

    async fn reconfigure(
        handle: &ProxyHandle,
        generation: u64,
        config: &str,
    ) -> Result<(), BuildError> {
        let config = Arc::new(parse(config).unwrap());
        handle
            .begin_reconfigure(generation, config)
            .await
            .unwrap()
            .await
            .unwrap()
    }

    async fn connect(handle: &ProxyHandle) -> tokio::net::TcpStream {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (stream, _) = listener.accept().await.unwrap();
        handle
            .send_connection(stream.into_std().unwrap())
            .await
            .unwrap();
        client
    }

    async fn expect_reply(client: &mut tokio::net::TcpStream, expected: &[u8]) {
        let mut reply = vec![0u8; expected.len()];
        timeout(Duration::from_secs(2), client.read_exact(&mut reply))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reply, expected);
    }

    #[tokio::test]
    async fn existing_connection_uses_the_new_generation_on_its_next_request() {
        run_local(async {
            let (runtime, handle) = test_runtime(r#"{"route": "NullRoute"}"#);
            let task = tokio::task::spawn_local(runtime.run());
            let mut client = connect(&handle).await;

            client.write_all(b"mg foo v\r\n").await.unwrap();
            expect_reply(&mut client, b"EN\r\n").await;

            reconfigure(&handle, 2, r#"{"route": "ErrorRoute|two"}"#)
                .await
                .unwrap();
            client.write_all(b"mg foo v\r\n").await.unwrap();
            expect_reply(&mut client, b"SERVER_ERROR two\r\n").await;

            handle.shutdown().await.unwrap();
            task.await.unwrap().unwrap();
        })
        .await;
    }

    #[tokio::test]
    async fn queued_request_pins_the_generation_when_received() {
        run_local(async {
            let (runtime, handle) = test_runtime(r#"{"route": "ErrorRoute|one"}"#);
            let (reply_tx, reply_rx) = oneshot::channel();
            handle
                .request_sender()
                .send(ProxyRequest {
                    request: get(b"key"),
                    reply_tx,
                })
                .await
                .unwrap();
            let applied = handle
                .begin_reconfigure(
                    2,
                    Arc::new(parse(r#"{"route": "ErrorRoute|two"}"#).unwrap()),
                )
                .await
                .unwrap();

            // Both messages are queued before the runtime starts. Commands
            // take priority, so this request receives the new generation.
            let task = tokio::task::spawn_local(runtime.run());
            applied.await.unwrap().unwrap();
            assert_eq!(reply_rx.await.unwrap(), server_error(b"two"));

            handle.shutdown().await.unwrap();
            task.await.unwrap().unwrap();
        })
        .await;
    }

    #[tokio::test]
    async fn frontend_mailbox_requests_finish_pool_metrics() {
        run_local(async {
            let (runtime, handle) = test_runtime(r#"{"route": "NullRoute"}"#);
            let config = parse(
                r#"{"pools": {"pool": {"servers": ["unused:1"]}}, "route": "PoolRoute|pool"}"#,
            )
            .unwrap();
            let route = build_route(
                &config,
                &MockBackendFactory::new(),
                &DestinationConfig::default(),
            )
            .unwrap();
            let state =
                RoutingState::new(RoutingMetricsShard::new(), Rc::new(noop_sink()), &config);
            runtime.context.routes.replace(Rc::new(RouteGeneration {
                generation: 1,
                route,
                state: Rc::clone(&state),
            }));
            let metrics = Arc::clone(&runtime.context.metrics);
            let task = tokio::task::spawn_local(runtime.run());
            let mut client = connect(&handle).await;

            client
                .write_all(b"mg foo v\r\nmn\r\nnot_a_command\r\n")
                .await
                .unwrap();
            expect_reply(&mut client, b"EN\r\nMN\r\nERROR\r\n").await;

            let pool = state.pool(config.pool_id("pool").unwrap());
            assert_eq!(pool.requests.load(), 1);
            assert_eq!(pool.completed_requests.load(), 1);
            assert_eq!(pool.final_errors.load(), 0);
            assert_eq!(metrics.processing.load(), 0);

            handle.shutdown().await.unwrap();
            task.await.unwrap().unwrap();
            assert_eq!(metrics.client_connections.load(), 0);
        })
        .await;
    }

    #[tokio::test]
    async fn fatal_client_frame_closes_only_that_connection() {
        run_local(async {
            let (runtime, handle) = test_runtime(r#"{"route": "NullRoute"}"#);
            let task = tokio::task::spawn_local(runtime.run());
            let mut client = connect(&handle).await;
            client.write_all(&vec![b'x'; 32 * 1024 + 1]).await.unwrap();
            let mut replies = Vec::new();
            let closed = timeout(Duration::from_secs(2), client.read_to_end(&mut replies))
                .await
                .unwrap();
            if let Err(error) = closed {
                // Closing with unread oversized-frame bytes may reset the
                // socket instead of producing a clean EOF.
                assert_eq!(error.kind(), std::io::ErrorKind::ConnectionReset);
            }
            assert!(replies.is_empty());

            assert_eq!(handle.send_request(get(b"key")).await, get_miss());
            handle.shutdown().await.unwrap();
            task.await.unwrap().unwrap();
        })
        .await;
    }

    #[tokio::test]
    async fn unexpected_work_mailbox_closure_stops_the_runtime() {
        run_local(async {
            let (mut runtime, _handle) = test_runtime(r#"{"route": "NullRoute"}"#);
            runtime.inbox.work_rx.close();
            let error = runtime.run().await.unwrap_err();
            assert_eq!(error.to_string(), "proxy work channel closed");
        })
        .await;
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

    #[tokio::test]
    async fn reconfigure_swaps_the_graph_before_acknowledging() {
        run_local(async {
            let (runtime, handle) = test_runtime(r#"{"route": "NullRoute"}"#);
            let task = tokio::task::spawn_local(runtime.run());
            assert_eq!(handle.send_request(get(b"key")).await, get_miss());

            reconfigure(&handle, 2, r#"{"route": "ErrorRoute|two"}"#)
                .await
                .unwrap();
            assert_eq!(handle.send_request(get(b"key")).await, server_error(b"two"));

            handle.shutdown().await.unwrap();
            task.await.unwrap().unwrap();
        })
        .await;
    }

    #[tokio::test]
    async fn failed_reconfigure_keeps_the_current_generation() {
        run_local(async {
            let (runtime, handle) = test_runtime(r#"{"route": "NullRoute"}"#);
            let task = tokio::task::spawn_local(runtime.run());

            // plural routes without the default /././ prefix cannot build
            let error = reconfigure(&handle, 2, r#"{"routes": {"/a/b/": "NullRoute"}}"#)
                .await
                .unwrap_err();
            assert!(matches!(error, BuildError::DefaultRouteMissing { .. }));
            assert_eq!(handle.send_request(get(b"key")).await, get_miss());

            handle.shutdown().await.unwrap();
            task.await.unwrap().unwrap();
        })
        .await;
    }

    #[tokio::test]
    async fn request_in_flight_across_a_swap_finishes_on_the_old_graph() {
        run_local(async {
            let server =
                scripted_backend_serial(vec![vec![Step::ReadRequests(1), Step::Hang]]).await;
            let (runtime, handle) = test_runtime(&format!(
                r#"{{"pools": {{"p": {{"servers": ["{}"]}}}}, "route": "PoolRoute|p"}}"#,
                server.addr
            ));
            let task = tokio::task::spawn_local(runtime.run());

            let in_flight = tokio::task::spawn_local({
                let handle = handle.clone();
                async move { handle.send_request(get(b"key")).await }
            });
            while server.accept_count() == 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }

            reconfigure(&handle, 2, r#"{"route": "ErrorRoute|two"}"#)
                .await
                .unwrap();
            assert_eq!(handle.send_request(get(b"key")).await, server_error(b"two"));
            assert_eq!(
                in_flight.await.unwrap(),
                server_error(b"backend unavailable")
            );

            handle.shutdown().await.unwrap();
            task.await.unwrap().unwrap();
        })
        .await;
    }
}
