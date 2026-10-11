use std::net::SocketAddr;

use anyhow::Context;
use rusty_mcrouter_observability::http::{MetricsHttp, MetricsHttpSetup};
use rusty_mcrouter_observability::{logging, EventConsumer};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;

use crate::message::ControlCommand;
use crate::{ConfigReloader, ControlSetup};

pub struct ControlRuntime {
    bound_addr: SocketAddr,
    command_rx: mpsc::Receiver<ControlCommand>,
    events: EventConsumer,
    metrics_task: JoinSet<anyhow::Result<()>>,
    metrics_shutdown: Option<oneshot::Sender<()>>,
    reloader: Option<ConfigReloader>,
    workers_ready: bool,
}

impl ControlRuntime {
    pub async fn build(setup: ControlSetup) -> anyhow::Result<Self> {
        let listener = tokio::net::TcpListener::bind(setup.metrics_addr)
            .await
            .with_context(|| format!("bind({}) failed", setup.metrics_addr))?;
        let bound_addr = listener.local_addr()?;
        let (metrics_shutdown, shutdown_rx) = oneshot::channel();
        let metrics = MetricsHttp::new(MetricsHttpSetup {
            listener,
            source: setup.source,
            metrics: setup.metrics,
            options: setup.http_options,
            shutdown: shutdown_rx,
        });
        let mut metrics_task = JoinSet::new();
        metrics_task.spawn(metrics.run());
        Ok(Self {
            bound_addr,
            command_rx: setup.inbox.command_rx,
            events: setup.events,
            metrics_task,
            metrics_shutdown: Some(metrics_shutdown),
            reloader: setup.reloader,
            workers_ready: false,
        })
    }

    pub fn bound_addr(&self) -> SocketAddr {
        self.bound_addr
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                biased;

                command = self.command_rx.recv() => {
                    match command {
                        Some(ControlCommand::WorkersReady) => self.workers_ready = true,
                        Some(ControlCommand::Shutdown { acknowledged }) => {
                            self.shutdown().await?;
                            let _ = acknowledged.send(());
                            return Ok(());
                        }
                        None => anyhow::bail!("control command channel closed"),
                    }
                }

                event = self.events.recv() => {
                    let event = event.context("event channel closed unexpectedly")?;
                    logging::write(&event);
                }

                Some(result) = self.metrics_task.join_next() => {
                    result.context("metrics task failed")??;
                    anyhow::bail!("metrics server exited unexpectedly");
                }

                // Only the cancel-safe tick races; an apply runs to completion.
                _ = tick(&mut self.reloader),
                    if self.workers_ready && self.reloader.is_some() => {
                    self.reloader.as_mut().expect("guarded by is_some").poll().await;
                }
            }
        }
    }

    async fn shutdown(&mut self) -> anyhow::Result<()> {
        while let Some(event) = self.events.try_recv() {
            logging::write(&event);
        }
        if let Some(shutdown) = self.metrics_shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(result) = self.metrics_task.join_next().await {
            result.context("metrics task failed during shutdown")??;
        }
        Ok(())
    }
}

async fn tick(reloader: &mut Option<ConfigReloader>) {
    reloader.as_mut().expect("guarded by is_some").tick().await;
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use rusty_mcrouter_observability::http::MetricsHttpOptions;
    use rusty_mcrouter_observability::{channel, EventSender, ObservabilityMetrics};
    use rusty_mcrouter_observability_primitives::prometheus::{
        MetricsRegistry, MetricsSource, MetricsText,
    };
    use rusty_mcrouter_worker::{WorkerCommand, WorkerHandle};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    use super::*;
    use crate::{ConfigMetrics, ControlHandle, ReloaderSetup, RunningConfig};

    async fn scrape(addr: SocketAddr) -> String {
        tokio::time::timeout(Duration::from_secs(5), async {
            let mut stream = TcpStream::connect(addr).await.unwrap();
            stream
                .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
                .await
                .unwrap();
            let mut response = String::new();
            stream.read_to_string(&mut response).await.unwrap();
            response
        })
        .await
        .expect("metrics scrape timed out")
    }

    async fn build_runtime(
        source: MetricsRegistry,
    ) -> (ControlHandle, ControlRuntime, EventSender) {
        let metrics = Arc::new(ObservabilityMetrics::default());
        let (events, consumer) = channel(8, Arc::clone(&metrics));
        let (handle, inbox) = ControlHandle::allocate();
        let runtime = ControlRuntime::build(ControlSetup {
            inbox,
            events: consumer,
            source: Arc::new(source),
            metrics_addr: "127.0.0.1:0".parse().unwrap(),
            metrics,
            reloader: None,
            http_options: MetricsHttpOptions {
                request_timeout: Duration::from_secs(60),
                ..MetricsHttpOptions::default()
            },
        })
        .await
        .unwrap();
        (handle, runtime, events)
    }

    async fn assert_metrics_stopped(addr: SocketAddr, mut idle: TcpStream) {
        let mut response = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), idle.read_to_end(&mut response))
            .await
            .expect("idle metrics connection survived control")
            .unwrap();
        assert!(response.is_empty());
        TcpListener::bind(addr)
            .await
            .expect("metrics listener survived control");
    }

    #[tokio::test]
    async fn metrics_work_before_readiness_and_while_reload_waits_for_workers() {
        let initial = br#"{ "route": "NullRoute" }"#;
        let path = std::env::temp_dir().join(format!(
            "rusty-mcrouter-control-readiness-{}.json",
            std::process::id()
        ));
        std::fs::write(&path, br#"{ "route": "ErrorRoute|changed" }"#).unwrap();
        let metrics = Arc::new(ObservabilityMetrics::default());
        let config_metrics = Arc::new(ConfigMetrics::default());
        let (_events, consumer) = channel(8, Arc::clone(&metrics));
        let (worker, mut worker_inbox) = WorkerHandle::allocate(0);
        let reloader = ConfigReloader::new(ReloaderSetup {
            path: path.clone(),
            delay: Duration::from_millis(5),
            running: RunningConfig {
                bytes: initial.to_vec(),
                document: Arc::new(
                    rusty_mcrouter_config::parse(std::str::from_utf8(initial).unwrap()).unwrap(),
                ),
            },
            workers: vec![worker],
            defaults: Default::default(),
            root_options: Default::default(),
            metrics: Arc::clone(&config_metrics),
        });
        let (handle, inbox) = ControlHandle::allocate();
        let runtime = ControlRuntime::build(ControlSetup {
            inbox,
            events: consumer,
            source: Arc::new(MetricsRegistry::new()),
            metrics_addr: "127.0.0.1:0".parse().unwrap(),
            metrics,
            reloader: Some(reloader),
            http_options: MetricsHttpOptions {
                request_timeout: Duration::from_secs(60),
                ..MetricsHttpOptions::default()
            },
        })
        .await
        .unwrap();
        let bound = runtime.bound_addr();
        let task = tokio::spawn(runtime.run());
        tokio::time::sleep(Duration::from_millis(50)).await;
        let response = scrape(bound).await;
        assert!(response.starts_with("HTTP/1.1 200 OK\r\n"), "{response}");
        assert_eq!(config_metrics.reload_attempts.load(), 0);
        config_metrics.applied(1, 1);
        handle.workers_ready().await.unwrap();
        let command = tokio::time::timeout(Duration::from_secs(5), worker_inbox.command_rx.recv())
            .await
            .expect("reload did not start after workers became ready")
            .unwrap();
        let WorkerCommand::Reconfigure {
            generation,
            applied,
            ..
        } = command
        else {
            panic!("expected a reconfigure command");
        };
        assert_eq!(generation, 2);
        let idle = TcpStream::connect(bound).await.unwrap();
        let response = scrape(bound).await;
        assert!(response.starts_with("HTTP/1.1 200 OK\r\n"), "{response}");
        assert_eq!(config_metrics.generation.load(), 1);
        applied.send(Ok(())).unwrap();
        handle.shutdown().await.unwrap();
        task.await.unwrap().unwrap();
        assert_metrics_stopped(bound, idle).await;
        std::fs::remove_file(path).unwrap();
        assert_eq!(config_metrics.reload_attempts.load(), 1);
        assert_eq!(config_metrics.generation.load(), 2);
    }

    #[tokio::test]
    async fn aborting_control_stops_metrics_and_its_connections() {
        let (_handle, runtime, _events) = build_runtime(MetricsRegistry::new()).await;
        let bound = runtime.bound_addr();
        let task = tokio::spawn(runtime.run());
        let idle = TcpStream::connect(bound).await.unwrap();
        let response = scrape(bound).await;
        assert!(response.starts_with("HTTP/1.1 200 OK\r\n"), "{response}");

        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_metrics_stopped(bound, idle).await;
    }

    #[tokio::test]
    async fn metrics_failure_stops_control() {
        struct Panicking;

        impl MetricsSource for Panicking {
            fn encode(&self, _out: &mut MetricsText) {
                panic!("metrics encoding failed");
            }
        }

        let mut source = MetricsRegistry::new();
        source.register(Arc::new(Panicking));
        let (handle, runtime, _events) = build_runtime(source).await;
        let bound = runtime.bound_addr();
        let task = tokio::spawn(runtime.run());
        let idle = TcpStream::connect(bound).await.unwrap();
        let mut stream = TcpStream::connect(bound).await.unwrap();
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();

        let error = tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("control did not stop after metrics failed")
            .unwrap()
            .unwrap_err();
        assert!(format!("{error:#}").contains("metrics connection task failed"));
        assert!(handle.workers_ready().await.is_err());
        assert_metrics_stopped(bound, idle).await;
    }
}
