use std::net::SocketAddr;

use anyhow::Context;
use rusty_mcrouter_observability::http::{MetricsHttp, MetricsHttpSetup};
use rusty_mcrouter_observability::{logging, EventConsumer};
use tokio::sync::mpsc;

use crate::message::ControlCommand;
use crate::{ConfigReloader, ControlSetup};

pub struct ControlRuntime {
    bound_addr: SocketAddr,
    command_rx: mpsc::Receiver<ControlCommand>,
    events: EventConsumer,
    metrics: MetricsHttp,
    reloader: Option<ConfigReloader>,
    workers_ready: bool,
}

impl ControlRuntime {
    pub async fn build(setup: ControlSetup) -> anyhow::Result<Self> {
        let listener = tokio::net::TcpListener::bind(setup.metrics_addr)
            .await
            .with_context(|| format!("bind({}) failed", setup.metrics_addr))?;
        let bound_addr = listener.local_addr()?;
        let metrics = MetricsHttp::new(MetricsHttpSetup {
            listener,
            registry: setup.registry,
            metrics: setup.metrics,
            options: setup.http_options,
        });
        Ok(Self {
            bound_addr,
            command_rx: setup.inbox.command_rx,
            events: setup.events,
            metrics,
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
                            self.shutdown().await;
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

                result = self.metrics.step() => {
                    result?;
                }

                // Only the cancel-safe tick races; an apply runs to completion.
                _ = tick(&mut self.reloader),
                    if self.workers_ready && self.reloader.is_some() => {
                    self.reloader.as_mut().expect("guarded by is_some").poll().await;
                }
            }
        }
    }

    async fn shutdown(&mut self) {
        while let Some(event) = self.events.try_recv() {
            logging::write(&event);
        }
        self.metrics.shutdown().await;
    }
}

async fn tick(reloader: &mut Option<ConfigReloader>) {
    reloader.as_mut().expect("guarded by is_some").tick().await;
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use rusty_mcrouter_observability::http::MetricsHttpOptions;
    use rusty_mcrouter_observability::{channel, ControlMetrics, MetricsRegistry};
    use rusty_mcrouter_worker::{WorkerCommand, WorkerHandle};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;
    use crate::{ConfigMetrics, ControlHandle, ReloaderSetup, RunningConfig};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn metrics_work_before_worker_readiness_and_reload_waits_for_it() {
        let initial = br#"{ "route": "NullRoute" }"#;
        let path = std::env::temp_dir().join(format!(
            "rusty-mcrouter-control-readiness-{}.json",
            std::process::id()
        ));
        std::fs::write(&path, br#"{ "route": "ErrorRoute|changed" }"#).unwrap();
        let metrics = Arc::new(ControlMetrics::default());
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
            registry: Arc::new(MetricsRegistry::new()),
            metrics_addr: "127.0.0.1:0".parse().unwrap(),
            metrics,
            reloader: Some(reloader),
            http_options: MetricsHttpOptions::default(),
        })
        .await
        .unwrap();
        let bound = runtime.bound_addr();
        let task = tokio::spawn(runtime.run());
        tokio::time::sleep(Duration::from_millis(50)).await;
        let mut stream = tokio::net::TcpStream::connect(bound).await.unwrap();
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        tokio::time::timeout(Duration::from_secs(5), stream.read_to_string(&mut response))
            .await
            .unwrap()
            .unwrap();
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
        applied.send(Ok(())).unwrap();
        handle.shutdown().await.unwrap();
        task.await.unwrap().unwrap();
        std::fs::remove_file(path).unwrap();
        assert_eq!(config_metrics.reload_attempts.load(), 1);
        assert_eq!(config_metrics.generation.load(), 2);
    }
}
