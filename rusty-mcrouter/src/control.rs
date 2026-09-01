use std::net::SocketAddr;
use std::sync::mpsc::SyncSender;
use std::sync::Arc;

use anyhow::Context;
use rusty_mcrouter_observability::http::MetricsHttp;
use rusty_mcrouter_observability::{logging, ControlMetrics, EventConsumer, MetricsRegistry};
use tokio::sync::{mpsc, oneshot};

pub struct ControlThreadConfig {
    pub events: EventConsumer,
    pub registry: Arc<MetricsRegistry>,
    pub metrics_addr: SocketAddr,
    pub metrics: Arc<ControlMetrics>,
}

pub enum ProcessEvent {
    ShutdownRequested,
    ProxyExited { id: usize },
    ControlExited,
}

pub(crate) struct ExitNotifier {
    process_events: std::sync::mpsc::Sender<ProcessEvent>,
    event: Option<ProcessEvent>,
}

impl ExitNotifier {
    pub(crate) fn new(
        process_events: std::sync::mpsc::Sender<ProcessEvent>,
        event: ProcessEvent,
    ) -> Self {
        Self {
            process_events,
            event: Some(event),
        }
    }
}

impl Drop for ExitNotifier {
    fn drop(&mut self) {
        if let Some(event) = self.event.take() {
            let _ = self.process_events.send(event);
        }
    }
}

const CONTROL_COMMAND_CAPACITY: usize = 16;

type ReadyEvent = anyhow::Result<SocketAddr>;

enum ControlCommand {
    Shutdown { acknowledged: oneshot::Sender<()> },
}

#[derive(Clone)]
pub struct ControlHandle {
    command_tx: mpsc::Sender<ControlCommand>,
}

impl ControlHandle {
    fn shutdown_blocking(&self) -> anyhow::Result<()> {
        let (acknowledged, acknowledgement) = oneshot::channel();
        self.command_tx
            .blocking_send(ControlCommand::Shutdown { acknowledged })
            .context("control command channel closed")?;
        acknowledgement
            .blocking_recv()
            .context("control thread exited before acknowledging shutdown")
    }
}

pub struct ControlThread {
    handle: ControlHandle,
    join: Option<std::thread::JoinHandle<anyhow::Result<()>>>,
}

impl ControlThread {
    pub fn spawn(
        cfg: ControlThreadConfig,
        process_events: std::sync::mpsc::Sender<ProcessEvent>,
    ) -> anyhow::Result<(Self, SocketAddr)> {
        let (command_tx, command_rx) = mpsc::channel(CONTROL_COMMAND_CAPACITY);
        let handle = ControlHandle { command_tx };
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel::<ReadyEvent>(1);
        let runtime_events = process_events.clone();
        let join = std::thread::Builder::new()
            .name("control".into())
            .spawn(move || {
                let _exit = ExitNotifier::new(process_events, ProcessEvent::ControlExited);
                control_thread_main(cfg, command_rx, ready_tx, runtime_events)
            })?;

        let metrics_addr = match ready_rx.recv() {
            Ok(result) => result?,
            Err(_) => anyhow::bail!("control thread died during startup"),
        };

        Ok((
            Self {
                handle,
                join: Some(join),
            },
            metrics_addr,
        ))
    }

    pub fn shutdown(mut self) -> anyhow::Result<()> {
        let shutdown = if self
            .join
            .as_ref()
            .is_some_and(std::thread::JoinHandle::is_finished)
        {
            Ok(())
        } else {
            self.handle.shutdown_blocking()
        };
        let joined = self
            .join
            .take()
            .expect("control thread exists")
            .join()
            .map_err(|_| anyhow::anyhow!("control thread panicked"))?;
        shutdown.and(joined)
    }
}

struct ControlRuntime {
    command_rx: mpsc::Receiver<ControlCommand>,
    events: rusty_mcrouter_observability::bus::EventConsumer,
    metrics: MetricsHttp,
    process_events: std::sync::mpsc::Sender<ProcessEvent>,
}

impl ControlRuntime {
    async fn run(mut self) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                biased;

                command = self.command_rx.recv() => {
                    match command {
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

                result = tokio::signal::ctrl_c() => {
                    result.context("listen for Ctrl-C")?;
                    let _ = self.process_events.send(ProcessEvent::ShutdownRequested);
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

fn control_thread_main(
    cfg: ControlThreadConfig,
    command_rx: mpsc::Receiver<ControlCommand>,
    ready_tx: SyncSender<ReadyEvent>,
    process_events: std::sync::mpsc::Sender<ProcessEvent>,
) -> anyhow::Result<()> {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            let error = anyhow::Error::from(error);
            let _ = ready_tx.send(Err(anyhow::anyhow!(error.to_string())));
            return Err(error);
        }
    };

    runtime.block_on(async move {
        let ControlThreadConfig {
            events,
            registry,
            metrics_addr,
            metrics: control_metrics,
        } = cfg;

        let listener = match tokio::net::TcpListener::bind(metrics_addr).await {
            Ok(listener) => listener,
            Err(error) => {
                let _ = ready_tx.send(Err(anyhow::anyhow!("bind({metrics_addr}) failed: {error}")));
                anyhow::bail!("bind({metrics_addr}) failed: {error}");
            }
        };
        let bound = listener.local_addr()?;
        let metrics = MetricsHttp::new(listener, registry, control_metrics);

        let _ = ready_tx.send(Ok(bound));
        drop(ready_tx);

        ControlRuntime {
            command_rx,
            events,
            metrics,
            process_events,
        }
        .run()
        .await
    })
}

#[cfg(test)]
mod tests {
    use rusty_mcrouter_observability::{channel, EventSender};

    use super::*;

    fn ephemeral() -> SocketAddr {
        "127.0.0.1:0".parse().unwrap()
    }

    // the returned sender keeps the bus open for the thread's lifetime
    fn spawn_control(
        metrics_addr: SocketAddr,
    ) -> anyhow::Result<(ControlThread, SocketAddr, EventSender)> {
        let metrics = Arc::new(ControlMetrics::default());
        let (events, consumer) = channel(8, Arc::clone(&metrics));
        let (process_tx, _process_rx) = std::sync::mpsc::channel();
        let cfg = ControlThreadConfig {
            events: consumer,
            registry: Arc::new(MetricsRegistry::new()),
            metrics_addr,
            metrics,
        };
        let (control, bound) = ControlThread::spawn(cfg, process_tx)?;
        Ok((control, bound, events))
    }

    #[test]
    fn control_thread_acknowledges_shutdown_and_joins() {
        let (control, _, _events) = spawn_control(ephemeral()).unwrap();
        control.shutdown().unwrap();
    }

    #[test]
    fn control_thread_binds_metrics_listener_and_reports_address() {
        let (control, bound, _events) = spawn_control(ephemeral()).unwrap();
        assert_ne!(bound.port(), 0);
        assert!(std::net::TcpStream::connect(bound).is_ok());
        control.shutdown().unwrap();
    }

    #[test]
    fn control_thread_reports_bind_failure_through_spawn() {
        let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let error = spawn_control(taken.local_addr().unwrap())
            .err()
            .expect("bind conflict surfaces as a spawn error");
        assert!(error.to_string().contains("bind("), "{error}");
    }
}
