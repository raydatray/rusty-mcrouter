use std::sync::mpsc::SyncSender;

use anyhow::Context;
use rusty_mcrouter_observability::http::MetricsHttp;
use rusty_mcrouter_observability::{logging, ObservabilityParts};
use tokio::sync::{mpsc, oneshot};

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
        parts: ObservabilityParts,
        process_events: std::sync::mpsc::Sender<ProcessEvent>,
    ) -> anyhow::Result<Self> {
        let (command_tx, command_rx) = mpsc::channel(CONTROL_COMMAND_CAPACITY);
        let handle = ControlHandle { command_tx };
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        let runtime_events = process_events.clone();
        let join = std::thread::Builder::new()
            .name("control".into())
            .spawn(move || {
                let _exit = ExitNotifier::new(process_events, ProcessEvent::ControlExited);
                control_thread_main(parts, command_rx, ready_tx, runtime_events)
            })?;

        match ready_rx.recv() {
            Ok(result) => result?,
            Err(_) => anyhow::bail!("control thread died during startup"),
        }

        Ok(Self {
            handle,
            join: Some(join),
        })
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
    parts: ObservabilityParts,
    command_rx: mpsc::Receiver<ControlCommand>,
    ready_tx: SyncSender<anyhow::Result<()>>,
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

    let entered = runtime.enter();
    let metrics = match tokio::net::TcpListener::from_std(parts.metrics_listener) {
        Ok(listener) => MetricsHttp::new(listener, parts.registry, parts.metrics),
        Err(error) => {
            let error = anyhow::Error::from(error);
            let _ = ready_tx.send(Err(anyhow::anyhow!(error.to_string())));
            return Err(error);
        }
    };
    drop(entered);

    let _ = ready_tx.send(Ok(()));
    runtime.block_on(
        ControlRuntime {
            command_rx,
            events: parts.consumer,
            metrics,
            process_events,
        }
        .run(),
    )
}

#[cfg(test)]
mod tests {
    use rusty_mcrouter_observability::Observability;

    use super::*;

    #[test]
    fn control_thread_acknowledges_shutdown_and_joins() {
        let observability = Observability::new(8);
        let events = observability.events().clone();
        let (_, parts) = observability
            .into_parts("127.0.0.1:0".parse().unwrap())
            .unwrap();
        let (process_tx, _process_rx) = std::sync::mpsc::channel();
        ControlThread::spawn(parts, process_tx)
            .unwrap()
            .shutdown()
            .unwrap();
        drop(events);
    }
}
