use std::sync::mpsc::SyncSender;

use anyhow::Context;
use rusty_mcrouter_observability::http::MetricsHttp;
use rusty_mcrouter_observability::{logging, ObservabilityParts};
use tokio::sync::{mpsc, oneshot};

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
    pub fn spawn(parts: ObservabilityParts) -> anyhow::Result<Self> {
        let (command_tx, command_rx) = mpsc::channel(CONTROL_COMMAND_CAPACITY);
        let handle = ControlHandle { command_tx };
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        let join = std::thread::Builder::new()
            .name("control".into())
            .spawn(move || control_thread_main(parts, command_rx, ready_tx))?;

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
        self.handle.shutdown_blocking()?;
        self.join
            .take()
            .expect("control thread exists")
            .join()
            .map_err(|_| anyhow::anyhow!("control thread panicked"))?
    }
}

struct ControlRuntime {
    command_rx: mpsc::Receiver<ControlCommand>,
    events: rusty_mcrouter_observability::bus::EventConsumer,
    metrics: Option<MetricsHttp>,
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

                result = step_metrics(&mut self.metrics), if self.metrics.is_some() => {
                    result?;
                }
            }
        }
    }

    async fn shutdown(&mut self) {
        while let Some(event) = self.events.try_recv() {
            logging::write(&event);
        }
        if let Some(metrics) = &mut self.metrics {
            metrics.shutdown().await;
        }
    }
}

async fn step_metrics(metrics: &mut Option<MetricsHttp>) -> anyhow::Result<()> {
    metrics.as_mut().expect("guarded by is_some").step().await
}

fn control_thread_main(
    parts: ObservabilityParts,
    command_rx: mpsc::Receiver<ControlCommand>,
    ready_tx: SyncSender<anyhow::Result<()>>,
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
    let metrics = match parts.metrics_listener {
        Some(listener) => match tokio::net::TcpListener::from_std(listener) {
            Ok(listener) => Some(MetricsHttp::new(
                listener,
                parts.registry,
                parts.http_rejected,
            )),
            Err(error) => {
                let error = anyhow::Error::from(error);
                let _ = ready_tx.send(Err(anyhow::anyhow!(error.to_string())));
                return Err(error);
            }
        },
        None => None,
    };
    drop(entered);

    let _ = ready_tx.send(Ok(()));
    runtime.block_on(
        ControlRuntime {
            command_rx,
            events: parts.consumer,
            metrics,
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
        let (_, parts) = observability.into_parts(None).unwrap();
        ControlThread::spawn(parts).unwrap().shutdown().unwrap();
        drop(events);
    }
}
