use std::sync::mpsc::SyncSender;

use anyhow::Context;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

pub enum ProcessEvent {
    WorkerExited { id: usize },
    ControlExited,
}

/// main's end of the process-event channel
pub struct Supervisor {
    tx: UnboundedSender<ProcessEvent>,
    rx: UnboundedReceiver<ProcessEvent>,
}

/// drop guard: fires when the owning thread body ends, panic included
pub struct ExitNotifier {
    process_events: UnboundedSender<ProcessEvent>,
    event: Option<ProcessEvent>,
}

impl Supervisor {
    pub fn new() -> Self {
        let (tx, rx) = unbounded_channel();
        Self { tx, rx }
    }

    pub fn exit_notifier(&self, on_exit: ProcessEvent) -> ExitNotifier {
        ExitNotifier {
            process_events: self.tx.clone(),
            event: Some(on_exit),
        }
    }

    pub async fn wait(mut self) -> anyhow::Result<ProcessEvent> {
        drop(self.tx); // only threads may satisfy recv()
        self.rx
            .recv()
            .await
            .context("every thread exited without reporting")
    }
}

impl Drop for ExitNotifier {
    fn drop(&mut self) {
        if let Some(event) = self.event.take() {
            let _ = self.process_events.send(event);
        }
    }
}

/// Send exactly one readiness result, including executor or worker-build failures.
pub(crate) fn report_startup<T, A>(
    prepared: anyhow::Result<T>,
    ready_tx: SyncSender<anyhow::Result<A>>,
    address: impl FnOnce(&T) -> A,
) -> anyhow::Result<T> {
    match prepared {
        Ok(prepared) => {
            if ready_tx.send(Ok(address(&prepared))).is_err() {
                anyhow::bail!("startup receiver closed");
            }
            Ok(prepared)
        }
        Err(error) => {
            let _ = ready_tx.send(Err(anyhow::anyhow!("{error:#}")));
            Err(error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn exit_notifier_reports_a_panicking_thread() {
        let supervisor = Supervisor::new();
        let exit = supervisor.exit_notifier(ProcessEvent::WorkerExited { id: 7 });
        let join = std::thread::spawn(move || {
            let _exit = exit;
            panic!("boom");
        });
        assert!(matches!(
            supervisor.wait().await.unwrap(),
            ProcessEvent::WorkerExited { id: 7 }
        ));
        assert!(join.join().is_err());
    }

    #[tokio::test]
    async fn supervisor_wait_errors_when_nothing_can_report() {
        assert!(Supervisor::new().wait().await.is_err());
    }
}
