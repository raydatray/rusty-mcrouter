use std::net::SocketAddr;
use std::sync::mpsc::{sync_channel, SyncSender};
use std::thread::{Builder, JoinHandle};

use anyhow::Context;
use rusty_mcrouter_control::{ControlHandle, ControlRuntime, ControlSetup};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::startup::report_startup;

type ReadyEvent = anyhow::Result<SocketAddr>;

pub enum ProcessEvent {
    ProxyExited { id: usize },
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

pub struct ControlThreadOwner {
    handle: ControlHandle,
    join: Option<JoinHandle<anyhow::Result<()>>>,
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

impl ControlThreadOwner {
    pub fn spawn(
        handle: ControlHandle,
        setup: ControlSetup,
        supervisor: &Supervisor,
    ) -> anyhow::Result<(Self, SocketAddr)> {
        let (ready_tx, ready_rx) = sync_channel::<ReadyEvent>(1);
        let exit = supervisor.exit_notifier(ProcessEvent::ControlExited);
        let join = Builder::new().name("control".into()).spawn(move || {
            let _exit = exit;
            control_thread_main(setup, ready_tx)
        })?;

        let owner = Self {
            handle,
            join: Some(join),
        };
        let metrics_addr = ready_rx
            .recv()
            .context("control thread died during startup")
            .and_then(|result| result)?;

        Ok((owner, metrics_addr))
    }

    pub fn shutdown(mut self) -> anyhow::Result<()> {
        self.stop()
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        let Some(join) = self.join.take() else {
            return Ok(());
        };
        let shutdown = if join.is_finished() {
            Ok(())
        } else {
            self.handle.shutdown_blocking()
        };
        let joined = join
            .join()
            .map_err(|_| anyhow::anyhow!("control thread panicked"))?;
        shutdown.and(joined)
    }
}

impl Drop for ControlThreadOwner {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

fn control_thread_main(
    setup: ControlSetup,
    ready_tx: SyncSender<ReadyEvent>,
) -> anyhow::Result<()> {
    let prepared = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("create control executor")
        .and_then(|executor| {
            let runtime = executor.block_on(ControlRuntime::build(setup))?;
            Ok((executor, runtime))
        });
    let (executor, runtime) =
        report_startup(prepared, ready_tx, |(_, runtime)| runtime.bound_addr())?;
    executor.block_on(runtime.run())
}

#[cfg(test)]
mod tests {
    use std::net::{TcpListener, TcpStream};
    use std::sync::Arc;

    use rusty_mcrouter_observability::http::MetricsHttpOptions;
    use rusty_mcrouter_observability::{channel, ControlMetrics, EventSender, MetricsRegistry};

    use super::*;

    fn ephemeral() -> SocketAddr {
        "127.0.0.1:0".parse().unwrap()
    }

    // the returned sender keeps the bus open for the thread's lifetime
    fn spawn_control(
        metrics_addr: SocketAddr,
    ) -> anyhow::Result<(ControlThreadOwner, SocketAddr, EventSender)> {
        let metrics = Arc::new(ControlMetrics::default());
        let (events, consumer) = channel(8, Arc::clone(&metrics));
        let supervisor = Supervisor::new();
        let (handle, inbox) = ControlHandle::allocate();
        let cfg = ControlSetup {
            inbox,
            events: consumer,
            registry: Arc::new(MetricsRegistry::new()),
            metrics_addr,
            metrics,
            reloader: None,
            http_options: MetricsHttpOptions::default(),
        };
        let (control, bound) = ControlThreadOwner::spawn(handle, cfg, &supervisor)?;
        Ok((control, bound, events))
    }

    #[tokio::test]
    async fn exit_notifier_reports_a_panicking_thread() {
        let supervisor = Supervisor::new();
        let exit = supervisor.exit_notifier(ProcessEvent::ProxyExited { id: 7 });
        let join = std::thread::spawn(move || {
            let _exit = exit;
            panic!("boom");
        });
        assert!(matches!(
            supervisor.wait().await.unwrap(),
            ProcessEvent::ProxyExited { id: 7 }
        ));
        assert!(join.join().is_err());
    }

    #[tokio::test]
    async fn supervisor_wait_errors_when_nothing_can_report() {
        assert!(Supervisor::new().wait().await.is_err());
    }

    #[test]
    fn control_thread_acknowledges_shutdown_and_joins() {
        let (control, _, _events) = spawn_control(ephemeral()).unwrap();
        control.shutdown().unwrap();
    }

    #[test]
    fn dropping_control_thread_joins_and_releases_its_listener() {
        let (control, bound, _events) = spawn_control(ephemeral()).unwrap();
        let handle = control.handle.clone();
        drop(control);
        TcpListener::bind(bound).expect("control listener survived its thread owner");
        assert!(handle.proxies_ready_blocking().is_err());
    }

    #[test]
    fn control_thread_binds_metrics_listener_and_reports_address() {
        let (control, bound, _events) = spawn_control(ephemeral()).unwrap();
        assert_ne!(bound.port(), 0);
        assert!(TcpStream::connect(bound).is_ok());
        control.shutdown().unwrap();
    }

    #[test]
    fn control_thread_reports_bind_failure_through_spawn() {
        let taken = TcpListener::bind("127.0.0.1:0").unwrap();
        let error = spawn_control(taken.local_addr().unwrap())
            .err()
            .expect("bind conflict surfaces as a spawn error");
        assert!(error.to_string().contains("bind("), "{error}");
    }
}
