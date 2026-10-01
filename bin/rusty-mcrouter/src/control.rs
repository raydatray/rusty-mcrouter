use std::net::SocketAddr;
use std::sync::mpsc::{sync_channel, Receiver, Sender, SyncSender};
use std::sync::Arc;
use std::thread::{Builder, JoinHandle};

use anyhow::Context;
use rusty_mcrouter_control::ConfigReloader;
use rusty_mcrouter_observability::http::{MetricsHttp, MetricsHttpOptions, MetricsHttpSetup};
use rusty_mcrouter_observability::{logging, ControlMetrics, EventConsumer, MetricsRegistry};
// std mpsc for main's channels; tokio mpsc, module-qualified, for the runtime's
use tokio::sync::{mpsc, oneshot};

use crate::startup::report_startup;

pub struct ControlInbox {
    command_rx: mpsc::Receiver<ControlCommand>,
}

pub struct ControlThreadSetup {
    pub inbox: ControlInbox,
    pub events: EventConsumer,
    pub registry: Arc<MetricsRegistry>,
    pub metrics_addr: SocketAddr,
    pub metrics: Arc<ControlMetrics>,
    pub reloader: Option<ConfigReloader>,
    pub process_events: Sender<ProcessEvent>,
    pub http_options: MetricsHttpOptions,
}

pub enum ProcessEvent {
    ShutdownRequested,
    ProxyExited { id: usize },
    ControlExited,
}

/// main's end of the process-event channel
pub struct Supervisor {
    tx: Sender<ProcessEvent>,
    rx: Receiver<ProcessEvent>,
}

impl Supervisor {
    pub fn new() -> Self {
        let (tx, rx) = std::sync::mpsc::channel();
        Self { tx, rx }
    }

    pub fn exit_notifier(&self, on_exit: ProcessEvent) -> ExitNotifier {
        ExitNotifier {
            process_events: self.tx.clone(),
            event: Some(on_exit),
        }
    }

    pub fn sender(&self) -> Sender<ProcessEvent> {
        self.tx.clone()
    }

    pub fn wait(self) -> anyhow::Result<ProcessEvent> {
        drop(self.tx); // only threads may satisfy recv()
        self.rx
            .recv()
            .context("every thread exited without reporting")
    }
}

/// drop guard: fires when the owning thread body ends, panic included
pub struct ExitNotifier {
    process_events: Sender<ProcessEvent>,
    event: Option<ProcessEvent>,
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
    ProxiesReady,
    Shutdown { acknowledged: oneshot::Sender<()> },
}

#[derive(Clone)]
pub struct ControlHandle {
    command_tx: mpsc::Sender<ControlCommand>,
}

impl ControlHandle {
    pub fn allocate() -> (Self, ControlInbox) {
        let (command_tx, command_rx) = mpsc::channel(CONTROL_COMMAND_CAPACITY);
        (Self { command_tx }, ControlInbox { command_rx })
    }

    fn proxies_ready_blocking(&self) -> anyhow::Result<()> {
        self.command_tx
            .blocking_send(ControlCommand::ProxiesReady)
            .context("control command channel closed")
    }

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
    join: Option<JoinHandle<anyhow::Result<()>>>,
}

impl ControlThread {
    pub fn spawn(
        handle: ControlHandle,
        cfg: ControlThreadSetup,
        supervisor: &Supervisor,
    ) -> anyhow::Result<(Self, SocketAddr)> {
        let (ready_tx, ready_rx) = sync_channel::<ReadyEvent>(1);
        let exit = supervisor.exit_notifier(ProcessEvent::ControlExited);
        let join = Builder::new().name("control".into()).spawn(move || {
            let _exit = exit;
            control_thread_main(cfg, ready_tx)
        })?;

        let started = ready_rx
            .recv()
            .context("control thread died during startup")
            .and_then(|result| result);
        let metrics_addr = match started {
            Ok(addr) => addr,
            Err(error) => {
                let _ = join.join();
                return Err(error);
            }
        };

        Ok((
            Self {
                handle,
                join: Some(join),
            },
            metrics_addr,
        ))
    }

    /// Enable config reload polling after every proxy has started.
    pub fn proxies_ready(&self) -> anyhow::Result<()> {
        self.handle.proxies_ready_blocking()
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

impl Drop for ControlThread {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

struct ControlWorker {
    bound_addr: SocketAddr,
    command_rx: mpsc::Receiver<ControlCommand>,
    events: EventConsumer,
    metrics: MetricsHttp,
    reloader: Option<ConfigReloader>,
    proxies_ready: bool,
    process_events: Sender<ProcessEvent>,
}

impl ControlWorker {
    async fn build(setup: ControlThreadSetup) -> anyhow::Result<Self> {
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
            proxies_ready: false,
            process_events: setup.process_events,
        })
    }

    fn bound_addr(&self) -> SocketAddr {
        self.bound_addr
    }

    async fn run(mut self) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                biased;

                command = self.command_rx.recv() => {
                    match command {
                        Some(ControlCommand::ProxiesReady) => self.proxies_ready = true,
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

                // only the cancel-safe tick races; the reload runs to completion
                _ = tick(&mut self.reloader),
                    if self.proxies_ready && self.reloader.is_some() => {
                    self.reloader.as_mut().expect("guarded by is_some").poll().await;
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

async fn tick(reloader: &mut Option<ConfigReloader>) {
    reloader.as_mut().expect("guarded by is_some").tick().await;
}

fn control_thread_main(
    setup: ControlThreadSetup,
    ready_tx: SyncSender<ReadyEvent>,
) -> anyhow::Result<()> {
    let prepared = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("create control executor")
        .and_then(|executor| {
            let worker = executor.block_on(ControlWorker::build(setup))?;
            Ok((executor, worker))
        });
    let (executor, worker) = report_startup(prepared, ready_tx, |(_, worker)| worker.bound_addr())?;
    executor.block_on(worker.run())
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::time::Duration;

    use rusty_mcrouter_control::{ConfigMetrics, ReloaderSetup, RunningConfig};
    use rusty_mcrouter_observability::{channel, EventSender};
    use rusty_mcrouter_proxy::{ProxyCommand, ProxyHandle};

    use crate::config;

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
        let supervisor = Supervisor::new();
        let (handle, inbox) = ControlHandle::allocate();
        let cfg = ControlThreadSetup {
            inbox,
            events: consumer,
            registry: Arc::new(MetricsRegistry::new()),
            metrics_addr,
            metrics,
            reloader: None,
            process_events: supervisor.sender(),
            http_options: MetricsHttpOptions::default(),
        };
        let (control, bound) = ControlThread::spawn(handle, cfg, &supervisor)?;
        Ok((control, bound, events))
    }

    #[test]
    fn exit_notifier_reports_a_panicking_thread() {
        let supervisor = Supervisor::new();
        let exit = supervisor.exit_notifier(ProcessEvent::ProxyExited { id: 7 });
        let join = std::thread::spawn(move || {
            let _exit = exit;
            panic!("boom");
        });
        assert!(join.join().is_err());
        assert!(matches!(
            supervisor.wait().unwrap(),
            ProcessEvent::ProxyExited { id: 7 }
        ));
    }

    #[test]
    fn supervisor_wait_errors_when_nothing_can_report() {
        assert!(Supervisor::new().wait().is_err());
    }

    #[test]
    fn control_thread_acknowledges_shutdown_and_joins() {
        let (control, _, _events) = spawn_control(ephemeral()).unwrap();
        control.shutdown().unwrap();
    }

    #[test]
    fn dropping_control_thread_joins_and_releases_its_listener() {
        let (control, bound, _events) = spawn_control(ephemeral()).unwrap();
        drop(control);
        TcpListener::bind(bound).expect("control listener survived its thread owner");
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

    #[test]
    fn control_thread_serves_metrics_before_proxies_and_delays_reload_until_ready() {
        let initial = br#"{ "route": "NullRoute" }"#;
        let path = std::env::temp_dir().join(format!(
            "rusty-mcrouter-control-readiness-{}.json",
            std::process::id()
        ));
        std::fs::write(&path, br#"{ "route": "ErrorRoute|changed" }"#).unwrap();

        let metrics = Arc::new(ControlMetrics::default());
        let config_metrics = Arc::new(ConfigMetrics::default());
        let (_events, consumer) = channel(8, Arc::clone(&metrics));
        let (proxy, mut inbox) = ProxyHandle::allocate(0);
        let reloader = ConfigReloader::new(ReloaderSetup {
            path: path.clone(),
            delay: Duration::from_millis(5),
            running: RunningConfig {
                bytes: initial.to_vec(),
                document: Arc::new(config::parse(initial).unwrap()),
            },
            proxies: vec![proxy],
            defaults: Default::default(),
            root_options: Default::default(),
            metrics: Arc::clone(&config_metrics),
        });
        let supervisor = Supervisor::new();
        let (handle, inbox_control) = ControlHandle::allocate();
        let (control, bound) = ControlThread::spawn(
            handle,
            ControlThreadSetup {
                inbox: inbox_control,
                events: consumer,
                registry: Arc::new(MetricsRegistry::new()),
                metrics_addr: ephemeral(),
                metrics,
                reloader: Some(reloader),
                process_events: supervisor.sender(),
                http_options: MetricsHttpOptions::default(),
            },
            &supervisor,
        )
        .unwrap();

        // An ungated reload would await this not-yet-running proxy and stall HTTP.
        std::thread::sleep(Duration::from_millis(50));
        let mut stream = TcpStream::connect(bound).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).unwrap();
        assert!(response.starts_with("HTTP/1.1 200 OK\r\n"), "{response}");
        assert_eq!(config_metrics.reload_attempts.load(), 0);

        config_metrics.applied(1, 1);
        control.proxies_ready().unwrap();
        let command = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap()
            .block_on(async {
                tokio::time::timeout(Duration::from_secs(5), inbox.command_rx.recv())
                    .await
                    .expect("reload did not start after proxies became ready")
                    .expect("proxy command channel closed")
            });
        let ProxyCommand::Reconfigure {
            generation,
            applied,
            ..
        } = command
        else {
            panic!("expected a reconfigure command");
        };
        assert_eq!(generation, 2);
        applied.send(Ok(())).unwrap();

        control.shutdown().unwrap();
        std::fs::remove_file(path).unwrap();
        assert_eq!(config_metrics.reload_attempts.load(), 1);
        assert_eq!(config_metrics.generation.load(), 2);
    }
}
