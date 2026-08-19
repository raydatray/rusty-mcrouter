pub mod bus;
pub mod events;
pub mod http;
pub mod logging;
pub mod metrics;
pub mod sources;

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use rusty_mcrouter_observability_primitives::Counter;

use crate::bus::{channel, EventConsumer, EventSender};
use crate::metrics::{MetricsRegistry, MetricsSource};

/// the wiring handle: construct FIRST in main (installs the tracing
/// subscriber), hand out sinks, register sources, then spawn() the
/// control thread (bus consumer + optional /metrics server).
pub struct Observability {
    events: EventSender,
    consumer: EventConsumer,
    registry: MetricsRegistry,
    http_rejected: Arc<Counter>,
}

impl Observability {
    pub fn new(bus_capacity: usize) -> Self {
        // logs go to stderr: stdout is the READY/METRICS control channel.
        // try_init so tests constructing multiple instances don't panic.
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::builder()
                    .with_default_directive(tracing::level_filters::LevelFilter::INFO.into())
                    .from_env_lossy(),
            )
            .with_writer(io::stderr)
            .try_init();

        let (events, consumer) = channel(bus_capacity);
        Self {
            events,
            consumer,
            registry: MetricsRegistry::new(),
            http_rejected: Arc::new(Counter::default()),
        }
    }

    pub fn events(&self) -> &EventSender {
        &self.events
    }

    pub fn register(&mut self, source: Box<dyn MetricsSource>) {
        self.registry.register(source);
    }

    pub fn http_rejected_counter(&self) -> Arc<Counter> {
        Arc::clone(&self.http_rejected)
    }

    /// binds the metrics listener (if any) and spawns the control thread.
    /// returns the bound address so an ephemeral port can be reported.
    /// the consumer runs until every EventSender clone is dropped.
    pub fn spawn(self, metrics_addr: Option<SocketAddr>) -> io::Result<Option<SocketAddr>> {
        let listener = match metrics_addr {
            Some(addr) => {
                let listener = std::net::TcpListener::bind(addr)?;
                listener.set_nonblocking(true)?;
                Some(listener)
            }
            None => None,
        };
        let bound = listener.as_ref().map(|l| l.local_addr()).transpose()?;

        let Observability {
            events,
            mut consumer,
            registry,
            http_rejected,
        } = self;
        drop(events); // sinks hold their own clones; ours must not keep the consumer alive

        std::thread::Builder::new()
            .name("observability".into())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_io()
                    .enable_time()
                    .build()
                    .expect("observability runtime");
                let result = rt.block_on(async move {
                    let registry = Arc::new(registry);
                    let mut metrics = listener.map(|listener| {
                        http::MetricsHttp::new(
                            tokio::net::TcpListener::from_std(listener)
                                .expect("register metrics listener"),
                            registry,
                            http_rejected,
                        )
                    });

                    loop {
                        match &mut metrics {
                            Some(metrics) => tokio::select! {
                                event = consumer.recv() => {
                                    let Some(event) = event else {
                                        anyhow::bail!("event channel closed");
                                    };
                                    logging::write(&event);
                                }
                                result = metrics.step() => result?,
                            },
                            None => {
                                let Some(event) = consumer.recv().await else {
                                    return Ok(());
                                };
                                logging::write(&event);
                            }
                        }
                    }
                });
                if let Err(error) = result {
                    tracing::error!(%error, "observability service stopped");
                }
            })?;

        Ok(bound)
    }
}
