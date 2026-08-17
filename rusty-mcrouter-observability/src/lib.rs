pub mod bus;
pub mod events;
pub mod frontend;
pub mod http;
pub mod logging;
pub mod metrics;
pub mod sources;

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use crate::bus::{channel, EventConsumer, EventSender};
use crate::metrics::{MetricsRegistry, MetricsSource};

/// the wiring handle: construct FIRST in main (installs the tracing
/// subscriber), hand out sinks, register sources, then spawn() the
/// control thread (bus consumer + optional /metrics server).
pub struct Observability {
    events: EventSender,
    consumer: EventConsumer,
    registry: MetricsRegistry,
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
        }
    }

    pub fn events(&self) -> &EventSender {
        &self.events
    }

    pub fn register(&mut self, source: Box<dyn MetricsSource>) {
        self.registry.register(source);
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
            consumer,
            registry,
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
                rt.block_on(async move {
                    if let Some(listener) = listener {
                        let listener = tokio::net::TcpListener::from_std(listener)
                            .expect("register metrics listener");
                        tokio::spawn(http::serve(listener, Arc::new(registry)));
                    }
                    consumer.run().await;
                });
            })?;

        Ok(bound)
    }
}
