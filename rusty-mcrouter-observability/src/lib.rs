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

pub struct ObservabilityParts {
    pub consumer: EventConsumer,
    pub registry: Arc<MetricsRegistry>,
    pub metrics_listener: std::net::TcpListener,
    pub http_rejected: Arc<Counter>,
}

/// the wiring handle: hand out sinks, register sources, then spawn() the
/// control thread (bus consumer + optional /metrics server).
pub struct Observability {
    events: EventSender,
    consumer: EventConsumer,
    registry: MetricsRegistry,
    http_rejected: Arc<Counter>,
}

impl Observability {
    pub fn new(bus_capacity: usize) -> Self {
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

    pub fn into_parts(
        self,
        metrics_addr: SocketAddr,
    ) -> io::Result<(SocketAddr, ObservabilityParts)> {
        let listener = std::net::TcpListener::bind(metrics_addr)?;
        listener.set_nonblocking(true)?;
        let bound = listener.local_addr()?;

        drop(self.events); // leaf-owned sinks keep the event channel alive
        Ok((
            bound,
            ObservabilityParts {
                consumer: self.consumer,
                registry: Arc::new(self.registry),
                metrics_listener: listener,
                http_rejected: self.http_rejected,
            },
        ))
    }
}
