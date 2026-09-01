pub mod bus;
pub mod events;
pub mod http;
pub mod logging;
pub mod metrics;
pub mod sources;

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use crate::bus::{channel, EventConsumer, EventSender};
use crate::metrics::{ControlMetrics, MetricsRegistry, MetricsSource};

pub struct ObservabilityParts {
    pub consumer: EventConsumer,
    pub registry: Arc<MetricsRegistry>,
    pub metrics_listener: std::net::TcpListener,
    pub metrics: Arc<ControlMetrics>,
}

/// the wiring handle: hand out sinks, register sources, then spawn() the
/// control thread (bus consumer + optional /metrics server).
pub struct Observability {
    events: EventSender,
    consumer: EventConsumer,
    registry: MetricsRegistry,
    metrics: Arc<ControlMetrics>,
}

impl Observability {
    pub fn new(bus_capacity: usize) -> Self {
        let metrics = Arc::new(ControlMetrics::default());
        let (events, consumer) = channel(bus_capacity, Arc::clone(&metrics));
        Self {
            events,
            consumer,
            registry: MetricsRegistry::new(),
            metrics,
        }
    }

    pub fn events(&self) -> &EventSender {
        &self.events
    }

    pub fn register(&mut self, source: Box<dyn MetricsSource>) {
        self.registry.register(source);
    }

    pub fn control_metrics(&self) -> Arc<ControlMetrics> {
        Arc::clone(&self.metrics)
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
                metrics: self.metrics,
            },
        ))
    }
}
