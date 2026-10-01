use std::{net::SocketAddr, sync::Arc};

use rusty_mcrouter_observability::http::MetricsHttpOptions;
use rusty_mcrouter_observability::{ControlMetrics, EventConsumer, MetricsRegistry};

use crate::{ConfigReloader, ControlInbox};

pub struct ControlSetup {
    pub inbox: ControlInbox,
    pub events: EventConsumer,
    pub registry: Arc<MetricsRegistry>,
    pub metrics_addr: SocketAddr,
    pub metrics: Arc<ControlMetrics>,
    pub reloader: Option<ConfigReloader>,
    pub http_options: MetricsHttpOptions,
    pub request_shutdown: Box<dyn Fn() + Send + Sync>,
}
