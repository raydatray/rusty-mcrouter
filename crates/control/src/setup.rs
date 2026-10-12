use std::{net::SocketAddr, sync::Arc};

use rusty_mcrouter_observability::http::MetricsHttpOptions;
use rusty_mcrouter_observability::{ApplicationMetrics, EventConsumer, ObservabilityMetrics};

use crate::{ConfigReloader, ControlInbox};

pub struct ControlSetup {
    pub inbox: ControlInbox,
    pub events: EventConsumer,
    pub source: Arc<ApplicationMetrics>,
    pub metrics_addr: SocketAddr,
    pub metrics: Arc<ObservabilityMetrics>,
    pub reloader: Option<ConfigReloader>,
    pub http_options: MetricsHttpOptions,
}
