use std::sync::Arc;

use rusty_mcrouter_observability_primitives::{Counter, Gauge};
use rusty_mcrouter_protocol::RequestKind;

#[derive(Default)]
pub struct FrontendMetricsShard {
    pub requests: [Counter; RequestKind::COUNT],
    pub noops: Counter,
    pub parse_errors: Counter,
    pub failed: Counter,
    pub client_connections: Gauge,
    pub processing: Gauge,
}

impl FrontendMetricsShard {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
}
