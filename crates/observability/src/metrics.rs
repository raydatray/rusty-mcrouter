use rusty_mcrouter_observability_primitives::Counter;

#[derive(Default)]
pub struct ObservabilityMetrics {
    pub events_dropped: Counter,
    pub http_rejected: Counter,
}
