use rusty_mcrouter_observability_primitives::prometheus::{MetricsSource, MetricsText};
use rusty_mcrouter_observability_primitives::Counter;

#[derive(Default)]
pub struct ObservabilityMetrics {
    pub events_dropped: Counter,
    pub http_rejected: Counter,
}

#[derive(Clone, Copy, Debug)]
pub struct ProcessMetadata {
    pub start_unix_secs: u64, // computed at startup once
    pub num_proxies: usize,
}

impl MetricsSource for ObservabilityMetrics {
    fn encode(&self, out: &mut MetricsText) {
        out.counter(
            "rusty_mcrouter_events_dropped_total",
            &[],
            self.events_dropped.load(),
        );
        out.counter(
            "rusty_mcrouter_metrics_http_rejected_total",
            &[],
            self.http_rejected.load(),
        );
    }
}

impl MetricsSource for ProcessMetadata {
    fn encode(&self, out: &mut MetricsText) {
        out.gauge("rusty_mcrouter_proxies", &[], self.num_proxies as i64);
        out.gauge(
            "rusty_mcrouter_start_time_seconds",
            &[],
            self.start_unix_secs as i64,
        );
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn observability_and_process_metrics_golden() {
        let metrics = Arc::new(ObservabilityMetrics::default());
        metrics.events_dropped.add(2);
        metrics.http_rejected.add(3);
        let metadata = ProcessMetadata {
            num_proxies: 4,
            start_unix_secs: 1_700_000_000,
        };
        assert_eq!(
            format!("{}{}", metrics.render(), metadata.render()),
            "rusty_mcrouter_events_dropped_total 2\n\
             rusty_mcrouter_metrics_http_rejected_total 3\n\
             rusty_mcrouter_proxies 4\n\
             rusty_mcrouter_start_time_seconds 1700000000\n"
        );
    }
}
