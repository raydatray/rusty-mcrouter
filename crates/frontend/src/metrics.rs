use std::sync::Arc;

use rusty_mcrouter_observability_primitives::prometheus::{MetricsSource, MetricsText};
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

pub struct FrontendMetrics {
    pub shards: Vec<Arc<FrontendMetricsShard>>,
}

impl FrontendMetrics {
    fn sum(&self, load: impl Fn(&FrontendMetricsShard) -> u64) -> u64 {
        self.shards.iter().map(|shard| load(shard)).sum()
    }

    fn sum_gauge(&self, load: impl Fn(&FrontendMetricsShard) -> i64) -> i64 {
        self.shards.iter().map(|shard| load(shard)).sum()
    }
}

impl MetricsSource for FrontendMetrics {
    fn encode(&self, out: &mut MetricsText) {
        out.counter(
            "rusty_mcrouter_noops_total",
            &[],
            self.sum(|shard| shard.noops.load()),
        );
        out.counter(
            "rusty_mcrouter_parse_errors_total",
            &[],
            self.sum(|shard| shard.parse_errors.load()),
        );
        out.counter(
            "rusty_mcrouter_requests_failed_total",
            &[],
            self.sum(|shard| shard.failed.load()),
        );
        out.gauge(
            "rusty_mcrouter_client_connections",
            &[],
            self.sum_gauge(|shard| shard.client_connections.load()),
        );
        out.gauge(
            "rusty_mcrouter_requests_processing",
            &[],
            self.sum_gauge(|shard| shard.processing.load()),
        );

        for kind in RequestKind::ALL {
            out.counter(
                "rusty_mcrouter_requests_total",
                &[("command", kind.meta_command())],
                self.sum(|shard| shard.requests[kind as usize].load()),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frontend_metrics_sum_real_shards() {
        let s1 = FrontendMetricsShard::new();
        let s2 = FrontendMetricsShard::new();
        s1.requests[RequestKind::Get as usize].add(3);
        s2.requests[RequestKind::Get as usize].add(2);
        s1.failed.inc();
        s2.failed.add(2);
        s1.noops.inc();
        s2.noops.add(2);
        s1.parse_errors.add(3);
        s2.parse_errors.inc();
        s1.client_connections.add(2);
        s2.client_connections.sub(1);
        s1.processing.inc();
        s2.processing.add(2);

        let text = FrontendMetrics {
            shards: vec![s1, s2],
        }
        .render();
        assert!(text.contains("rusty_mcrouter_requests_total{command=\"mg\"} 5\n"));
        assert!(text.contains("rusty_mcrouter_requests_failed_total 3\n"));
        assert!(text.contains("rusty_mcrouter_noops_total 3\n"));
        assert!(text.contains("rusty_mcrouter_parse_errors_total 4\n"));
        assert!(text.contains("rusty_mcrouter_client_connections 1\n"));
        assert!(text.contains("rusty_mcrouter_requests_processing 3\n"));
        assert_eq!(
            text.matches("rusty_mcrouter_requests_total{command=\"mg\"}")
                .count(),
            1
        );
        assert!(
            text.find("rusty_mcrouter_requests_processing").unwrap()
                < text.find("rusty_mcrouter_requests_total").unwrap()
        );
    }
}
