use std::sync::Arc;

use rusty_mcrouter_observability_primitives::prometheus::{MetricsSource, MetricsText};
use rusty_mcrouter_observability_primitives::{Counter, Gauge};
use rusty_mcrouter_protocol::RequestKind;

use crate::classify::ResultCode;
use crate::destination::DestinationMetricsRegistry;
use crate::tko::TkoTrackerMap;

#[derive(Default)]
#[repr(align(64))]
// this is one shard. we must align this to a cache line so that no two
// thread's shards share one cache line
pub struct BackendMetricsShard {
    // monotonic counters
    pub requests: [[Counter; ResultCode::COUNT]; RequestKind::COUNT],
    pub latency_us_sum: Counter,
    pub connections_opened: Counter,
    pub connections_closed: Counter,
    pub connect_retries: Counter,
    pub connect_success_after_retry: Counter,
    pub write_batches: Counter,
    pub batched_requests: Counter,
    pub queue_full: Counter,
    pub bytes_read: Counter,
    pub bytes_written: Counter,

    // gauges
    pub pending_reqs: Gauge,
    pub inflight_reqs: Gauge,
}

impl BackendMetricsShard {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn record_send(&self, kind: RequestKind, code: ResultCode, latency_us: u64) {
        self.requests[kind as usize][code as usize].inc();
        self.latency_us_sum.add(latency_us);
    }

    pub fn record_result(&self, kind: RequestKind, code: ResultCode) {
        self.requests[kind as usize][code as usize].inc();
    }
}

pub struct BackendMetrics {
    pub shards: Vec<Arc<BackendMetricsShard>>,
}

impl BackendMetrics {
    fn sum(&self, load: impl Fn(&BackendMetricsShard) -> u64) -> u64 {
        self.shards.iter().map(|shard| load(shard)).sum()
    }

    fn sum_gauge(&self, load: impl Fn(&BackendMetricsShard) -> i64) -> i64 {
        self.shards.iter().map(|shard| load(shard)).sum()
    }
}

impl MetricsSource for BackendMetrics {
    fn encode(&self, out: &mut MetricsText) {
        out.counter(
            "rusty_mcrouter_backend_latency_us_sum_total",
            &[],
            self.sum(|shard| shard.latency_us_sum.load()),
        );
        out.counter(
            "rusty_mcrouter_backend_connections_opened_total",
            &[],
            self.sum(|shard| shard.connections_opened.load()),
        );
        out.counter(
            "rusty_mcrouter_backend_connections_closed_total",
            &[],
            self.sum(|shard| shard.connections_closed.load()),
        );
        out.counter(
            "rusty_mcrouter_backend_connect_retries_total",
            &[],
            self.sum(|shard| shard.connect_retries.load()),
        );
        out.counter(
            "rusty_mcrouter_backend_connect_retry_successes_total",
            &[],
            self.sum(|shard| shard.connect_success_after_retry.load()),
        );
        out.counter(
            "rusty_mcrouter_backend_write_batches_total",
            &[],
            self.sum(|shard| shard.write_batches.load()),
        );
        out.counter(
            "rusty_mcrouter_backend_batched_requests_total",
            &[],
            self.sum(|shard| shard.batched_requests.load()),
        );
        out.counter(
            "rusty_mcrouter_backend_queue_full_total",
            &[],
            self.sum(|shard| shard.queue_full.load()),
        );
        out.counter(
            "rusty_mcrouter_backend_bytes_read_total",
            &[],
            self.sum(|shard| shard.bytes_read.load()),
        );
        out.counter(
            "rusty_mcrouter_backend_bytes_written_total",
            &[],
            self.sum(|shard| shard.bytes_written.load()),
        );
        out.gauge(
            "rusty_mcrouter_backend_pending_reqs",
            &[],
            self.sum_gauge(|shard| shard.pending_reqs.load()),
        );
        out.gauge(
            "rusty_mcrouter_backend_inflight_reqs",
            &[],
            self.sum_gauge(|shard| shard.inflight_reqs.load()),
        );

        for kind in RequestKind::ALL {
            for code in ResultCode::ALL {
                out.counter(
                    "rusty_mcrouter_backend_requests_total",
                    &[
                        ("command", kind.meta_command()),
                        ("result", code.prometheus_label()),
                    ],
                    self.sum(|shard| shard.requests[kind as usize][code as usize].load()),
                );
            }
        }
    }
}

impl MetricsSource for TkoTrackerMap {
    fn encode(&self, out: &mut MetricsText) {
        let global = self.global_metrics();
        out.gauge(
            "rusty_mcrouter_tko",
            &[("kind", "soft")],
            global.soft_tkos.load(),
        );
        out.gauge(
            "rusty_mcrouter_tko",
            &[("kind", "hard")],
            global.hard_tkos.load(),
        );

        for gate in self.pool_snapshot() {
            let pool = &[("pool", &**gate.name())];
            out.gauge(
                "rusty_mcrouter_pool_fail_open",
                pool,
                gate.fail_open() as i64,
            );
            out.gauge(
                "rusty_mcrouter_pool_destinations_tko",
                pool,
                gate.num_destinations_tko() as i64,
            );
            out.counter(
                "rusty_mcrouter_fail_open_entered_total",
                pool,
                gate.fail_open_entered_total(),
            );
            out.counter(
                "rusty_mcrouter_fail_open_exited_total",
                pool,
                gate.fail_open_exited_total(),
            );
        }
    }
}

impl MetricsSource for DestinationMetricsRegistry {
    fn encode(&self, out: &mut MetricsText) {
        for block in self.snapshot() {
            let destination = block.destination();
            let dest = &[("destination", destination)];
            out.gauge(
                "rusty_mcrouter_destination_up",
                dest,
                !block.tracker.is_tko() as i64,
            );
            for code in ResultCode::ALL {
                out.counter(
                    "rusty_mcrouter_destination_requests_total",
                    &[
                        ("destination", destination),
                        ("result", code.prometheus_label()),
                    ],
                    block.requests[code as usize].load(),
                );
            }
            out.counter(
                "rusty_mcrouter_destination_latency_us_sum_total",
                dest,
                block.latency_us_sum.load(),
            );
            out.counter(
                "rusty_mcrouter_destination_connects_total",
                dest,
                block.connects.load(),
            );
            out.counter(
                "rusty_mcrouter_destination_idle_closes_total",
                dest,
                block.idle_closes.load(),
            );
            // per tko episode, reset on unmark - a gauge
            out.gauge(
                "rusty_mcrouter_destination_probes_sent",
                dest,
                block.probes_sent.load(),
            );
            out.gauge(
                "rusty_mcrouter_destination_inflight_reqs",
                dest,
                block.inflight_reqs.load(),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use rusty_mcrouter_observability_primitives::test_support::noop_sink;

    use super::*;
    use crate::tko::{DestTokenAllocator, FailOpenThresholds};

    #[test]
    fn backend_metrics_sum_real_shards() {
        let s1 = BackendMetricsShard::new();
        let s2 = BackendMetricsShard::new();
        s1.record_send(RequestKind::Get, ResultCode::Success, 100);
        s2.record_send(RequestKind::Get, ResultCode::Success, 250);
        s2.record_result(RequestKind::Store, ResultCode::Tko);
        s1.bytes_read.add(10);
        s2.bytes_read.add(20);
        s1.pending_reqs.add(3);
        s2.pending_reqs.sub(1);

        let text = BackendMetrics {
            shards: vec![s1, s2],
        }
        .render();
        assert!(text.contains(
            "rusty_mcrouter_backend_requests_total{command=\"mg\",result=\"success\"} 2\n"
        ));
        assert!(text
            .contains("rusty_mcrouter_backend_requests_total{command=\"ms\",result=\"tko\"} 1\n"));
        assert!(text.contains("rusty_mcrouter_backend_latency_us_sum_total 350\n"));
        assert!(text.contains("rusty_mcrouter_backend_bytes_read_total 30\n"));
        assert!(text.contains("rusty_mcrouter_backend_pending_reqs 2\n"));
        assert!(
            text.find("rusty_mcrouter_backend_inflight_reqs").unwrap()
                < text.find("rusty_mcrouter_backend_requests_total").unwrap()
        );
    }

    #[test]
    fn tko_metrics_reflect_marks_and_gates() {
        let allocator = DestTokenAllocator::new();
        let map = TkoTrackerMap::new(noop_sink());
        let tracker = map.tracker_for("10.0.0.1:11211", 3);
        assert!(tracker.record_hard_failure(allocator.allocate(), ResultCode::ConnectError));

        // soft mark on a second server for the kind="soft" gauge
        let soft = map.tracker_for("10.0.0.2:11211", 1);
        assert!(soft.record_soft_failure(allocator.allocate(), ResultCode::Timeout));

        // drive the gate directly into fail-open (enter=1: one slot
        // admitted, the next reservation flips the gate)
        let gate = map.pool_tracker_for("pool_a", FailOpenThresholds { enter: 1, exit: 1 });
        gate.inc_num_destinations_tko();
        gate.inc_num_destinations_tko();

        let text = map.render();
        assert!(text.contains("rusty_mcrouter_tko{kind=\"hard\"} 1\n"));
        assert!(text.contains("rusty_mcrouter_tko{kind=\"soft\"} 1\n"));
        assert!(text.contains("rusty_mcrouter_pool_fail_open{pool=\"pool_a\"} 1\n"));
        assert!(text.contains("rusty_mcrouter_fail_open_entered_total{pool=\"pool_a\"} 1\n"));
    }

    #[test]
    fn destination_metrics_walk_and_label_live_blocks() {
        let map = TkoTrackerMap::new(noop_sink());
        let registry = DestinationMetricsRegistry::new();
        let addr: Arc<str> = Arc::from("10.0.0.1:11211");
        let tracker = map.tracker_for(&addr, 3);
        let block = registry.get_or_create_metrics(&tracker);
        block.record_send(ResultCode::Success, 500);

        let text = registry.render();
        assert!(text.contains("rusty_mcrouter_destination_up{destination=\"10.0.0.1:11211\"} 1\n"));
        assert!(text.contains(
            "rusty_mcrouter_destination_requests_total{destination=\"10.0.0.1:11211\",result=\"success\"} 1\n"
        ));
        assert!(text.contains(
            "rusty_mcrouter_destination_latency_us_sum_total{destination=\"10.0.0.1:11211\"} 500\n"
        ));

        drop(block);
        let text = registry.render();
        assert!(
            !text.contains("10.0.0.1:11211"),
            "dead destinations must leave the scrape"
        );
    }

    #[test]
    fn request_kind_discriminants_fit_the_array() {
        for kind in RequestKind::ALL {
            assert!((kind as usize) < RequestKind::COUNT, "for {kind:?}");
        }
    }

    /// every (command, result) cell is independently addressable -
    /// no two records land in the same slot.
    #[test]
    fn every_command_result_cell_is_distinct() {
        let metrics = BackendMetricsShard::new();
        for kind in 0..RequestKind::COUNT {
            for code in 0..ResultCode::COUNT {
                let cell = &metrics.requests[kind][code];
                assert_eq!(cell.load(), 0);
                cell.inc();
            }
        }
        let total: u64 = metrics.requests.iter().flatten().map(Counter::load).sum();
        assert_eq!(total, (RequestKind::COUNT * ResultCode::COUNT) as u64);
    }

    #[test]
    fn record_send_accumulates_latency() {
        let metrics = BackendMetricsShard::new();
        metrics.record_send(RequestKind::Get, ResultCode::Success, 150);
        metrics.record_send(RequestKind::Get, ResultCode::Success, 250);
        metrics.record_send(RequestKind::Get, ResultCode::Timeout, 1000);
        assert_eq!(metrics.latency_us_sum.load(), 1400);
        assert_eq!(
            metrics.requests[RequestKind::Get as usize][ResultCode::Success as usize].load(),
            2
        );
    }

    /// gauges go both ways and settle back to zero after a drain.
    #[test]
    fn gauges_return_to_zero() {
        let metrics = BackendMetricsShard::new();
        metrics.pending_reqs.add(3);
        metrics.pending_reqs.sub(3);
        metrics.inflight_reqs.add(2);
        metrics.inflight_reqs.sub(2);
        assert_eq!(metrics.pending_reqs.load(), 0);
        assert_eq!(metrics.inflight_reqs.load(), 0);
    }

    /// shards must not share cache lines across threads.
    #[test]
    fn shard_is_cache_line_aligned() {
        assert!(std::mem::align_of::<BackendMetricsShard>() >= 64);
    }
}
