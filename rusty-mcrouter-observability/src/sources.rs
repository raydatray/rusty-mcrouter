// the metric sources: one per fact-owner. shard-sum scalars go through
// the shard_source! macro; matrices, walks and direct reads are hand
// written (unique shapes, one instance each).

use std::sync::Arc;

use rusty_mcrouter_backend::classify::ResultCode;
use rusty_mcrouter_backend::destination::DestinationMetricsRegistry;
use rusty_mcrouter_backend::metrics::{BackendMetricsShard, CommandKind};
use rusty_mcrouter_backend::tko::TkoTrackerMap;
use rusty_mcrouter_observability_primitives::Counter;
use rusty_mcrouter_proxy::FrontendMetricsShard;

use crate::metrics::{MetricsSource, MetricsText};
use crate::shard_source;

shard_source! {
    /// Backend metric shards -> the mcrouter_backend_* scalar families.
    /// the {command, result} matrix is BackendRequestsSource.
    pub struct BackendScalarsSource(BackendMetricsShard) {
        counter latency_us_sum              => "mcrouter_backend_latency_us_sum_total";
        counter connections_opened          => "mcrouter_backend_connections_opened_total";
        counter connections_closed          => "mcrouter_backend_connections_closed_total";
        counter connect_retries             => "mcrouter_backend_connect_retries_total";
        counter connect_success_after_retry => "mcrouter_backend_connect_retry_successes_total";
        counter write_batches               => "mcrouter_backend_write_batches_total";
        counter batched_requests            => "mcrouter_backend_batched_requests_total";
        counter queue_full                  => "mcrouter_backend_queue_full_total";
        counter bytes_read                  => "mcrouter_backend_bytes_read_total";
        counter bytes_written               => "mcrouter_backend_bytes_written_total";
        gauge   pending_reqs                => "mcrouter_backend_pending_reqs";
        gauge   inflight_reqs               => "mcrouter_backend_inflight_reqs";
    }
}

shard_source! {
    /// Frontend metric shards -> the client-facing families. the
    /// per-command matrix is FrontendRequestsSource.
    pub struct FrontendScalarsSource(FrontendMetricsShard) {
        counter noops              => "mcrouter_noops_total";
        counter parse_errors       => "mcrouter_parse_errors_total";
        counter failed             => "mcrouter_requests_failed_total";
        gauge   client_connections => "mcrouter_client_connections";
        gauge   processing         => "mcrouter_requests_processing";
    }
}

pub struct BackendRequestsSource {
    pub shards: Vec<Arc<BackendMetricsShard>>,
}

impl MetricsSource for BackendRequestsSource {
    fn encode(&self, out: &mut MetricsText) {
        for cmd in CommandKind::ALL {
            for code in ResultCode::ALL {
                let total: u64 = self
                    .shards
                    .iter()
                    .map(|s| s.requests[cmd as usize][code as usize].load())
                    .sum();
                out.counter(
                    "mcrouter_backend_requests_total",
                    &[
                        ("command", cmd.prometheus_label()),
                        ("result", code.prometheus_label()),
                    ],
                    total,
                );
            }
        }
    }
}

pub struct FrontendRequestsSource {
    pub shards: Vec<Arc<FrontendMetricsShard>>,
}

impl MetricsSource for FrontendRequestsSource {
    fn encode(&self, out: &mut MetricsText) {
        for cmd in CommandKind::ALL {
            let total: u64 = self
                .shards
                .iter()
                .map(|s| s.requests[cmd as usize].load())
                .sum();
            out.counter(
                "mcrouter_requests_total",
                &[("command", cmd.prometheus_label())],
                total,
            );
        }
    }
}

pub struct TkoSource {
    pub map: Arc<TkoTrackerMap>,
}

impl MetricsSource for TkoSource {
    fn encode(&self, out: &mut MetricsText) {
        let global = self.map.global_metrics();
        out.gauge("mcrouter_tko", &[("kind", "soft")], global.soft_tkos.load());
        out.gauge("mcrouter_tko", &[("kind", "hard")], global.hard_tkos.load());
        out.gauge(
            "mcrouter_suspect_servers",
            &[],
            self.map.sus_servers().len() as i64,
        );

        for gate in self.map.pool_snapshot() {
            let pool = &[("pool", &**gate.name())];
            out.gauge("mcrouter_pool_fail_open", pool, gate.fail_open() as i64);
            out.gauge(
                "mcrouter_pool_destinations_tko",
                pool,
                gate.num_destinations_tko() as i64,
            );
            out.counter(
                "mcrouter_fail_open_entered_total",
                pool,
                gate.fail_open_entered_total(),
            );
            out.counter(
                "mcrouter_fail_open_exited_total",
                pool,
                gate.fail_open_exited_total(),
            );
        }
    }
}

pub struct DestinationSource {
    pub registry: Arc<DestinationMetricsRegistry>,
}

impl MetricsSource for DestinationSource {
    fn encode(&self, out: &mut MetricsText) {
        for block in self.registry.snapshot() {
            let destination = block.destination();
            let dest = &[("destination", destination)];
            out.gauge(
                "mcrouter_destination_up",
                dest,
                !block.tracker.is_tko() as i64,
            );
            for code in ResultCode::ALL {
                out.counter(
                    "mcrouter_destination_requests_total",
                    &[
                        ("destination", destination),
                        ("result", code.prometheus_label()),
                    ],
                    block.requests[code as usize].load(),
                );
            }
            out.counter(
                "mcrouter_destination_latency_us_sum_total",
                dest,
                block.latency_us_sum.load(),
            );
            out.counter(
                "mcrouter_destination_connects_total",
                dest,
                block.connects.load(),
            );
            out.counter(
                "mcrouter_destination_idle_closes_total",
                dest,
                block.idle_closes.load(),
            );
            // per tko episode, reset on unmark - a gauge
            out.gauge(
                "mcrouter_destination_probes_sent",
                dest,
                block.probes_sent.load(),
            );
            out.gauge(
                "mcrouter_destination_inflight_reqs",
                dest,
                block.inflight_reqs.load(),
            );
        }
    }
}

pub struct SelfSource {
    pub dropped: Arc<Counter>,
    pub num_proxies: usize,
    /// computed once at startup - no clock reads at scrape time
    pub start_unix_secs: u64,
}

impl MetricsSource for SelfSource {
    fn encode(&self, out: &mut MetricsText) {
        out.counter("mcrouter_events_dropped_total", &[], self.dropped.load());
        out.gauge("mcrouter_proxies", &[], self.num_proxies as i64);
        out.gauge(
            "mcrouter_start_time_seconds",
            &[],
            self.start_unix_secs as i64,
        );
        out.counter(
            "mcrouter_build_info",
            &[("version", env!("CARGO_PKG_VERSION"))],
            1,
        );
    }
}

#[cfg(test)]
mod tests {
    use rusty_mcrouter_backend::tko::{DestToken, FailOpenThresholds, TkoEventSink};

    use super::*;
    use crate::metrics::MetricsRegistry;

    fn render(source: impl MetricsSource + 'static) -> String {
        let mut registry = MetricsRegistry::new();
        registry.register(Box::new(source));
        registry.render()
    }

    #[test]
    fn backend_sources_sum_real_shards() {
        let s1 = BackendMetricsShard::new();
        let s2 = BackendMetricsShard::new();
        s1.record_send(CommandKind::Get, ResultCode::Success, 100);
        s2.record_send(CommandKind::Get, ResultCode::Success, 250);
        s2.record_result(CommandKind::Store, ResultCode::Tko);

        let text = render(BackendRequestsSource {
            shards: vec![Arc::clone(&s1), Arc::clone(&s2)],
        });
        assert!(
            text.contains("mcrouter_backend_requests_total{command=\"mg\",result=\"success\"} 2\n")
        );
        assert!(text.contains("mcrouter_backend_requests_total{command=\"ms\",result=\"tko\"} 1\n"));

        let text = render(BackendScalarsSource {
            shards: vec![s1, s2],
        });
        assert!(text.contains("mcrouter_backend_latency_us_sum_total 350\n"));
    }

    #[test]
    fn frontend_sources_render() {
        let shard = FrontendMetricsShard::new();
        shard.requests[CommandKind::Get as usize].add(3);
        shard.failed.inc();

        let text = render(FrontendRequestsSource {
            shards: vec![Arc::clone(&shard)],
        });
        assert!(text.contains("mcrouter_requests_total{command=\"mg\"} 3\n"));

        let text = render(FrontendScalarsSource {
            shards: vec![shard],
        });
        assert!(text.contains("mcrouter_requests_failed_total 1\n"));
    }

    #[test]
    fn tko_source_reflects_marks_and_gates() {
        let map = TkoTrackerMap::with_sink(TkoEventSink::new(|_| {}));
        let tracker = map.tracker_for("10.0.0.1:11211", 3);
        assert!(tracker.record_hard_failure(DestToken::allocate(), ResultCode::ConnectError));

        // soft mark on a second server for the kind="soft" gauge
        let soft = map.tracker_for("10.0.0.2:11211", 1);
        assert!(soft.record_soft_failure(DestToken::allocate(), ResultCode::Timeout));

        // drive the gate directly into fail-open (enter=1: one slot
        // admitted, the next reservation flips the gate)
        let gate = map.pool_tracker_for("pool_a", FailOpenThresholds { enter: 1, exit: 1 });
        gate.inc_num_destinations_tko();
        gate.inc_num_destinations_tko();

        let text = render(TkoSource {
            map: Arc::clone(&map),
        });
        assert!(text.contains("mcrouter_tko{kind=\"hard\"} 1\n"));
        assert!(text.contains("mcrouter_tko{kind=\"soft\"} 1\n"));
        assert!(text.contains("mcrouter_pool_fail_open{pool=\"pool_a\"} 1\n"));
        assert!(text.contains("mcrouter_fail_open_entered_total{pool=\"pool_a\"} 1\n"));
    }

    #[test]
    fn destination_source_walks_and_labels() {
        let map = TkoTrackerMap::with_sink(TkoEventSink::new(|_| {}));
        let registry = DestinationMetricsRegistry::new();
        let addr: Arc<str> = Arc::from("10.0.0.1:11211");
        let tracker = map.tracker_for(&addr, 3);
        let block = registry.metrics_for(&tracker);
        block.record_send(ResultCode::Success, 500);

        let text = render(DestinationSource {
            registry: Arc::clone(&registry),
        });
        assert!(text.contains("mcrouter_destination_up{destination=\"10.0.0.1:11211\"} 1\n"));
        assert!(text.contains(
            "mcrouter_destination_requests_total{destination=\"10.0.0.1:11211\",result=\"success\"} 1\n"
        ));
        assert!(text.contains(
            "mcrouter_destination_latency_us_sum_total{destination=\"10.0.0.1:11211\"} 500\n"
        ));

        drop(block);
        let text = render(DestinationSource { registry });
        assert!(
            !text.contains("10.0.0.1:11211"),
            "dead destinations must leave the scrape"
        );
    }

    #[test]
    fn self_source_golden() {
        let text = render(SelfSource {
            dropped: {
                let dropped = Arc::new(Counter::default());
                dropped.add(2);
                dropped
            },
            num_proxies: 4,
            start_unix_secs: 1_700_000_000,
        });
        assert_eq!(
            text,
            format!(
                "mcrouter_events_dropped_total 2\n\
                 mcrouter_proxies 4\n\
                 mcrouter_start_time_seconds 1700000000\n\
                 mcrouter_build_info{{version=\"{}\"}} 1\n",
                env!("CARGO_PKG_VERSION")
            )
        );
    }
}
