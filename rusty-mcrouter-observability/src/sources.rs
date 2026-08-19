// the metric sources: one per fact-owner. shard-sum scalars go through
// the shard_source! macro; matrices, walks and direct reads are hand
// written (unique shapes, one instance each).

use std::sync::Arc;

use rusty_mcrouter_backend::classify::ResultCode;
use rusty_mcrouter_backend::destination::DestinationMetricsRegistry;
use rusty_mcrouter_backend::metrics::{BackendMetricsShard, CommandKind};
use rusty_mcrouter_backend::tko::TkoTrackerMap;
use rusty_mcrouter_core::{FailoverErrorClass, FailoverPolicyKind, RoutingMetricsShard};
use rusty_mcrouter_observability_primitives::Counter;
use rusty_mcrouter_proxy::FrontendMetricsShard;

use crate::metrics::{MetricsSource, MetricsText};
use crate::shard_source;

shard_source! {
    /// Backend metric shards -> the rusty_mcrouter_backend_* scalar families.
    /// the {command, result} matrix is BackendRequestsSource.
    pub struct BackendScalarsSource(BackendMetricsShard) {
        counter latency_us_sum              => "rusty_mcrouter_backend_latency_us_sum_total";
        counter connections_opened          => "rusty_mcrouter_backend_connections_opened_total";
        counter connections_closed          => "rusty_mcrouter_backend_connections_closed_total";
        counter connect_retries             => "rusty_mcrouter_backend_connect_retries_total";
        counter connect_success_after_retry => "rusty_mcrouter_backend_connect_retry_successes_total";
        counter write_batches               => "rusty_mcrouter_backend_write_batches_total";
        counter batched_requests            => "rusty_mcrouter_backend_batched_requests_total";
        counter queue_full                  => "rusty_mcrouter_backend_queue_full_total";
        counter bytes_read                  => "rusty_mcrouter_backend_bytes_read_total";
        counter bytes_written               => "rusty_mcrouter_backend_bytes_written_total";
        gauge   pending_reqs                => "rusty_mcrouter_backend_pending_reqs";
        gauge   inflight_reqs               => "rusty_mcrouter_backend_inflight_reqs";
    }
}

shard_source! {
    /// Frontend metric shards -> the client-facing families. the
    /// per-command matrix is FrontendRequestsSource.
    pub struct FrontendScalarsSource(FrontendMetricsShard) {
        counter noops              => "rusty_mcrouter_noops_total";
        counter parse_errors       => "rusty_mcrouter_parse_errors_total";
        counter failed             => "rusty_mcrouter_requests_failed_total";
        gauge   client_connections => "rusty_mcrouter_client_connections";
        gauge   processing         => "rusty_mcrouter_requests_processing";
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
                    "rusty_mcrouter_backend_requests_total",
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
                "rusty_mcrouter_requests_total",
                &[("command", cmd.prometheus_label())],
                total,
            );
        }
    }
}

pub struct RoutingSource {
    pub shards: Vec<Arc<RoutingMetricsShard>>,
}

impl RoutingSource {
    fn sum(&self, load: impl Fn(&RoutingMetricsShard) -> u64) -> u64 {
        self.shards.iter().map(|shard| load(shard)).sum()
    }
}

impl MetricsSource for RoutingSource {
    fn encode(&self, out: &mut MetricsText) {
        out.counter(
            "rusty_mcrouter_dev_null_requests_total",
            &[],
            self.sum(|shard| shard.dev_null_requests.load()),
        );

        for policy in FailoverPolicyKind::ALL {
            let labels = &[("policy", policy.prometheus_label())];
            out.counter(
                "rusty_mcrouter_failover_total",
                labels,
                self.sum(|shard| shard.failover[policy as usize].load()),
            );
            out.counter(
                "rusty_mcrouter_failover_exhausted_total",
                labels,
                self.sum(|shard| shard.failover_exhausted[policy as usize].load()),
            );
        }

        for class in FailoverErrorClass::ALL {
            out.counter(
                "rusty_mcrouter_failover_policy_errors_total",
                &[("class", class.prometheus_label())],
                self.sum(|shard| shard.failover_policy_errors[class as usize].load()),
            );
        }

        let Some(first) = self.shards.first() else {
            return;
        };

        debug_assert!(self
            .shards
            .iter()
            .all(|shard| Arc::ptr_eq(first.layout(), shard.layout())));

        for index in 0..first.layout().pools_len() {
            let pool = first
                .layout()
                .pool_name(index)
                .expect("index came from layout length");
            let labels = &[("pool", pool)];

            out.counter(
                "rusty_mcrouter_pool_requests_total",
                labels,
                self.sum(|shard| shard.pools[index].requests.load()),
            );
            out.counter(
                "rusty_mcrouter_pool_duration_us_sum_total",
                labels,
                self.sum(|shard| shard.pools[index].duration_us_sum.load()),
            );
            out.counter(
                "rusty_mcrouter_pool_completed_requests_total",
                labels,
                self.sum(|shard| shard.pools[index].completed_requests.load()),
            );
            out.counter(
                "rusty_mcrouter_pool_requests_failed_total",
                labels,
                self.sum(|shard| shard.pools[index].final_errors.load()),
            );
            out.counter(
                "rusty_mcrouter_pool_total_duration_us_sum_total",
                labels,
                self.sum(|shard| shard.pools[index].total_duration_us_sum.load()),
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
        out.gauge(
            "rusty_mcrouter_suspect_servers",
            &[],
            self.map.sus_servers().len() as i64,
        );

        for gate in self.map.pool_snapshot() {
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

pub struct DestinationSource {
    pub registry: Arc<DestinationMetricsRegistry>,
}

impl MetricsSource for DestinationSource {
    fn encode(&self, out: &mut MetricsText) {
        for block in self.registry.snapshot() {
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

pub struct SelfSource {
    pub dropped: Arc<Counter>,
    pub num_proxies: usize,
    /// computed once at startup - no clock reads at scrape time
    pub start_unix_secs: u64,
}

impl MetricsSource for SelfSource {
    fn encode(&self, out: &mut MetricsText) {
        out.counter(
            "rusty_mcrouter_events_dropped_total",
            &[],
            self.dropped.load(),
        );
        out.gauge("rusty_mcrouter_proxies", &[], self.num_proxies as i64);
        out.gauge(
            "rusty_mcrouter_start_time_seconds",
            &[],
            self.start_unix_secs as i64,
        );
        out.counter(
            "rusty_mcrouter_build_info",
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
        assert!(text.contains(
            "rusty_mcrouter_backend_requests_total{command=\"mg\",result=\"success\"} 2\n"
        ));
        assert!(text
            .contains("rusty_mcrouter_backend_requests_total{command=\"ms\",result=\"tko\"} 1\n"));

        let text = render(BackendScalarsSource {
            shards: vec![s1, s2],
        });
        assert!(text.contains("rusty_mcrouter_backend_latency_us_sum_total 350\n"));
    }

    #[test]
    fn frontend_sources_render() {
        let shard = FrontendMetricsShard::new();
        shard.requests[CommandKind::Get as usize].add(3);
        shard.failed.inc();

        let text = render(FrontendRequestsSource {
            shards: vec![Arc::clone(&shard)],
        });
        assert!(text.contains("rusty_mcrouter_requests_total{command=\"mg\"} 3\n"));

        let text = render(FrontendScalarsSource {
            shards: vec![shard],
        });
        assert!(text.contains("rusty_mcrouter_requests_failed_total 1\n"));
    }

    #[test]
    fn routing_source_sums_shards_and_pool_metrics() {
        let layout = rusty_mcrouter_core::RoutingMetricsLayout::new([
            "primary".to_string(),
            "backup".to_string(),
        ]);
        let s1 = RoutingMetricsShard::new(Arc::clone(&layout));
        let s2 = RoutingMetricsShard::new(layout);

        s1.dev_null_requests.add(2);
        s2.dev_null_requests.inc();
        s1.failover[FailoverPolicyKind::InOrder as usize].inc();
        s2.failover_exhausted[FailoverPolicyKind::InOrder as usize].inc();
        s2.failover_policy_errors[FailoverErrorClass::Tko as usize].add(3);
        s1.pools[0].requests.add(4);
        s2.pools[0].requests.add(5);
        s2.pools[0].final_errors.inc();

        let text = render(RoutingSource {
            shards: vec![s1, s2],
        });

        assert!(text.contains("rusty_mcrouter_dev_null_requests_total 3\n"));
        assert!(text.contains("rusty_mcrouter_failover_total{policy=\"inorder\"} 1\n"));
        assert!(text.contains("rusty_mcrouter_failover_exhausted_total{policy=\"inorder\"} 1\n"));
        assert!(text.contains("rusty_mcrouter_failover_policy_errors_total{class=\"tko\"} 3\n"));
        assert!(text.contains("rusty_mcrouter_pool_requests_total{pool=\"primary\"} 9\n"));
        assert!(text.contains("rusty_mcrouter_pool_requests_failed_total{pool=\"primary\"} 1\n"));
        assert!(text.contains("rusty_mcrouter_pool_requests_total{pool=\"backup\"} 0\n"));
    }

    #[test]
    fn routing_source_escapes_configured_pool_names() {
        let layout =
            rusty_mcrouter_core::RoutingMetricsLayout::new(
                ["quoted\"pool\\line\nnext".to_string()],
            );
        let shard = RoutingMetricsShard::new(layout);

        let text = render(RoutingSource {
            shards: vec![shard],
        });

        assert!(text.contains(
            "rusty_mcrouter_pool_requests_total{pool=\"quoted\\\"pool\\\\line\\nnext\"} 0\n"
        ));
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
        assert!(text.contains("rusty_mcrouter_tko{kind=\"hard\"} 1\n"));
        assert!(text.contains("rusty_mcrouter_tko{kind=\"soft\"} 1\n"));
        assert!(text.contains("rusty_mcrouter_pool_fail_open{pool=\"pool_a\"} 1\n"));
        assert!(text.contains("rusty_mcrouter_fail_open_entered_total{pool=\"pool_a\"} 1\n"));
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
        assert!(text.contains("rusty_mcrouter_destination_up{destination=\"10.0.0.1:11211\"} 1\n"));
        assert!(text.contains(
            "rusty_mcrouter_destination_requests_total{destination=\"10.0.0.1:11211\",result=\"success\"} 1\n"
        ));
        assert!(text.contains(
            "rusty_mcrouter_destination_latency_us_sum_total{destination=\"10.0.0.1:11211\"} 500\n"
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
                "rusty_mcrouter_events_dropped_total 2\n\
                 rusty_mcrouter_proxies 4\n\
                 rusty_mcrouter_start_time_seconds 1700000000\n\
                 rusty_mcrouter_build_info{{version=\"{}\"}} 1\n",
                env!("CARGO_PKG_VERSION")
            )
        );
    }
}
