// the metric sources: one per fact-owner. shard-sum scalars go through
// the shard_source! macro; matrices, walks and direct reads are hand
// written (unique shapes, one instance each).

use std::collections::BTreeMap;
use std::sync::Arc;

use rusty_mcrouter_backend::destination::DestinationMetricsRegistry;
use rusty_mcrouter_backend::tko::TkoTrackerMap;
use rusty_mcrouter_backend::BackendMetrics;
use rusty_mcrouter_core::{
    FailoverErrorClass, FailoverPolicyKind, PoolMetrics, RoutingMetricsShard,
};
use rusty_mcrouter_frontend::FrontendMetrics;
use rusty_mcrouter_observability_primitives::prometheus::{
    MetricsRegistry, MetricsSource, MetricsText,
};
use rusty_mcrouter_worker::WorkerShards;

use crate::metrics::ObservabilityMetrics;

#[derive(Clone, Copy, Debug)]
pub struct ProcessMetadata {
    pub start_unix_secs: u64,
    pub num_proxies: usize,
}

pub struct ScrapeInputs {
    pub metadata: ProcessMetadata,
    pub workers: Vec<WorkerShards>,
    pub tko_map: Arc<TkoTrackerMap>,
    pub destinations: Arc<DestinationMetricsRegistry>,
    pub observability: Arc<ObservabilityMetrics>,
    pub additional_sources: Vec<Arc<dyn MetricsSource>>,
}

impl ScrapeInputs {
    pub fn into_registry(self) -> MetricsRegistry {
        let backend: Vec<_> = self
            .workers
            .iter()
            .map(|p| Arc::clone(&p.backend))
            .collect();
        let frontend: Vec<_> = self
            .workers
            .iter()
            .map(|p| Arc::clone(&p.frontend))
            .collect();
        let routing: Vec<_> = self
            .workers
            .iter()
            .map(|p| Arc::clone(&p.routing))
            .collect();

        let mut registry = MetricsRegistry::new();
        registry.register(Arc::new(BackendMetrics { shards: backend }));
        registry.register(Arc::new(FrontendMetrics { shards: frontend }));
        registry.register(Arc::new(RoutingSource { shards: routing }));
        registry.register(self.tko_map);
        registry.register(self.destinations);
        registry.register(Arc::new(SelfSource {
            metrics: self.observability,
            num_proxies: self.metadata.num_proxies,
            start_unix_secs: self.metadata.start_unix_secs,
        }));
        for source in self.additional_sources {
            registry.register(source);
        }
        registry
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

        let mut pools = BTreeMap::<Arc<str>, PoolTotals>::new();
        for shard in &self.shards {
            for (name, block) in shard.pool_blocks() {
                pools.entry(name).or_default().add(&block);
            }
        }

        for (name, totals) in &pools {
            let labels = &[("pool", &**name)];

            out.counter(
                "rusty_mcrouter_pool_requests_total",
                labels,
                totals.requests,
            );
            out.counter(
                "rusty_mcrouter_pool_duration_us_sum_total",
                labels,
                totals.duration_us_sum,
            );
            out.counter(
                "rusty_mcrouter_pool_completed_requests_total",
                labels,
                totals.completed_requests,
            );
            out.counter(
                "rusty_mcrouter_pool_requests_failed_total",
                labels,
                totals.final_errors,
            );
            out.counter(
                "rusty_mcrouter_pool_total_duration_us_sum_total",
                labels,
                totals.total_duration_us_sum,
            );
        }
    }
}

#[derive(Default)]
struct PoolTotals {
    requests: u64,
    duration_us_sum: u64,
    completed_requests: u64,
    final_errors: u64,
    total_duration_us_sum: u64,
}

impl PoolTotals {
    fn add(&mut self, block: &PoolMetrics) {
        self.requests += block.requests.load();
        self.duration_us_sum += block.duration_us_sum.load();
        self.completed_requests += block.completed_requests.load();
        self.final_errors += block.final_errors.load();
        self.total_duration_us_sum += block.total_duration_us_sum.load();
    }
}

pub struct SelfSource {
    pub metrics: Arc<ObservabilityMetrics>,
    pub num_proxies: usize,
    /// computed once at startup - no clock reads at scrape time
    pub start_unix_secs: u64,
}

impl MetricsSource for SelfSource {
    fn encode(&self, out: &mut MetricsText) {
        out.counter(
            "rusty_mcrouter_events_dropped_total",
            &[],
            self.metrics.events_dropped.load(),
        );
        out.counter(
            "rusty_mcrouter_metrics_http_rejected_total",
            &[],
            self.metrics.http_rejected.load(),
        );
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
    use rusty_mcrouter_backend::classify::ResultCode;
    use rusty_mcrouter_config::{parse, ConfigDocument};
    use rusty_mcrouter_observability_primitives::test_support::noop_sink;
    use rusty_mcrouter_protocol::RequestKind;

    use super::*;
    use rusty_mcrouter_observability_primitives::prometheus::MetricsRegistry;

    fn render(source: impl MetricsSource + 'static) -> String {
        let mut registry = MetricsRegistry::new();
        registry.register(Arc::new(source));
        registry.render()
    }

    struct InjectedSource;

    impl MetricsSource for InjectedSource {
        fn encode(&self, out: &mut MetricsText) {
            out.gauge("injected_metric", &[], 1);
        }
    }

    fn pools_config(names: &[&str]) -> ConfigDocument {
        let pools = names
            .iter()
            .map(|name| {
                (
                    (*name).to_string(),
                    serde_json::json!({ "servers": [format!("{name}:1")] }),
                )
            })
            .collect::<serde_json::Map<_, _>>();
        parse(&serde_json::json!({ "pools": pools, "route": "NullRoute" }).to_string()).unwrap()
    }

    #[test]
    fn routing_source_sums_shards_and_pool_metrics() {
        let config = pools_config(&["primary", "backup"]);
        let primary = config.pool_id("primary").unwrap();
        let s1 = RoutingMetricsShard::new();
        let s2 = RoutingMetricsShard::new();
        let (t1, t2) = (s1.table_for(&config), s2.table_for(&config));

        s1.dev_null_requests.add(2);
        s2.dev_null_requests.inc();
        s1.failover[FailoverPolicyKind::InOrder as usize].inc();
        s2.failover_exhausted[FailoverPolicyKind::InOrder as usize].inc();
        s2.failover_policy_errors[FailoverErrorClass::Tko as usize].add(3);
        t1[primary].requests.add(4);
        t2[primary].requests.add(5);
        t2[primary].final_errors.inc();

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
        let shard = RoutingMetricsShard::new();
        let _table = shard.table_for(&pools_config(&["quoted\"pool\\line\nnext"]));

        let text = render(RoutingSource {
            shards: vec![shard],
        });

        assert!(text.contains(
            "rusty_mcrouter_pool_requests_total{pool=\"quoted\\\"pool\\\\line\\nnext\"} 0\n"
        ));
    }

    #[test]
    fn routing_source_joins_shards_on_different_generations_by_name() {
        let old = pools_config(&["users"]);
        let new = pools_config(&["aaa", "users"]);
        let s0 = RoutingMetricsShard::new();
        let s1 = RoutingMetricsShard::new();
        let t0 = s0.table_for(&new);
        let t1 = s1.table_for(&old);
        t0[new.pool_id("users").unwrap()].requests.add(2);
        t1[old.pool_id("users").unwrap()].requests.add(3);

        let text = render(RoutingSource {
            shards: vec![s0, s1],
        });

        assert!(text.contains("rusty_mcrouter_pool_requests_total{pool=\"users\"} 5\n"));
        assert!(text.contains("rusty_mcrouter_pool_requests_total{pool=\"aaa\"} 0\n"));
    }

    #[test]
    fn scrape_inputs_assemble_every_source_in_order() {
        let workers = vec![WorkerShards::new(), WorkerShards::new()];
        // a live generation on worker 0 is what makes its pools scrapeable
        let _generation = workers[0].routing.table_for(&pools_config(&["pool_a"]));
        workers[0]
            .backend
            .record_result(RequestKind::Get, ResultCode::Success);
        workers[1].frontend.failed.inc();
        workers[1].routing.dev_null_requests.inc();
        let observability = Arc::new(ObservabilityMetrics::default());
        observability.events_dropped.inc();

        let text = ScrapeInputs {
            metadata: ProcessMetadata {
                start_unix_secs: 1_700_000_000,
                num_proxies: 2,
            },
            workers,
            tko_map: TkoTrackerMap::new(noop_sink()),
            destinations: DestinationMetricsRegistry::new(),
            observability,
            additional_sources: vec![Arc::new(InjectedSource)],
        }
        .into_registry()
        .render();

        let position = |needle: &str| text.find(needle).unwrap_or_else(|| panic!("{needle}"));
        let order = [
            position("rusty_mcrouter_backend_latency_us_sum_total 0\n"),
            position(
                "rusty_mcrouter_backend_requests_total{command=\"mg\",result=\"success\"} 1\n",
            ),
            position("rusty_mcrouter_noops_total 0\n"),
            position("rusty_mcrouter_requests_total{command=\"mg\"} 0\n"),
            position("rusty_mcrouter_dev_null_requests_total 1\n"),
            position("rusty_mcrouter_tko{kind=\"soft\"} 0\n"),
            position("rusty_mcrouter_events_dropped_total 1\n"),
            position("rusty_mcrouter_proxies 2\n"),
            position("injected_metric 1\n"),
        ];
        assert!(order.windows(2).all(|w| w[0] < w[1]), "{text}");
        assert!(text.contains("rusty_mcrouter_requests_failed_total 1\n"));
        assert!(text.contains("rusty_mcrouter_pool_requests_total{pool=\"pool_a\"} 0\n"));
    }

    #[test]
    fn self_source_golden() {
        let metrics = Arc::new(ObservabilityMetrics::default());
        metrics.events_dropped.add(2);
        let text = render(SelfSource {
            metrics,
            num_proxies: 4,
            start_unix_secs: 1_700_000_000,
        });
        assert_eq!(
            text,
            "rusty_mcrouter_events_dropped_total 2\n\
             rusty_mcrouter_metrics_http_rejected_total 0\n\
             rusty_mcrouter_proxies 4\n\
             rusty_mcrouter_start_time_seconds 1700000000\n"
        );
    }
}
