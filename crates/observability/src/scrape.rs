//! Scrape assembly.

use std::sync::Arc;

use rusty_mcrouter_backend::destination::DestinationMetricsRegistry;
use rusty_mcrouter_backend::tko::TkoTrackerMap;
use rusty_mcrouter_backend::BackendMetrics;
use rusty_mcrouter_core::RoutingMetrics;
use rusty_mcrouter_frontend::FrontendMetrics;
use rusty_mcrouter_observability_primitives::prometheus::{MetricsRegistry, MetricsSource};
use rusty_mcrouter_worker::WorkerShards;

use crate::metrics::{ObservabilityMetrics, ProcessMetadata};

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
        registry.register(Arc::new(RoutingMetrics { shards: routing }));
        registry.register(self.tko_map);
        registry.register(self.destinations);
        registry.register(self.observability);
        registry.register(Arc::new(self.metadata));
        for source in self.additional_sources {
            registry.register(source);
        }
        registry
    }
}

#[cfg(test)]
mod tests {
    use rusty_mcrouter_backend::classify::ResultCode;
    use rusty_mcrouter_config::{parse, ConfigDocument};
    use rusty_mcrouter_observability_primitives::test_support::noop_sink;
    use rusty_mcrouter_protocol::RequestKind;

    use super::*;
    use rusty_mcrouter_observability_primitives::prometheus::MetricsText;

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
        observability.http_rejected.add(3);
        let tko_map = TkoTrackerMap::new(noop_sink());
        let destinations = DestinationMetricsRegistry::new();
        let tracker = tko_map.tracker_for("10.0.0.1:11211", 3);
        let _destination = destinations.metrics_for(&tracker);

        let text = ScrapeInputs {
            metadata: ProcessMetadata {
                start_unix_secs: 1_700_000_000,
                num_proxies: 2,
            },
            workers,
            tko_map,
            destinations,
            observability,
            additional_sources: vec![Arc::new(InjectedSource)],
        }
        .into_registry()
        .render();

        let position = |needle: &str| text.find(needle).unwrap_or_else(|| panic!("{needle}"));
        let order = [
            position("rusty_mcrouter_backend_latency_us_sum_total 0\n"),
            position("rusty_mcrouter_backend_inflight_reqs 0\n"),
            position(
                "rusty_mcrouter_backend_requests_total{command=\"mg\",result=\"success\"} 1\n",
            ),
            position("rusty_mcrouter_noops_total 0\n"),
            position("rusty_mcrouter_requests_processing 0\n"),
            position("rusty_mcrouter_requests_total{command=\"mg\"} 0\n"),
            position("rusty_mcrouter_dev_null_requests_total 1\n"),
            position("rusty_mcrouter_tko{kind=\"soft\"} 0\n"),
            position("rusty_mcrouter_destination_up{destination=\"10.0.0.1:11211\"} 1\n"),
            position("rusty_mcrouter_events_dropped_total 1\n"),
            position("rusty_mcrouter_metrics_http_rejected_total 3\n"),
            position("rusty_mcrouter_proxies 2\n"),
            position("rusty_mcrouter_start_time_seconds 1700000000\n"),
            position("injected_metric 1\n"),
        ];
        assert!(order.windows(2).all(|w| w[0] < w[1]), "{text}");
        assert!(text.contains("rusty_mcrouter_requests_failed_total 1\n"));
        assert!(text.contains("rusty_mcrouter_pool_requests_total{pool=\"pool_a\"} 0\n"));
    }
}
