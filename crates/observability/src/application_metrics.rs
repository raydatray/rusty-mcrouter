use std::sync::Arc;

use rusty_mcrouter_backend::destination::DestinationMetricsRegistry;
use rusty_mcrouter_backend::tko::TkoTrackerMap;
use rusty_mcrouter_backend::BackendMetrics;
use rusty_mcrouter_config::ConfigMetrics;
use rusty_mcrouter_core::RoutingMetrics;
use rusty_mcrouter_frontend::FrontendMetrics;
use rusty_mcrouter_observability_primitives::prometheus::{MetricsSource, MetricsText};
use rusty_mcrouter_worker::WorkerShards;

use crate::{ObservabilityMetrics, ProcessMetadata};

pub struct ApplicationMetricsSetup {
    pub metadata: ProcessMetadata,
    pub workers: Vec<WorkerShards>,
    pub tko_map: Arc<TkoTrackerMap>,
    pub destinations: Arc<DestinationMetricsRegistry>,
    pub observability: Arc<ObservabilityMetrics>,
    pub config: Arc<ConfigMetrics>,
}

pub struct ApplicationMetrics {
    backend: BackendMetrics,
    frontend: FrontendMetrics,
    routing: RoutingMetrics,
    tko_map: Arc<TkoTrackerMap>,
    destinations: Arc<DestinationMetricsRegistry>,
    observability: Arc<ObservabilityMetrics>,
    metadata: ProcessMetadata,
    config: Arc<ConfigMetrics>,
}

impl ApplicationMetrics {
    pub fn new(setup: ApplicationMetricsSetup) -> Self {
        let backend: Vec<_> = setup
            .workers
            .iter()
            .map(|worker| Arc::clone(&worker.backend))
            .collect();

        let frontend: Vec<_> = setup
            .workers
            .iter()
            .map(|worker| Arc::clone(&worker.frontend))
            .collect();

        let routing: Vec<_> = setup
            .workers
            .iter()
            .map(|worker| Arc::clone(&worker.routing))
            .collect();

        Self {
            backend: BackendMetrics { shards: backend },
            frontend: FrontendMetrics { shards: frontend },
            routing: RoutingMetrics { shards: routing },
            tko_map: setup.tko_map,
            destinations: setup.destinations,
            observability: setup.observability,
            metadata: setup.metadata,
            config: setup.config,
        }
    }
}

impl MetricsSource for ApplicationMetrics {
    fn encode(&self, out: &mut MetricsText) {
        self.backend.encode(out);
        self.frontend.encode(out);
        self.routing.encode(out);
        self.tko_map.encode(out);
        self.destinations.encode(out);
        self.observability.encode(out);
        self.metadata.encode(out);
        self.config.encode(out);
    }
}

#[cfg(test)]
mod tests {
    use rusty_mcrouter_backend::classify::ResultCode;
    use rusty_mcrouter_config::{parse, ConfigDocument, ReloadStage};
    use rusty_mcrouter_observability_primitives::test_support::noop_sink;
    use rusty_mcrouter_protocol::RequestKind;

    use super::*;

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
    fn application_metrics_assemble_every_required_metric_in_order() {
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
        let config = Arc::new(ConfigMetrics::default());
        config.applied(7, 1_700_000_000);
        config.reload_attempts.add(2);
        config.rejected(ReloadStage::Read);

        let metrics = ApplicationMetrics::new(ApplicationMetricsSetup {
            metadata: ProcessMetadata {
                start_unix_secs: 1_700_000_000,
                num_proxies: 2,
            },
            workers,
            tko_map,
            destinations,
            observability,
            config: Arc::clone(&config),
        });
        let text = metrics.render();

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
            position("rusty_mcrouter_config_generation 7\n"),
            position("rusty_mcrouter_config_reload_attempts_total 2\n"),
            position("rusty_mcrouter_config_reload_failures_total{stage=\"read\"} 1\n"),
            position("rusty_mcrouter_config_last_reload_successful 0\n"),
            position("rusty_mcrouter_config_last_success_timestamp_seconds 1700000000\n"),
        ];
        assert!(order.windows(2).all(|w| w[0] < w[1]), "{text}");
        assert!(text.contains("rusty_mcrouter_requests_failed_total 1\n"));
        assert!(text.contains("rusty_mcrouter_pool_requests_total{pool=\"pool_a\"} 0\n"));

        config.applied(8, 1_700_000_001);
        let text = metrics.render();
        assert!(text.contains("rusty_mcrouter_config_generation 8\n"));
        assert!(text.contains("rusty_mcrouter_config_last_reload_successful 1\n"));
        assert!(text.contains("rusty_mcrouter_config_last_success_timestamp_seconds 1700000001\n"));
    }
}
