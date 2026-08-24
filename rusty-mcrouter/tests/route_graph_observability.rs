use std::collections::BTreeMap;
use std::sync::Arc;

use rusty_mcrouter_backend::destination;
use rusty_mcrouter_backend::error::{RequestError, SendError};
use rusty_mcrouter_backend::test_support::MockBackend;
use rusty_mcrouter_backend::{BackendFactory, BackendFactoryError, PoolHealth};
use rusty_mcrouter_config::{parse, ConfigDocument};
use rusty_mcrouter_core::{
    build_route, DynRoute, RoutingMetricsLayout, RoutingMetricsShard, RoutingState,
};
use rusty_mcrouter_observability::metrics::MetricsRegistry;
use rusty_mcrouter_observability::sources::RoutingSource;
use rusty_mcrouter_observability_primitives::test_support::noop_sink;
use rusty_mcrouter_protocol::test_support::{bare_server_error, expect_error, get, get_miss};
use rusty_mcrouter_protocol::Reply;

#[derive(Clone)]
struct Factory {
    backends: BTreeMap<String, MockBackend>,
}

impl Factory {
    fn new(backends: impl IntoIterator<Item = (&'static str, MockBackend)>) -> Self {
        Self {
            backends: backends
                .into_iter()
                .map(|(server, backend)| (server.to_string(), backend))
                .collect(),
        }
    }
}

impl BackendFactory for Factory {
    type Backend = MockBackend;

    fn make(
        &self,
        server: &str,
        _cfg: &destination::DestinationConfig,
        _pool: &PoolHealth<'_>,
    ) -> Result<Self::Backend, BackendFactoryError> {
        self.backends
            .get(server)
            .cloned()
            .ok_or_else(|| BackendFactoryError::InvalidAddress {
                addr: server.to_string(),
            })
    }
}

fn build(
    config: &ConfigDocument,
    factory: &Factory,
) -> (
    Arc<RoutingMetricsShard>,
    std::rc::Rc<RoutingState>,
    std::rc::Rc<dyn DynRoute>,
) {
    let layout = RoutingMetricsLayout::new(config.pools.keys().cloned());
    let metrics = RoutingMetricsShard::new(layout);
    let state = RoutingState::new(Arc::clone(&metrics), noop_sink());
    let route = build_route(
        config,
        factory,
        &destination::DestinationConfig::default(),
        state.layout(),
    )
    .unwrap();
    (metrics, state, route)
}

async fn route(route: std::rc::Rc<dyn DynRoute>, state: std::rc::Rc<RoutingState>) -> Reply {
    let context = state.context();
    let result = route.route_dyn(&context, get(b"key")).await;
    context.finish(&result);
    result.unwrap_or_else(|_| bare_server_error())
}

fn scrape(metrics: Arc<RoutingMetricsShard>) -> String {
    let mut registry = MetricsRegistry::new();
    registry.register(Box::new(RoutingSource {
        shards: vec![metrics],
    }));
    registry.render()
}

fn assert_sample(text: &str, sample: &str) {
    assert!(
        text.lines().any(|line| line == sample),
        "missing {sample:?}\n{text}"
    );
}

#[tokio::test]
async fn healthy_route_exports_attempt_and_final_pool_metrics() {
    let config = parse(
        r#"{"pools": {"primary": {"servers": ["primary:1"]}}, "route": "PoolRoute|primary"}"#,
    )
    .unwrap();
    let factory = Factory::new([("primary:1", MockBackend::miss())]);
    let (metrics, state, route_graph) = build(&config, &factory);

    assert_eq!(route(route_graph, state).await, get_miss());

    let text = scrape(metrics);
    assert_sample(
        &text,
        "rusty_mcrouter_pool_requests_total{pool=\"primary\"} 1",
    );
    assert_sample(
        &text,
        "rusty_mcrouter_pool_completed_requests_total{pool=\"primary\"} 1",
    );
    assert_sample(
        &text,
        "rusty_mcrouter_pool_requests_failed_total{pool=\"primary\"} 0",
    );
    assert_sample(&text, "rusty_mcrouter_failover_total{policy=\"inorder\"} 0");
}

#[tokio::test]
async fn failover_exports_both_attempts_and_attributes_final_to_first_sendable_pool() {
    let config = parse(
        r#"{
            "pools": {
                "primary": {"servers": ["primary:1"]},
                "backup": {"servers": ["backup:1"]}
            },
            "route": {
                "type": "FailoverRoute",
                "children": ["PoolRoute|primary", "PoolRoute|backup"]
            }
        }"#,
    )
    .unwrap();
    let factory = Factory::new([
        (
            "primary:1",
            MockBackend::failing(SendError::Request(RequestError::Timeout { sent: true })),
        ),
        ("backup:1", MockBackend::miss()),
    ]);
    let (metrics, state, route_graph) = build(&config, &factory);

    assert_eq!(route(route_graph, state).await, get_miss());

    let text = scrape(metrics);
    assert_sample(
        &text,
        "rusty_mcrouter_pool_requests_total{pool=\"primary\"} 1",
    );
    assert_sample(
        &text,
        "rusty_mcrouter_pool_completed_requests_total{pool=\"primary\"} 1",
    );
    assert_sample(
        &text,
        "rusty_mcrouter_pool_requests_total{pool=\"backup\"} 1",
    );
    assert_sample(
        &text,
        "rusty_mcrouter_pool_completed_requests_total{pool=\"backup\"} 0",
    );
    assert_sample(
        &text,
        "rusty_mcrouter_pool_requests_failed_total{pool=\"primary\"} 0",
    );
    assert_sample(&text, "rusty_mcrouter_failover_total{policy=\"inorder\"} 1");
    assert_sample(
        &text,
        "rusty_mcrouter_failover_policy_errors_total{class=\"result\"} 1",
    );
    assert_sample(
        &text,
        "rusty_mcrouter_failover_exhausted_total{policy=\"inorder\"} 0",
    );
}

#[tokio::test]
async fn single_child_failure_exports_exhaustion_without_failover_entry() {
    let config = parse(
        r#"{
            "pools": {"primary": {"servers": ["primary:1"]}},
            "route": {
                "type": "FailoverRoute",
                "children": ["PoolRoute|primary"]
            }
        }"#,
    )
    .unwrap();
    let factory = Factory::new([(
        "primary:1",
        MockBackend::failing(SendError::Request(RequestError::Timeout { sent: true })),
    )]);
    let (metrics, state, route_graph) = build(&config, &factory);

    expect_error(route(route_graph, state).await);

    let text = scrape(metrics);
    assert_sample(&text, "rusty_mcrouter_failover_total{policy=\"inorder\"} 0");
    assert_sample(
        &text,
        "rusty_mcrouter_failover_exhausted_total{policy=\"inorder\"} 1",
    );
    assert_sample(
        &text,
        "rusty_mcrouter_failover_policy_errors_total{class=\"result\"} 1",
    );
    assert_sample(
        &text,
        "rusty_mcrouter_pool_completed_requests_total{pool=\"primary\"} 1",
    );
    assert_sample(
        &text,
        "rusty_mcrouter_pool_requests_failed_total{pool=\"primary\"} 1",
    );
}
