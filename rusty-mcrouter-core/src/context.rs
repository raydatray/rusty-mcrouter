use std::{cell::Cell, rc::Rc, sync::Arc};

use rusty_mcrouter_backend::classify::reply_code;
use rusty_mcrouter_config::{ConfigDocument, PoolId};
use rusty_mcrouter_protocol::{Reply, Request};
use tokio::time::Instant;

use crate::{
    DynRoute, PoolMetrics, PoolMetricsTable, RouteError, RoutingEventRecord, RoutingEventSink,
    RoutingMetricsShard,
};

/// One route graph generation's state; `metrics` and `events` are shared by
/// every generation on the proxy.
pub struct RoutingState {
    metrics: Arc<RoutingMetricsShard>,
    events: Rc<RoutingEventSink>,
    pools: PoolMetricsTable,
}

pub struct RouteContext {
    state: Rc<RoutingState>,
    selected_pool: Cell<Option<PoolId>>,
    started: Instant,
}

impl RouteContext {
    fn fork(&self) -> Self {
        Self {
            state: Rc::clone(&self.state),
            selected_pool: Cell::new(None),
            started: Instant::now(),
        }
    }

    pub(crate) fn spawn_background(&self, route: Rc<dyn DynRoute>, request: Request) {
        let context = self.fork();

        drop(tokio::task::spawn_local(async move {
            let _ = route.route_dyn(&context, request).await;
        }));
    }

    pub fn metrics(&self) -> &RoutingMetricsShard {
        &self.state.metrics
    }

    pub(crate) fn pool_metrics(&self, pool: PoolId) -> &PoolMetrics {
        &self.state.pools[pool]
    }

    pub(crate) fn emit(&self, event: RoutingEventRecord) {
        self.state.events.emit(event);
    }

    pub(crate) fn select_pool(&self, pool: PoolId) {
        if self.selected_pool.get().is_none() {
            self.selected_pool.set(Some(pool));
        }
    }

    pub fn finish(self, result: &Result<Reply, RouteError>) {
        let Some(pool) = self.selected_pool.get() else {
            return;
        };
        let metrics = &self.state.pools[pool];
        metrics.completed_requests.inc();
        metrics
            .total_duration_us_sum
            .add(self.started.elapsed().as_micros() as u64);
        if result
            .as_ref()
            .map_or(true, |reply| reply_code(reply).is_error())
        {
            metrics.final_errors.inc();
        }
    }
}

impl RoutingState {
    /// `config` must be the document the route graph was built from: the
    /// graph's `PoolId`s index the pool table resolved here.
    pub fn new(
        metrics: Arc<RoutingMetricsShard>,
        events: Rc<RoutingEventSink>,
        config: &ConfigDocument,
    ) -> Rc<Self> {
        let pools = metrics.table_for(config);
        Rc::new(Self {
            metrics,
            events,
            pools,
        })
    }

    pub fn metrics(&self) -> &Arc<RoutingMetricsShard> {
        &self.metrics
    }

    pub fn pool(&self, pool: PoolId) -> &PoolMetrics {
        &self.pools[pool]
    }

    pub fn context(self: &Rc<Self>) -> RouteContext {
        RouteContext {
            state: Rc::clone(self),
            selected_pool: Cell::new(None),
            started: Instant::now(),
        }
    }
}

#[cfg(test)]
pub(crate) fn test_routing_state() -> Rc<RoutingState> {
    test_state(&crate::metrics::test_config(&[]))
}

#[cfg(test)]
pub(crate) fn test_state(config: &ConfigDocument) -> Rc<RoutingState> {
    RoutingState::new(
        RoutingMetricsShard::new(),
        Rc::new(rusty_mcrouter_observability_primitives::test_support::noop_sink()),
        config,
    )
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use rusty_mcrouter_observability_primitives::test_support::noop_sink;
    use rusty_mcrouter_protocol::test_support::{get, get_miss};
    use rusty_mcrouter_protocol::{Reply, Request};

    use super::*;
    use crate::metrics::test_config;
    use crate::{DynRoute, Route, RouteError};

    struct InspectContext {
        expected: Arc<RoutingMetricsShard>,
        observations: Rc<Cell<usize>>,
    }

    struct CountRoute {
        calls: Rc<Cell<usize>>,
    }

    impl Route for CountRoute {
        async fn route(
            &self,
            _context: &RouteContext,
            _request: Request,
        ) -> Result<Reply, RouteError> {
            self.calls.set(self.calls.get() + 1);
            Ok(get_miss())
        }
    }

    impl Route for InspectContext {
        async fn route(
            &self,
            context: &RouteContext,
            _request: Request,
        ) -> Result<Reply, RouteError> {
            self.observe(context);
            tokio::task::yield_now().await;
            self.observe(context);
            Ok(get_miss())
        }
    }

    impl InspectContext {
        fn observe(&self, context: &RouteContext) {
            assert!(std::ptr::eq(context.metrics(), Arc::as_ptr(&self.expected)));
            self.observations.set(self.observations.get() + 1);
        }
    }

    fn state() -> (Rc<RoutingState>, Arc<RoutingMetricsShard>) {
        let state = test_routing_state();
        let metrics = Arc::clone(state.metrics());
        (state, metrics)
    }

    async fn execute(
        state: &Rc<RoutingState>,
        route: Rc<dyn DynRoute>,
        request: Request,
    ) -> Result<Reply, RouteError> {
        let context = state.context();
        route.route_dyn(&context, request).await
    }

    #[tokio::test]
    async fn context_stays_available_across_awaits() {
        let (state, metrics) = state();
        let observations = Rc::new(Cell::new(0));
        let route = InspectContext {
            expected: metrics,
            observations: Rc::clone(&observations),
        }
        .into_dyn();

        execute(&state, route, get(b"key")).await.unwrap();
        assert_eq!(observations.get(), 2);
    }

    #[tokio::test]
    async fn concurrent_routes_keep_their_states() {
        let (first_state, first_metrics) = state();
        let (second_state, second_metrics) = state();
        let first_observations = Rc::new(Cell::new(0));
        let second_observations = Rc::new(Cell::new(0));

        let first = execute(
            &first_state,
            InspectContext {
                expected: first_metrics,
                observations: Rc::clone(&first_observations),
            }
            .into_dyn(),
            get(b"first"),
        );
        let second = execute(
            &second_state,
            InspectContext {
                expected: second_metrics,
                observations: Rc::clone(&second_observations),
            }
            .into_dyn(),
            get(b"second"),
        );

        let (first_result, second_result) = tokio::join!(first, second);
        first_result.unwrap();
        second_result.unwrap();
        assert_eq!(first_observations.get(), 2);
        assert_eq!(second_observations.get(), 2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn background_route_runs_on_the_local_executor() {
        let local = tokio::task::LocalSet::new();

        local
            .run_until(async {
                let (state, _) = state();
                let context = state.context();
                let calls = Rc::new(Cell::new(0));
                let route = CountRoute {
                    calls: Rc::clone(&calls),
                }
                .into_dyn();

                context.spawn_background(route, get(b"key"));
                tokio::task::yield_now().await;

                assert_eq!(calls.get(), 1);
            })
            .await;
    }

    #[test]
    fn first_sendable_pool_wins_within_one_context() {
        let config = test_config(&["primary", "backup"]);
        let primary = config.pool_id("primary").unwrap();
        let backup = config.pool_id("backup").unwrap();
        let state = test_state(&config);
        let context = state.context();
        context.select_pool(primary);
        context.select_pool(backup);

        context.finish(&Ok(get_miss()));

        assert_eq!(state.pool(primary).completed_requests.load(), 1);
        assert_eq!(state.pool(backup).completed_requests.load(), 0);
    }

    #[test]
    fn concurrent_contexts_keep_selected_pools_isolated() {
        let config = test_config(&["first", "second"]);
        let first_pool = config.pool_id("first").unwrap();
        let second_pool = config.pool_id("second").unwrap();
        let state = test_state(&config);
        let first = state.context();
        let second = state.context();
        first.select_pool(first_pool);
        second.select_pool(second_pool);

        first.finish(&Ok(get_miss()));
        second.finish(&Ok(get_miss()));

        assert_eq!(state.pool(first_pool).completed_requests.load(), 1);
        assert_eq!(state.pool(second_pool).completed_requests.load(), 1);
    }

    #[test]
    fn request_without_selected_pool_has_no_final_pool_metrics() {
        let config = test_config(&["pool"]);
        let pool = config.pool_id("pool").unwrap();
        let state = test_state(&config);

        state.context().finish(&Ok(get_miss()));

        assert_eq!(state.pool(pool).completed_requests.load(), 0);
        assert_eq!(state.pool(pool).final_errors.load(), 0);
        assert_eq!(state.pool(pool).total_duration_us_sum.load(), 0);
    }

    #[test]
    fn contexts_resolve_pool_ids_against_their_own_generation() {
        let shard = RoutingMetricsShard::new();
        let v1 = test_config(&["users"]);
        let v2 = test_config(&["aaa", "users"]);
        let old = RoutingState::new(Arc::clone(&shard), Rc::new(noop_sink()), &v1);
        let new = RoutingState::new(Arc::clone(&shard), Rc::new(noop_sink()), &v2);

        let in_flight = old.context();
        in_flight.select_pool(v1.pool_id("users").unwrap());
        in_flight.finish(&Ok(get_miss()));

        let users = new.pool(v2.pool_id("users").unwrap());
        assert_eq!(users.completed_requests.load(), 1);
        assert_eq!(
            new.pool(v2.pool_id("aaa").unwrap())
                .completed_requests
                .load(),
            0
        );
    }
}
