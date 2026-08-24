use std::{cell::Cell, rc::Rc, sync::Arc};

use rusty_mcrouter_backend::classify::reply_code;
use rusty_mcrouter_protocol::Reply;
use tokio::time::Instant;

use crate::{
    RouteError, RoutingEventRecord, RoutingEventSink, RoutingMetricsLayout, RoutingMetricsShard,
};

pub struct RoutingState {
    metrics: Arc<RoutingMetricsShard>,
    events: RoutingEventSink,
}

pub struct RouteContext<'a> {
    state: &'a RoutingState,
    selected_pool: Cell<Option<usize>>,
    started: Instant,
}

impl RouteContext<'_> {
    pub fn metrics(&self) -> &RoutingMetricsShard {
        &self.state.metrics
    }

    pub(crate) fn emit(&self, event: RoutingEventRecord) {
        self.state.events.emit(event);
    }

    pub(crate) fn select_pool(&self, index: usize) {
        if self.selected_pool.get().is_none() {
            self.selected_pool.set(Some(index));
        }
    }

    pub fn finish(self, result: &Result<Reply, RouteError>) {
        let Some(index) = self.selected_pool.get() else {
            return;
        };
        let pool = self
            .state
            .metrics
            .pool(index)
            .expect("selected pool index must exist in routing metrics layout");
        pool.completed_requests.inc();
        pool.total_duration_us_sum
            .add(self.started.elapsed().as_micros() as u64);
        if result
            .as_ref()
            .map_or(true, |reply| reply_code(reply).is_error())
        {
            pool.final_errors.inc();
        }
    }
}

impl RoutingState {
    pub fn new(metrics: Arc<RoutingMetricsShard>) -> Rc<Self> {
        Self::with_event_sink(metrics, RoutingEventSink::new(|_| {}))
    }

    pub fn with_event_sink(
        metrics: Arc<RoutingMetricsShard>,
        events: RoutingEventSink,
    ) -> Rc<Self> {
        Rc::new(Self { metrics, events })
    }

    pub fn layout(&self) -> &Arc<RoutingMetricsLayout> {
        self.metrics.layout()
    }

    pub fn context(&self) -> RouteContext<'_> {
        RouteContext {
            state: self,
            selected_pool: Cell::new(None),
            started: Instant::now(),
        }
    }
}

#[cfg(test)]
pub(crate) fn test_routing_state() -> Rc<RoutingState> {
    let layout = RoutingMetricsLayout::new(Vec::<String>::new());
    RoutingState::new(RoutingMetricsShard::new(layout))
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use rusty_mcrouter_protocol::test_support::{get, get_miss};
    use rusty_mcrouter_protocol::{Reply, Request};

    use super::*;
    use crate::{DynRoute, Route, RouteError};

    struct InspectContext {
        expected: Arc<RoutingMetricsShard>,
        observations: Rc<Cell<usize>>,
    }

    impl Route for InspectContext {
        async fn route(
            &self,
            context: &RouteContext<'_>,
            _request: Request,
        ) -> Result<Reply, RouteError> {
            self.observe(context);
            tokio::task::yield_now().await;
            self.observe(context);
            Ok(get_miss())
        }
    }

    impl InspectContext {
        fn observe(&self, context: &RouteContext<'_>) {
            assert!(std::ptr::eq(context.metrics(), Arc::as_ptr(&self.expected)));
            self.observations.set(self.observations.get() + 1);
        }
    }

    fn state() -> (Rc<RoutingState>, Arc<RoutingMetricsShard>) {
        let layout = RoutingMetricsLayout::new(Vec::<String>::new());
        let metrics = RoutingMetricsShard::new(layout);
        (RoutingState::new(Arc::clone(&metrics)), metrics)
    }

    async fn execute(
        state: &RoutingState,
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

    #[test]
    fn first_sendable_pool_wins_within_one_context() {
        let layout = RoutingMetricsLayout::new(["primary".to_string(), "backup".to_string()]);
        let metrics = RoutingMetricsShard::new(layout);
        let state = RoutingState::new(Arc::clone(&metrics));
        let context = state.context();
        context.select_pool(0);
        context.select_pool(1);

        context.finish(&Ok(get_miss()));

        assert_eq!(metrics.pools[0].completed_requests.load(), 1);
        assert_eq!(metrics.pools[1].completed_requests.load(), 0);
    }

    #[test]
    fn concurrent_contexts_keep_selected_pools_isolated() {
        let layout = RoutingMetricsLayout::new(["first".to_string(), "second".to_string()]);
        let metrics = RoutingMetricsShard::new(layout);
        let state = RoutingState::new(Arc::clone(&metrics));
        let first = state.context();
        let second = state.context();
        first.select_pool(0);
        second.select_pool(1);

        first.finish(&Ok(get_miss()));
        second.finish(&Ok(get_miss()));

        assert_eq!(metrics.pools[0].completed_requests.load(), 1);
        assert_eq!(metrics.pools[1].completed_requests.load(), 1);
    }

    #[test]
    fn request_without_selected_pool_has_no_final_pool_metrics() {
        let layout = RoutingMetricsLayout::new(["pool".to_string()]);
        let metrics = RoutingMetricsShard::new(layout);
        let state = RoutingState::new(Arc::clone(&metrics));

        state.context().finish(&Ok(get_miss()));

        assert_eq!(metrics.pools[0].completed_requests.load(), 0);
        assert_eq!(metrics.pools[0].final_errors.load(), 0);
        assert_eq!(metrics.pools[0].total_duration_us_sum.load(), 0);
    }
}
