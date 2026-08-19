use std::{rc::Rc, sync::Arc};

use crate::{RoutingMetricsLayout, RoutingMetricsShard};

pub struct RoutingState {
    metrics: Arc<RoutingMetricsShard>,
}

pub struct RouteContext<'a> {
    state: &'a RoutingState,
}

impl RouteContext<'_> {
    pub fn metrics(&self) -> &RoutingMetricsShard {
        &self.state.metrics
    }
}

impl RoutingState {
    pub fn new(metrics: Arc<RoutingMetricsShard>) -> Rc<Self> {
        Rc::new(Self { metrics })
    }

    pub fn layout(&self) -> &Arc<RoutingMetricsLayout> {
        self.metrics.layout()
    }

    pub fn context(&self) -> RouteContext<'_> {
        RouteContext { state: self }
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

    use rusty_mcrouter_protocol::reply::GetReply;
    use rusty_mcrouter_protocol::test_support::get;
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
            Ok(Reply::Get(GetReply::Miss))
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
}
