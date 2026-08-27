use std::rc::Rc;

use rusty_mcrouter_protocol::{Reply, Request};

use crate::failover::{route_code, FailoverErrors, FailoverPolicy};
use crate::routes::{DynRoute, Result, Route, RouteError};
use crate::{
    FailoverErrorClass, FailoverPolicyKind, RouteContext, RoutingEvent, RoutingEventRecord,
};

pub struct FailoverRoute {
    children: Vec<Rc<dyn DynRoute>>,
    errors: FailoverErrors,
    policy: Box<dyn FailoverPolicy>,
    /// Non-TKO error budget. Candidate-list limits belong to the policy.
    max_error_tries: usize,
}

impl FailoverRoute {
    pub fn new(
        children: Vec<Rc<dyn DynRoute>>,
        errors: FailoverErrors,
        policy: Box<dyn FailoverPolicy>,
        max_error_tries: usize,
    ) -> Self {
        debug_assert!(!children.is_empty());
        debug_assert!(max_error_tries > 0);

        Self {
            children,
            errors,
            policy,
            max_error_tries,
        }
    }
}

/// mcrouter FailoverRoute.h:221-230 (verified): "We didn't do any work for
/// TKO or hard TKO. Don't count it as a try." A fast-failed child costs
/// nothing, so it must not consume failover budget.
fn is_free_try(result: &Result<Reply>) -> bool {
    route_code(result).is_some_and(|c| c.is_tko_or_hard_tko())
}

fn route_result_is_error(result: &Result<Reply>) -> bool {
    match route_code(result) {
        Some(code) => code.is_error(),
        None => result.is_err(),
    }
}

fn record_policy_error(context: &RouteContext<'_>, result: &Result<Reply>) {
    let Some(code) = route_code(result) else {
        return;
    };
    let class = if code.is_tko_or_hard_tko() {
        FailoverErrorClass::Tko
    } else if code.is_error() {
        FailoverErrorClass::Result
    } else {
        return;
    };
    context.metrics().failover_policy_errors[class as usize].inc();
}

fn record_exhausted(context: &RouteContext<'_>, policy: FailoverPolicyKind, request: &Request) {
    context.metrics().failover_exhausted[policy as usize].inc();
    context.emit(RoutingEventRecord {
        event: RoutingEvent::FailoverTargetsExhausted,
        policy,
        command: request.kind(),
    });
}

impl Route for FailoverRoute {
    async fn route(&self, context: &RouteContext<'_>, request: Request) -> Result<Reply> {
        let policy = self.policy.kind();
        let mut tries = 0usize;

        let primary = self.children[0].route_dyn(context, request.clone()).await;
        let primary_is_error = route_result_is_error(&primary);
        let primary_failed = self.errors.should_failover(&request, &primary);
        self.policy.record_outcome(0, primary_is_error);
        if !primary_failed {
            return primary;
        }

        record_policy_error(context, &primary);

        if !is_free_try(&primary) {
            tries += 1;
        }

        let mut order = self
            .policy
            .failover_order(&request, self.children.len())
            .into_iter()
            .peekable();

        if order.peek().is_none() {
            if primary_is_error {
                record_exhausted(context, policy, &request);
            }
            return primary;
        }

        context.metrics().failover[policy as usize].inc();

        let mut last = primary;
        while let Some(index) = order.next() {
            if tries >= self.max_error_tries {
                break;
            }
            let child = self
                .children
                .get(index)
                .ok_or(RouteError::SelectorOutOfRange {
                    idx: index,
                    len: self.children.len(),
                })?;
            let reply = child.route_dyn(context, request.clone()).await;
            let is_error = route_result_is_error(&reply);
            self.policy.record_outcome(index, is_error);

            if order.peek().is_none() {
                if is_error {
                    record_exhausted(context, policy, &request);
                }
                return reply;
            }

            let failed = self.errors.should_failover(&request, &reply);
            if !failed {
                return reply;
            }

            record_policy_error(context, &reply);

            if !is_free_try(&reply) {
                tries += 1;
            }
            last = reply;
        }
        last
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::failover::{InOrderPolicy, LeastFailuresPolicy};
    use crate::routes::{DestinationRoute, RouteError};
    use rusty_mcrouter_backend::classify::ResultCode;
    use rusty_mcrouter_backend::error::{ConnectError, LocalError, RequestError, SendError};
    use rusty_mcrouter_backend::test_support::MockBackend;
    use rusty_mcrouter_observability_primitives::test_support::{
        noop_sink, recording_sink, EventLog,
    };
    use rusty_mcrouter_protocol::test_support::{
        get, get_hit, get_miss, server_error, store, store_success,
    };

    use crate::context::test_routing_state;
    use crate::metrics::test_metrics_layout;
    use crate::{RoutingMetricsLayout, RoutingMetricsShard, RoutingState};

    fn dest(backend: MockBackend) -> Rc<dyn DynRoute> {
        DestinationRoute::new(backend).into_dyn()
    }

    fn pooled_dest(backend: MockBackend, pool_index: usize) -> Rc<dyn DynRoute> {
        DestinationRoute::for_pool(backend, pool_index).into_dyn()
    }

    fn timeout() -> SendError {
        SendError::Request(RequestError::Timeout { sent: true })
    }

    fn tko() -> SendError {
        SendError::Tko {
            reason: ResultCode::Timeout,
        }
    }

    fn in_order(children: Vec<Rc<dyn DynRoute>>) -> FailoverRoute {
        let max_tries = children.len();
        FailoverRoute::new(
            children,
            FailoverErrors::default(),
            Box::new(InOrderPolicy),
            max_tries,
        )
    }

    async fn execute(route: &FailoverRoute, request: Request) -> Result<Reply> {
        let state = test_routing_state();
        let context = state.context();
        route.route(&context, request).await
    }

    #[allow(clippy::type_complexity)]
    fn instrumented_state() -> (
        Rc<RoutingState>,
        Arc<RoutingMetricsShard>,
        EventLog<RoutingEventRecord>,
    ) {
        let metrics = RoutingMetricsShard::new(RoutingMetricsLayout::empty());
        let (sink, events) = recording_sink();
        (
            RoutingState::new(Arc::clone(&metrics), sink),
            metrics,
            events,
        )
    }

    #[tokio::test]
    async fn primary_success_records_no_failover_metrics() {
        let route = in_order(vec![
            dest(MockBackend::replying(get_hit(b"1"))),
            dest(MockBackend::replying(get_hit(b"2"))),
        ]);
        let (state, metrics, events) = instrumented_state();
        let context = state.context();

        assert_eq!(
            route.route(&context, get(b"key")).await.unwrap(),
            get_hit(b"1")
        );
        assert_eq!(
            metrics.failover[FailoverPolicyKind::InOrder as usize].load(),
            0
        );
        assert_eq!(
            metrics.failover_policy_errors[FailoverErrorClass::Result as usize].load(),
            0
        );
        assert_eq!(
            metrics.failover_exhausted[FailoverPolicyKind::InOrder as usize].load(),
            0
        );
        assert!(events.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn exhausted_targets_update_metrics_and_emit_once() {
        let route = in_order(vec![
            dest(MockBackend::failing(timeout())),
            dest(MockBackend::failing(timeout())),
        ]);
        let (state, metrics, events) = instrumented_state();
        let context = state.context();

        assert!(route.route(&context, get(b"key")).await.is_err());
        assert_eq!(
            metrics.failover[FailoverPolicyKind::InOrder as usize].load(),
            1
        );
        assert_eq!(
            metrics.failover_exhausted[FailoverPolicyKind::InOrder as usize].load(),
            1
        );
        assert_eq!(
            metrics.failover_policy_errors[FailoverErrorClass::Result as usize].load(),
            1
        );

        let events = events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event, RoutingEvent::FailoverTargetsExhausted);
        assert_eq!(events[0].policy, FailoverPolicyKind::InOrder);
        assert_eq!(events[0].command, rusty_mcrouter_protocol::RequestKind::Get);
    }

    #[tokio::test]
    async fn primary_and_middle_errors_are_policy_errors_but_terminal_is_not() {
        let route = in_order(vec![
            dest(MockBackend::failing(timeout())),
            dest(MockBackend::failing(timeout())),
            dest(MockBackend::failing(timeout())),
        ]);
        let (state, metrics, events) = instrumented_state();
        let context = state.context();

        assert!(route.route(&context, get(b"key")).await.is_err());
        assert_eq!(
            metrics.failover_policy_errors[FailoverErrorClass::Result as usize].load(),
            2
        );
        assert_eq!(
            metrics.failover_exhausted[FailoverPolicyKind::InOrder as usize].load(),
            1
        );
        assert_eq!(events.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn single_target_exhaustion_counts_the_primary_policy_error() {
        let route = in_order(vec![dest(MockBackend::failing(timeout()))]);
        let (state, metrics, events) = instrumented_state();
        let context = state.context();

        assert!(route.route(&context, get(b"key")).await.is_err());
        assert_eq!(
            metrics.failover[FailoverPolicyKind::InOrder as usize].load(),
            0
        );
        assert_eq!(
            metrics.failover_exhausted[FailoverPolicyKind::InOrder as usize].load(),
            1
        );
        assert_eq!(
            metrics.failover_policy_errors[FailoverErrorClass::Result as usize].load(),
            1
        );
        assert_eq!(events.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn tko_and_hard_tko_policy_errors_use_tko_class() {
        for error in [tko(), SendError::Connect(ConnectError::Timeout)] {
            let route = in_order(vec![
                dest(MockBackend::failing(error)),
                dest(MockBackend::replying(get_hit(b"1"))),
            ]);
            let (state, metrics, _events) = instrumented_state();
            let context = state.context();

            assert_eq!(
                route.route(&context, get(b"key")).await.unwrap(),
                get_hit(b"1")
            );
            assert_eq!(
                metrics.failover_policy_errors[FailoverErrorClass::Tko as usize].load(),
                1
            );
            assert_eq!(
                metrics.failover_exhausted[FailoverPolicyKind::InOrder as usize].load(),
                0
            );
        }
    }

    #[tokio::test]
    async fn error_budget_stop_is_not_target_exhaustion() {
        let route = FailoverRoute::new(
            vec![
                dest(MockBackend::failing(timeout())),
                dest(MockBackend::replying(get_hit(b"1"))),
            ],
            FailoverErrors::default(),
            Box::new(InOrderPolicy),
            1,
        );
        let (state, metrics, events) = instrumented_state();
        let context = state.context();

        assert!(route.route(&context, get(b"key")).await.is_err());
        assert_eq!(
            metrics.failover[FailoverPolicyKind::InOrder as usize].load(),
            1
        );
        assert_eq!(
            metrics.failover_exhausted[FailoverPolicyKind::InOrder as usize].load(),
            0
        );
        assert!(events.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn real_primary_error_claims_final_attribution() {
        let route = in_order(vec![
            pooled_dest(MockBackend::failing(timeout()), 0),
            pooled_dest(MockBackend::replying(get_hit(b"1")), 1),
        ]);
        let layout = test_metrics_layout(&["primary", "backup"]);
        let metrics = RoutingMetricsShard::new(layout);
        let state = RoutingState::new(Arc::clone(&metrics), noop_sink());
        let context = state.context();

        let result = route.route(&context, get(b"key")).await;
        assert_eq!(result.as_ref().unwrap(), &get_hit(b"1"));
        context.finish(&result);

        assert_eq!(metrics.pools[0].requests.load(), 1);
        assert_eq!(metrics.pools[0].completed_requests.load(), 1);
        assert_eq!(metrics.pools[0].final_errors.load(), 0);
        assert_eq!(metrics.pools[1].requests.load(), 1);
        assert_eq!(metrics.pools[1].completed_requests.load(), 0);
        assert_eq!(metrics.pools[1].final_errors.load(), 0);
    }

    #[tokio::test]
    async fn final_error_is_attributed_once_to_first_sendable_pool() {
        let route = in_order(vec![
            pooled_dest(MockBackend::failing(timeout()), 0),
            pooled_dest(MockBackend::failing(timeout()), 1),
        ]);
        let layout = test_metrics_layout(&["primary", "backup"]);
        let metrics = RoutingMetricsShard::new(layout);
        let state = RoutingState::new(Arc::clone(&metrics), noop_sink());
        let context = state.context();

        let result = route.route(&context, get(b"key")).await;
        assert!(result.is_err());
        context.finish(&result);

        assert_eq!(metrics.pools[0].completed_requests.load(), 1);
        assert_eq!(metrics.pools[0].final_errors.load(), 1);
        assert_eq!(metrics.pools[1].completed_requests.load(), 0);
        assert_eq!(metrics.pools[1].final_errors.load(), 0);
    }

    #[tokio::test]
    async fn tko_primary_does_not_claim_final_attribution() {
        let route = in_order(vec![
            pooled_dest(MockBackend::failing(tko()), 0),
            pooled_dest(MockBackend::replying(get_hit(b"1")), 1),
        ]);
        let layout = test_metrics_layout(&["primary", "backup"]);
        let metrics = RoutingMetricsShard::new(layout);
        let state = RoutingState::new(Arc::clone(&metrics), noop_sink());
        let context = state.context();

        let result = route.route(&context, get(b"key")).await;
        assert_eq!(result.as_ref().unwrap(), &get_hit(b"1"));
        context.finish(&result);

        assert_eq!(metrics.pools[0].requests.load(), 1);
        assert_eq!(metrics.pools[0].duration_us_sum.load(), 0);
        assert_eq!(metrics.pools[0].completed_requests.load(), 0);
        assert_eq!(metrics.pools[1].requests.load(), 1);
        assert_eq!(metrics.pools[1].completed_requests.load(), 1);
    }

    #[tokio::test]
    async fn all_tko_attempts_have_no_final_pool_attribution() {
        let route = in_order(vec![
            pooled_dest(MockBackend::failing(tko()), 0),
            pooled_dest(MockBackend::failing(tko()), 1),
        ]);
        let layout = test_metrics_layout(&["primary", "backup"]);
        let metrics = RoutingMetricsShard::new(layout);
        let state = RoutingState::new(Arc::clone(&metrics), noop_sink());
        let context = state.context();

        let result = route.route(&context, get(b"key")).await;
        assert!(result.is_err());
        context.finish(&result);

        assert_eq!(metrics.pools[0].requests.load(), 1);
        assert_eq!(metrics.pools[1].requests.load(), 1);
        assert_eq!(metrics.pools[0].completed_requests.load(), 0);
        assert_eq!(metrics.pools[1].completed_requests.load(), 0);
    }

    #[tokio::test]
    async fn least_failures_uses_its_policy_metric_label() {
        let route = FailoverRoute::new(
            vec![
                dest(MockBackend::failing(timeout())),
                dest(MockBackend::replying(get_hit(b"1"))),
            ],
            FailoverErrors::default(),
            Box::new(LeastFailuresPolicy::new(2, 2)),
            usize::MAX,
        );
        let (state, metrics, _events) = instrumented_state();
        let context = state.context();

        assert_eq!(
            route.route(&context, get(b"key")).await.unwrap(),
            get_hit(b"1")
        );
        assert_eq!(
            metrics.failover[FailoverPolicyKind::LeastFailures as usize].load(),
            1
        );
        assert_eq!(
            metrics.failover[FailoverPolicyKind::InOrder as usize].load(),
            0
        );
    }

    #[tokio::test]
    async fn nested_failover_emits_for_each_exhausted_route() {
        let inner = in_order(vec![
            dest(MockBackend::failing(timeout())),
            dest(MockBackend::failing(timeout())),
        ])
        .into_dyn();
        let outer = in_order(vec![inner, dest(MockBackend::failing(timeout()))]);
        let (state, _metrics, events) = instrumented_state();
        let context = state.context();

        assert!(outer.route(&context, get(b"key")).await.is_err());

        let events = events.lock().unwrap();
        assert_eq!(events.len(), 2);
        assert!(events
            .iter()
            .all(|record| record.event == RoutingEvent::FailoverTargetsExhausted));
    }

    #[tokio::test]
    async fn transport_errors_fail_over_to_a_healthy_backup() {
        for err in [
            timeout(),
            SendError::Connect(ConnectError::Timeout),
            SendError::Connect(ConnectError::Failed(std::io::ErrorKind::ConnectionRefused)),
            SendError::Request(RequestError::Dropped {
                kind: std::io::ErrorKind::ConnectionReset,
            }),
            SendError::Local(LocalError::QueueFull),
            tko(),
        ] {
            let primary = MockBackend::failing(err);
            let backup = MockBackend::replying(get_hit(b"1"));
            let route = in_order(vec![dest(primary.clone()), dest(backup.clone())]);

            assert_eq!(execute(&route, get(b"k")).await.unwrap(), get_hit(b"1"));
            assert_eq!(primary.received().len(), 1);
            assert_eq!(backup.received().len(), 1);
        }
    }

    #[tokio::test]
    async fn server_error_reply_fails_over() {
        let primary = MockBackend::replying(server_error(b"boom"));
        let backup = MockBackend::replying(get_hit(b"1"));
        let route = in_order(vec![dest(primary.clone()), dest(backup.clone())]);

        assert_eq!(execute(&route, get(b"k")).await.unwrap(), get_hit(b"1"));
        assert_eq!(backup.received().len(), 1);
    }

    #[tokio::test]
    async fn a_miss_does_not_fail_over() {
        let primary = MockBackend::replying(get_miss());
        let backup = MockBackend::replying(get_hit(b"1"));
        let route = in_order(vec![dest(primary.clone()), dest(backup.clone())]);

        assert_eq!(execute(&route, get(b"k")).await.unwrap(), get_miss());
        assert!(backup.received().is_empty());
    }

    #[tokio::test]
    async fn first_success_wins_and_later_children_are_untouched() {
        let a = MockBackend::failing(timeout());
        let b = MockBackend::replying(get_hit(b"2"));
        let c = MockBackend::replying(get_hit(b"3"));
        let route = in_order(vec![dest(a.clone()), dest(b.clone()), dest(c.clone())]);

        assert_eq!(execute(&route, get(b"k")).await.unwrap(), get_hit(b"2"));
        assert_eq!(a.received().len(), 1);
        assert_eq!(b.received().len(), 1);
        assert!(c.received().is_empty());
    }

    #[tokio::test]
    async fn all_children_failing_returns_the_last_result() {
        let route = in_order(vec![
            dest(MockBackend::failing(timeout())),
            dest(MockBackend::replying(server_error(b"x"))),
        ]);
        assert_eq!(
            execute(&route, get(b"k")).await.unwrap(),
            server_error(b"x")
        );

        let route = in_order(vec![
            dest(MockBackend::failing(timeout())),
            dest(MockBackend::failing(timeout())),
        ]);
        assert!(matches!(
            execute(&route, get(b"k")).await,
            Err(RouteError::Backend(SendError::Request(
                RequestError::Timeout { .. }
            )))
        ));
    }

    #[tokio::test]
    async fn single_child_has_no_backup() {
        let only = MockBackend::replying(get_hit(b"1"));
        let route = in_order(vec![dest(only.clone())]);
        assert_eq!(execute(&route, get(b"k")).await.unwrap(), get_hit(b"1"));
        assert_eq!(only.received().len(), 1);

        let route = in_order(vec![dest(MockBackend::failing(timeout()))]);
        assert!(matches!(
            execute(&route, get(b"k")).await,
            Err(RouteError::Backend(SendError::Request(
                RequestError::Timeout { .. }
            )))
        ));
    }

    #[tokio::test]
    async fn per_op_updates_empty_blocks_write_failover() {
        let primary = MockBackend::failing(timeout());
        let backup = MockBackend::replying(store_success());
        let route = FailoverRoute::new(
            vec![dest(primary.clone()), dest(backup.clone())],
            FailoverErrors::new(None, Some(vec![]), None),
            Box::new(InOrderPolicy),
            2,
        );

        assert!(matches!(
            execute(&route, store(b"k", b"v")).await,
            Err(RouteError::Backend(SendError::Request(
                RequestError::Timeout { .. }
            )))
        ));
        assert!(backup.received().is_empty());
    }

    /// max_tries counts ATTEMPTS including the primary: with a budget of 1,
    /// a failing (non-TKO) primary exhausts it and no backup is tried.
    #[tokio::test]
    async fn max_tries_budget_stops_the_walk() {
        let primary = MockBackend::failing(timeout());
        let backup = MockBackend::replying(get_hit(b"1"));
        let route = FailoverRoute::new(
            vec![dest(primary.clone()), dest(backup.clone())],
            FailoverErrors::default(),
            Box::new(InOrderPolicy),
            1,
        );

        assert!(execute(&route, get(b"k")).await.is_err());
        assert!(
            backup.received().is_empty(),
            "budget of 1 must not reach the backup"
        );
    }

    /// The verified mcrouter rule (FailoverRoute.h:221-230): TKO fast-fails
    /// did no work, so they cost no budget — with max_tries=1, a TKO'd
    /// primary still lets the walk reach a real backup.
    #[tokio::test]
    async fn tko_fast_fail_is_a_free_try() {
        let primary = MockBackend::failing(tko());
        let backup = MockBackend::replying(get_hit(b"1"));
        let route = FailoverRoute::new(
            vec![dest(primary.clone()), dest(backup.clone())],
            FailoverErrors::default(),
            Box::new(InOrderPolicy),
            1,
        );

        assert_eq!(execute(&route, get(b"k")).await.unwrap(), get_hit(b"1"));
    }

    /// Hard-TKO-class connect failures are also free (is_tko_or_hard_tko).
    #[tokio::test]
    async fn connect_errors_are_free_tries() {
        let a = MockBackend::failing(SendError::Connect(ConnectError::Failed(
            std::io::ErrorKind::ConnectionRefused,
        )));
        let b = MockBackend::failing(SendError::Connect(ConnectError::Timeout));
        let c = MockBackend::replying(get_hit(b"3"));
        let route = FailoverRoute::new(
            vec![dest(a.clone()), dest(b.clone()), dest(c.clone())],
            FailoverErrors::default(),
            Box::new(InOrderPolicy),
            1,
        );

        assert_eq!(execute(&route, get(b"k")).await.unwrap(), get_hit(b"3"));
    }
}
