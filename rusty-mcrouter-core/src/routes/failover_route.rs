use std::rc::Rc;

use rusty_mcrouter_protocol::{Reply, Request};

use crate::failover::{route_code, FailoverErrors, FailoverPolicy};

use super::{DynRoute, Result, Route};

pub struct FailoverRoute {
    children: Vec<Rc<dyn DynRoute>>,
    errors: FailoverErrors,
    policy: Box<dyn FailoverPolicy>,
    /// Total attempts INCLUDING the primary. Lives here (not in the policy)
    /// so the route can grant free tries for TKO fast-fails.
    max_tries: usize,
}

impl FailoverRoute {
    pub fn new(
        children: Vec<Rc<dyn DynRoute>>,
        errors: FailoverErrors,
        policy: Box<dyn FailoverPolicy>,
        max_tries: usize,
    ) -> Option<Self> {
        if children.is_empty() {
            return None;
        }
        Some(Self {
            children,
            errors,
            policy,
            max_tries: max_tries.max(1),
        })
    }
}

/// mcrouter FailoverRoute.h:221-230 (verified): "We didn't do any work for
/// TKO or hard TKO. Don't count it as a try." A fast-failed child costs
/// nothing, so it must not consume failover budget.
fn is_free_try(result: &Result<Reply>) -> bool {
    route_code(result).is_some_and(|c| c.is_tko_or_hard_tko())
}

impl Route for FailoverRoute {
    async fn route(&self, req: Request) -> Result<Reply> {
        let mut tries = 0usize;

        let primary = self.children[0].route_dyn(req.clone()).await;
        let primary_failed = self.errors.should_failover(&req, &primary);
        self.policy.record_outcome(0, primary_failed);
        if !primary_failed {
            return primary;
        }
        if !is_free_try(&primary) {
            tries += 1;
        }

        let mut last = primary;
        for idx in self.policy.failover_order(&req, self.children.len()) {
            if tries >= self.max_tries {
                break;
            }
            let Some(child) = self.children.get(idx) else {
                continue;
            };
            let reply = child.route_dyn(req.clone()).await;
            let failed = self.errors.should_failover(&req, &reply);
            self.policy.record_outcome(idx, failed);
            if !failed {
                return reply;
            }
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
    use super::*;
    use crate::failover::InOrderPolicy;
    use crate::routes::{DestinationRoute, RouteError};
    use bytes::Bytes;
    use rusty_mcrouter_net::classify::ResultCode;
    use rusty_mcrouter_net::error::{ConnectError, LocalError, RequestError, SendError};
    use rusty_mcrouter_net::test_support::MockBackend;
    use rusty_mcrouter_protocol::reply::{
        ArithmeticReply, ArithmeticResult, ErrorReply, GetReply, StoreReply, StoreResult,
    };
    use rusty_mcrouter_protocol::test_support::{get, store};

    fn numeric(value: u64) -> Reply {
        Reply::Arithmetic(ArithmeticReply::Success(ArithmeticResult {
            value: Some(value),
            ..ArithmeticResult::default()
        }))
    }

    fn server_error(message: &'static [u8]) -> Reply {
        Reply::Error(ErrorReply::Server(Some(Bytes::from_static(message))))
    }

    fn dest(backend: MockBackend) -> Rc<dyn DynRoute> {
        DestinationRoute::new(backend).into_dyn()
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
        .unwrap()
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
            let backup = MockBackend::replying(numeric(1));
            let route = in_order(vec![dest(primary.clone()), dest(backup.clone())]);

            assert_eq!(route.route(get(b"k")).await.unwrap(), numeric(1));
            assert_eq!(primary.received().len(), 1);
            assert_eq!(backup.received().len(), 1);
        }
    }

    #[tokio::test]
    async fn server_error_reply_fails_over() {
        let primary = MockBackend::replying(server_error(b"boom"));
        let backup = MockBackend::replying(numeric(1));
        let route = in_order(vec![dest(primary.clone()), dest(backup.clone())]);

        assert_eq!(route.route(get(b"k")).await.unwrap(), numeric(1));
        assert_eq!(backup.received().len(), 1);
    }

    #[tokio::test]
    async fn a_miss_does_not_fail_over() {
        let primary = MockBackend::replying(Reply::Get(GetReply::Miss));
        let backup = MockBackend::replying(numeric(1));
        let route = in_order(vec![dest(primary.clone()), dest(backup.clone())]);

        assert_eq!(
            route.route(get(b"k")).await.unwrap(),
            Reply::Get(GetReply::Miss)
        );
        assert!(backup.received().is_empty());
    }

    #[tokio::test]
    async fn first_success_wins_and_later_children_are_untouched() {
        let a = MockBackend::failing(timeout());
        let b = MockBackend::replying(numeric(2));
        let c = MockBackend::replying(numeric(3));
        let route = in_order(vec![dest(a.clone()), dest(b.clone()), dest(c.clone())]);

        assert_eq!(route.route(get(b"k")).await.unwrap(), numeric(2));
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
        assert_eq!(route.route(get(b"k")).await.unwrap(), server_error(b"x"));

        let route = in_order(vec![
            dest(MockBackend::failing(timeout())),
            dest(MockBackend::failing(timeout())),
        ]);
        assert!(matches!(
            route.route(get(b"k")).await,
            Err(RouteError::Backend(SendError::Request(
                RequestError::Timeout { .. }
            )))
        ));
    }

    #[tokio::test]
    async fn single_child_has_no_backup() {
        let only = MockBackend::replying(numeric(1));
        let route = in_order(vec![dest(only.clone())]);
        assert_eq!(route.route(get(b"k")).await.unwrap(), numeric(1));
        assert_eq!(only.received().len(), 1);

        let route = in_order(vec![dest(MockBackend::failing(timeout()))]);
        assert!(matches!(
            route.route(get(b"k")).await,
            Err(RouteError::Backend(SendError::Request(
                RequestError::Timeout { .. }
            )))
        ));
    }

    #[test]
    fn empty_children_is_rejected() {
        let route = FailoverRoute::new(
            vec![],
            FailoverErrors::default(),
            Box::new(InOrderPolicy),
            1,
        );
        assert!(route.is_none());
    }

    #[tokio::test]
    async fn per_op_updates_empty_blocks_write_failover() {
        let primary = MockBackend::failing(timeout());
        let backup =
            MockBackend::replying(Reply::Store(StoreReply::Success(StoreResult::default())));
        let route = FailoverRoute::new(
            vec![dest(primary.clone()), dest(backup.clone())],
            FailoverErrors::new(None, Some(vec![]), None),
            Box::new(InOrderPolicy),
            2,
        )
        .unwrap();

        assert!(matches!(
            route.route(store(b"k", b"v")).await,
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
        let backup = MockBackend::replying(numeric(1));
        let route = FailoverRoute::new(
            vec![dest(primary.clone()), dest(backup.clone())],
            FailoverErrors::default(),
            Box::new(InOrderPolicy),
            1,
        )
        .unwrap();

        assert!(route.route(get(b"k")).await.is_err());
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
        let backup = MockBackend::replying(numeric(1));
        let route = FailoverRoute::new(
            vec![dest(primary.clone()), dest(backup.clone())],
            FailoverErrors::default(),
            Box::new(InOrderPolicy),
            1,
        )
        .unwrap();

        assert_eq!(route.route(get(b"k")).await.unwrap(), numeric(1));
    }

    /// Hard-TKO-class connect failures are also free (is_tko_or_hard_tko).
    #[tokio::test]
    async fn connect_errors_are_free_tries() {
        let a = MockBackend::failing(SendError::Connect(ConnectError::Failed(
            std::io::ErrorKind::ConnectionRefused,
        )));
        let b = MockBackend::failing(SendError::Connect(ConnectError::Timeout));
        let c = MockBackend::replying(numeric(3));
        let route = FailoverRoute::new(
            vec![dest(a.clone()), dest(b.clone()), dest(c.clone())],
            FailoverErrors::default(),
            Box::new(InOrderPolicy),
            1,
        )
        .unwrap();

        assert_eq!(route.route(get(b"k")).await.unwrap(), numeric(3));
    }
}
