use std::rc::Rc;

use rusty_mcrouter_protocol::{Reply, Request};

use crate::failover::{FailoverErrors, FailoverPolicy};

use super::{DynRoute, Result, Route};

pub struct FailoverRoute {
    children: Vec<Rc<dyn DynRoute>>,
    errors: FailoverErrors,
    policy: Box<dyn FailoverPolicy>,
}

impl FailoverRoute {
    pub fn new(
        children: Vec<Rc<dyn DynRoute>>,
        errors: FailoverErrors,
        policy: Box<dyn FailoverPolicy>,
    ) -> Option<Self> {
        if children.is_empty() {
            return None;
        }
        Some(Self {
            children,
            errors,
            policy,
        })
    }
}

impl Route for FailoverRoute {
    async fn route(&self, req: Request) -> Result<Reply> {
        let primary = self.children[0].route_dyn(req.clone()).await;
        let primary_failed = self.errors.should_failover(&req, &primary);
        self.policy.record_outcome(0, primary_failed);
        if !primary_failed {
            return primary;
        }

        let mut last = primary;
        for idx in self.policy.failover_order(&req, self.children.len()) {
            let Some(child) = self.children.get(idx) else {
                continue;
            };
            let reply = child.route_dyn(req.clone()).await;
            let failed = self.errors.should_failover(&req, &reply);
            self.policy.record_outcome(idx, failed);
            if !failed {
                return reply;
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
    use rusty_mcrouter_net::testing::MockBackend;
    use rusty_mcrouter_net::{NetError, TimeoutPhase};

    fn get() -> Request {
        Request::Get {
            key: Bytes::from_static(b"k"),
        }
    }

    fn set() -> Request {
        Request::Set {
            key: Bytes::from_static(b"k"),
            flags: 0,
            exptime: 0,
            data: Bytes::from_static(b"v"),
        }
    }

    fn dest(backend: MockBackend) -> Rc<dyn DynRoute> {
        DestinationRoute::new(backend).into_dyn()
    }

    fn timeout() -> NetError {
        NetError::Timeout {
            phase: TimeoutPhase::Reply,
        }
    }

    fn in_order(children: Vec<Rc<dyn DynRoute>>) -> FailoverRoute {
        FailoverRoute::new(children, FailoverErrors::default(), Box::new(InOrderPolicy)).unwrap()
    }

    #[tokio::test]
    async fn transport_errors_fail_over_to_a_healthy_backup() {
        for err in [
            NetError::Timeout {
                phase: TimeoutPhase::Reply,
            },
            NetError::Timeout {
                phase: TimeoutPhase::Connect,
            },
            NetError::Io(std::io::Error::from(std::io::ErrorKind::BrokenPipe)),
            NetError::ClientClosed,
        ] {
            let primary = MockBackend::failing(err);
            let backup = MockBackend::replying(Reply::Numeric(1));
            let route = in_order(vec![dest(primary.clone()), dest(backup.clone())]);

            assert_eq!(route.route(get()).await.unwrap(), Reply::Numeric(1));
            assert_eq!(primary.received().len(), 1);
            assert_eq!(backup.received().len(), 1);
        }
    }

    #[tokio::test]
    async fn server_error_reply_fails_over() {
        let primary = MockBackend::replying(Reply::ServerError(Bytes::from_static(b"boom")));
        let backup = MockBackend::replying(Reply::Numeric(1));
        let route = in_order(vec![dest(primary.clone()), dest(backup.clone())]);

        assert_eq!(route.route(get()).await.unwrap(), Reply::Numeric(1));
        assert_eq!(backup.received().len(), 1);
    }

    #[tokio::test]
    async fn a_miss_does_not_fail_over() {
        let primary = MockBackend::replying(Reply::NotFound);
        let backup = MockBackend::replying(Reply::Numeric(1));
        let route = in_order(vec![dest(primary.clone()), dest(backup.clone())]);

        assert_eq!(route.route(get()).await.unwrap(), Reply::NotFound);
        assert!(backup.received().is_empty());
    }

    #[tokio::test]
    async fn first_success_wins_and_later_children_are_untouched() {
        let a = MockBackend::failing(timeout());
        let b = MockBackend::replying(Reply::Numeric(2));
        let c = MockBackend::replying(Reply::Numeric(3));
        let route = in_order(vec![dest(a.clone()), dest(b.clone()), dest(c.clone())]);

        assert_eq!(route.route(get()).await.unwrap(), Reply::Numeric(2));
        assert_eq!(a.received().len(), 1);
        assert_eq!(b.received().len(), 1);
        assert!(c.received().is_empty());
    }

    #[tokio::test]
    async fn all_children_failing_returns_the_last_result() {
        let route = in_order(vec![
            dest(MockBackend::failing(timeout())),
            dest(MockBackend::replying(Reply::ServerError(Bytes::from_static(b"x")))),
        ]);
        assert_eq!(
            route.route(get()).await.unwrap(),
            Reply::ServerError(Bytes::from_static(b"x"))
        );

        let route = in_order(vec![
            dest(MockBackend::failing(timeout())),
            dest(MockBackend::failing(timeout())),
        ]);
        assert!(matches!(
            route.route(get()).await,
            Err(RouteError::Backend(NetError::Timeout { .. }))
        ));
    }

    #[tokio::test]
    async fn single_child_has_no_backup() {
        let only = MockBackend::replying(Reply::Numeric(1));
        let route = in_order(vec![dest(only.clone())]);
        assert_eq!(route.route(get()).await.unwrap(), Reply::Numeric(1));
        assert_eq!(only.received().len(), 1);

        let route = in_order(vec![dest(MockBackend::failing(timeout()))]);
        assert!(matches!(
            route.route(get()).await,
            Err(RouteError::Backend(NetError::Timeout { .. }))
        ));
    }

    #[test]
    fn empty_children_is_rejected() {
        let route = FailoverRoute::new(vec![], FailoverErrors::default(), Box::new(InOrderPolicy));
        assert!(route.is_none());
    }

    #[tokio::test]
    async fn per_op_updates_empty_blocks_write_failover() {
        let primary = MockBackend::failing(timeout());
        let backup = MockBackend::replying(Reply::Stored);
        let route = FailoverRoute::new(
            vec![dest(primary.clone()), dest(backup.clone())],
            FailoverErrors::new(None, Some(vec![]), None),
            Box::new(InOrderPolicy),
        )
        .unwrap();

        assert!(matches!(
            route.route(set()).await,
            Err(RouteError::Backend(NetError::Timeout { .. }))
        ));
        assert!(backup.received().is_empty());
    }
}
