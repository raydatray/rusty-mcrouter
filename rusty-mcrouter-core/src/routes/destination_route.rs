use rusty_mcrouter_net::Backend;
use rusty_mcrouter_protocol::{Reply, Request};

use super::{Result, Route, RouteError};

pub struct DestinationRoute<B: Backend> {
    backend: B,
}

impl<B: Backend> DestinationRoute<B> {
    pub fn new(backend: B) -> Self {
        Self { backend }
    }
}

impl<B: Backend> Route for DestinationRoute<B> {
    async fn route(&self, req: Request) -> Result<Reply> {
        self.backend.send(req).await.map_err(RouteError::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{req_get, req_store};
    use bytes::Bytes;
    use rusty_mcrouter_net::testing::MockBackend;
    use rusty_mcrouter_net::{NetError, TimeoutPhase};
    use rusty_mcrouter_protocol::reply::{ErrorReply, GetHit, GetReply, StoreReply, StoreResult};
    use std::sync::Arc;

    #[tokio::test]
    async fn forwards_request_to_backend_and_returns_reply() {
        let backend = MockBackend::replying(Reply::Get(GetReply::Hit(GetHit {
            value: Some(Bytes::from_static(b"bar")),
            ..GetHit::default()
        })));
        let route = DestinationRoute::<MockBackend>::new(backend.clone());

        let reply = route.route(req_get(b"foo")).await.unwrap();
        let Reply::Get(GetReply::Hit(hit)) = reply else {
            panic!("expected a get hit");
        };
        assert_eq!(hit.value.as_deref(), Some(b"bar".as_slice()));
        assert_eq!(backend.received(), vec![req_get(b"foo")]);
    }

    #[tokio::test]
    async fn returns_miss_reply_on_miss() {
        let route = DestinationRoute::<MockBackend>::new(MockBackend::miss());
        let reply = route.route(req_get(b"foo")).await.unwrap();
        assert_eq!(reply, Reply::Get(GetReply::Miss));
    }

    #[tokio::test]
    async fn propagates_backend_protocol_error() {
        let backend = MockBackend::failing(NetError::Desync("bad reply"));
        let route = DestinationRoute::<MockBackend>::new(backend);

        let result = route.route(req_get(b"foo")).await;
        assert!(matches!(result, Err(RouteError::Backend(_))));
    }

    #[tokio::test]
    async fn propagates_backend_timeout_as_route_error() {
        let backend = MockBackend::failing(NetError::Timeout {
            phase: TimeoutPhase::Reply,
        });
        let route = DestinationRoute::<MockBackend>::new(backend);

        let result = route.route(req_get(b"foo")).await;
        assert!(matches!(
            result,
            Err(RouteError::Backend(NetError::Timeout {
                phase: TimeoutPhase::Reply
            }))
        ));
    }

    #[tokio::test]
    async fn forwards_store_request_and_returns_success() {
        let stored = Reply::Store(StoreReply::Success(StoreResult::default()));
        let backend = MockBackend::replying(stored.clone());
        let route = DestinationRoute::<MockBackend>::new(backend.clone());

        let req = req_store(b"foo", b"bar");
        let reply = route.route(req.clone()).await.unwrap();
        assert_eq!(reply, stored);
        assert_eq!(backend.received(), vec![req]);
    }

    #[tokio::test]
    async fn propagates_backend_server_error_as_reply() {
        let server_error = Reply::Error(ErrorReply::Server(Some(Bytes::from_static(b"oom"))));
        let route =
            DestinationRoute::<MockBackend>::new(MockBackend::replying(server_error.clone()));
        let reply = route.route(req_get(b"foo")).await.unwrap();
        assert_eq!(reply, server_error);
    }

    #[tokio::test]
    async fn can_be_shared_across_tasks_via_arc() {
        let route = Arc::new(DestinationRoute::<MockBackend>::new(MockBackend::miss()));

        let route_clone = Arc::clone(&route);
        let result = tokio::spawn(async move { route_clone.route(req_get(b"foo")).await })
            .await
            .unwrap();

        assert_eq!(result.unwrap(), Reply::Get(GetReply::Miss));
    }

    #[tokio::test]
    async fn serves_concurrent_requests_without_locking() {
        let backend = MockBackend::miss();
        let route = Arc::new(DestinationRoute::<MockBackend>::new(backend.clone()));

        let r1 = {
            let route = Arc::clone(&route);
            tokio::spawn(async move { route.route(req_get(b"a")).await })
        };
        let r2 = {
            let route = Arc::clone(&route);
            tokio::spawn(async move { route.route(req_get(b"b")).await })
        };

        let (a, b) = tokio::join!(r1, r2);
        assert_eq!(a.unwrap().unwrap(), Reply::Get(GetReply::Miss));
        assert_eq!(b.unwrap().unwrap(), Reply::Get(GetReply::Miss));
        assert_eq!(backend.received().len(), 2);
    }
}
