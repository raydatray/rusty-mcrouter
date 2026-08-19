use rusty_mcrouter_backend::Backend;
use rusty_mcrouter_protocol::{Reply, Request};

use super::{Result, Route, RouteError};
use crate::RouteContext;

pub struct DestinationRoute<B: Backend> {
    backend: B,
}

impl<B: Backend> DestinationRoute<B> {
    pub fn new(backend: B) -> Self {
        Self { backend }
    }
}

impl<B: Backend> Route for DestinationRoute<B> {
    async fn route(&self, _context: &RouteContext<'_>, request: Request) -> Result<Reply> {
        self.backend.send(request).await.map_err(RouteError::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use rusty_mcrouter_backend::error::{ProtocolError, RequestError, SendError};
    use rusty_mcrouter_backend::test_support::MockBackend;
    use rusty_mcrouter_protocol::reply::{ErrorReply, GetHit, GetReply, StoreReply, StoreResult};
    use rusty_mcrouter_protocol::test_support::{get, store};
    use std::sync::Arc;

    use crate::context::test_routing_state;

    async fn execute<B: Backend>(route: &DestinationRoute<B>, request: Request) -> Result<Reply> {
        let state = test_routing_state();
        let context = state.context();
        route.route(&context, request).await
    }

    #[tokio::test]
    async fn forwards_request_to_backend_and_returns_reply() {
        let backend = MockBackend::replying(Reply::Get(GetReply::Hit(GetHit {
            value: Some(Bytes::from_static(b"bar")),
            ..GetHit::default()
        })));
        let route = DestinationRoute::<MockBackend>::new(backend.clone());

        let reply = execute(&route, get(b"foo")).await.unwrap();
        let Reply::Get(GetReply::Hit(hit)) = reply else {
            panic!("expected a get hit");
        };
        assert_eq!(hit.value.as_deref(), Some(b"bar".as_slice()));
        assert_eq!(backend.received(), vec![get(b"foo")]);
    }

    #[tokio::test]
    async fn returns_miss_reply_on_miss() {
        let route = DestinationRoute::<MockBackend>::new(MockBackend::miss());
        let reply = execute(&route, get(b"foo")).await.unwrap();
        assert_eq!(reply, Reply::Get(GetReply::Miss));
    }

    #[tokio::test]
    async fn propagates_backend_protocol_error() {
        let backend = MockBackend::failing(SendError::Protocol(ProtocolError::Desync("bad reply")));
        let route = DestinationRoute::<MockBackend>::new(backend);

        let result = execute(&route, get(b"foo")).await;
        assert!(matches!(result, Err(RouteError::Backend(_))));
    }

    #[tokio::test]
    async fn propagates_backend_timeout_as_route_error() {
        let backend =
            MockBackend::failing(SendError::Request(RequestError::Timeout { sent: true }));
        let route = DestinationRoute::<MockBackend>::new(backend);

        let result = execute(&route, get(b"foo")).await;
        assert!(matches!(
            result,
            Err(RouteError::Backend(SendError::Request(
                RequestError::Timeout { sent: true }
            )))
        ));
    }

    #[tokio::test]
    async fn forwards_store_request_and_returns_success() {
        let stored = Reply::Store(StoreReply::Success(StoreResult::default()));
        let backend = MockBackend::replying(stored.clone());
        let route = DestinationRoute::<MockBackend>::new(backend.clone());

        let req = store(b"foo", b"bar");
        let reply = execute(&route, req.clone()).await.unwrap();
        assert_eq!(reply, stored);
        assert_eq!(backend.received(), vec![req]);
    }

    #[tokio::test]
    async fn propagates_backend_server_error_as_reply() {
        let server_error = Reply::Error(ErrorReply::Server(Some(Bytes::from_static(b"oom"))));
        let route =
            DestinationRoute::<MockBackend>::new(MockBackend::replying(server_error.clone()));
        let reply = execute(&route, get(b"foo")).await.unwrap();
        assert_eq!(reply, server_error);
    }

    #[tokio::test]
    async fn can_be_shared_across_local_tasks_via_arc() {
        let route = Arc::new(DestinationRoute::<MockBackend>::new(MockBackend::miss()));

        let result = tokio::task::LocalSet::new()
            .run_until(async move {
                let route_clone = Arc::clone(&route);
                tokio::task::spawn_local(async move { execute(&route_clone, get(b"foo")).await })
                    .await
            })
            .await
            .unwrap();

        assert_eq!(result.unwrap(), Reply::Get(GetReply::Miss));
    }

    #[tokio::test]
    async fn serves_concurrent_requests_without_locking() {
        let backend = MockBackend::miss();
        let route = Arc::new(DestinationRoute::<MockBackend>::new(backend.clone()));

        let (a, b) = tokio::task::LocalSet::new()
            .run_until(async move {
                let r1 = {
                    let route = Arc::clone(&route);
                    tokio::task::spawn_local(async move { execute(&route, get(b"a")).await })
                };
                let r2 = {
                    let route = Arc::clone(&route);
                    tokio::task::spawn_local(async move { execute(&route, get(b"b")).await })
                };
                tokio::join!(r1, r2)
            })
            .await;
        assert_eq!(a.unwrap().unwrap(), Reply::Get(GetReply::Miss));
        assert_eq!(b.unwrap().unwrap(), Reply::Get(GetReply::Miss));
        assert_eq!(backend.received().len(), 2);
    }
}
