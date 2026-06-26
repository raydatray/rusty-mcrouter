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
    use crate::test_support::req_get;
    use bytes::Bytes;
    use rusty_mcrouter_net::testing::MockBackend;
    use rusty_mcrouter_net::NetError;
    use rusty_mcrouter_protocol::{ProtocolError, Value};
    use std::sync::Arc;

    #[tokio::test]
    async fn forwards_request_to_backend_and_returns_reply() {
        let backend = MockBackend::replying(Reply::Get {
            hits: vec![Value {
                key: Bytes::from_static(b"foo"),
                flags: 0,
                data: Bytes::from_static(b"bar"),
            }],
        });
        let route = DestinationRoute::<MockBackend>::new(backend.clone());

        let reply = route.route(req_get(b"foo")).await.unwrap();
        let Reply::Get { hits } = reply else {
            panic!("expected Reply::Get");
        };
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].key.as_ref(), b"foo");
        assert_eq!(hits[0].data.as_ref(), b"bar");
        assert_eq!(backend.received(), vec![req_get(b"foo")]);
    }

    #[tokio::test]
    async fn returns_empty_reply_on_miss() {
        let route = DestinationRoute::<MockBackend>::new(MockBackend::miss());
        let reply = route.route(req_get(b"foo")).await.unwrap();
        assert_eq!(reply, Reply::Get { hits: vec![] });
    }

    #[tokio::test]
    async fn propagates_backend_protocol_error() {
        let backend =
            MockBackend::failing(NetError::Protocol(ProtocolError::Malformed("bad reply")));
        let route = DestinationRoute::<MockBackend>::new(backend);

        let result = route.route(req_get(b"foo")).await;
        assert!(matches!(result, Err(RouteError::Backend(_))));
    }

    #[tokio::test]
    async fn forwards_set_request_and_returns_stored() {
        let backend = MockBackend::replying(Reply::Stored);
        let route = DestinationRoute::<MockBackend>::new(backend.clone());

        let req = Request::Set {
            key: Bytes::from_static(b"foo"),
            flags: 0,
            exptime: 0,
            data: Bytes::from_static(b"bar"),
        };
        let reply = route.route(req.clone()).await.unwrap();
        assert_eq!(reply, Reply::Stored);
        assert_eq!(backend.received(), vec![req]);
    }

    #[tokio::test]
    async fn propagates_backend_server_error_as_reply() {
        let route = DestinationRoute::<MockBackend>::new(MockBackend::replying(
            Reply::ServerError(Bytes::from_static(b"oom")),
        ));
        let reply = route.route(req_get(b"foo")).await.unwrap();
        assert_eq!(reply, Reply::ServerError(Bytes::from_static(b"oom")));
    }

    #[tokio::test]
    async fn can_be_shared_across_tasks_via_arc() {
        let route = Arc::new(DestinationRoute::<MockBackend>::new(MockBackend::miss()));

        let route_clone = Arc::clone(&route);
        let result = tokio::spawn(async move { route_clone.route(req_get(b"foo")).await })
            .await
            .unwrap();

        assert_eq!(result.unwrap(), Reply::Get { hits: vec![] });
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
        assert_eq!(a.unwrap().unwrap(), Reply::Get { hits: vec![] });
        assert_eq!(b.unwrap().unwrap(), Reply::Get { hits: vec![] });
        assert_eq!(backend.received().len(), 2);
    }
}
