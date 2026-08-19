use rusty_mcrouter_backend::Backend;
use rusty_mcrouter_protocol::{Reply, Request};
use tokio::time::Instant;

use super::{Result, Route, RouteError};
use crate::RouteContext;

pub struct DestinationRoute<B: Backend> {
    backend: B,
    pool_index: Option<usize>,
}

impl<B: Backend> DestinationRoute<B> {
    pub fn new(backend: B) -> Self {
        Self {
            backend,
            pool_index: None,
        }
    }

    pub(crate) fn for_pool(backend: B, pool_index: usize) -> Self {
        Self {
            backend,
            pool_index: Some(pool_index),
        }
    }
}

impl<B: Backend> Route for DestinationRoute<B> {
    async fn route(&self, context: &RouteContext<'_>, request: Request) -> Result<Reply> {
        let started = Instant::now();
        let result = match self.backend.prepare_send(request) {
            Err(error) => Err(error),
            Ok(prepared) => {
                if let Some(pool_index) = self.pool_index {
                    context.select_pool(pool_index);
                }

                let result = prepared.await;

                if let Some(pool_index) = self.pool_index {
                    context.metrics().pools[pool_index]
                        .duration_us_sum
                        .add(started.elapsed().as_micros() as u64);
                }

                result
            }
        };

        if let Some(pool_index) = self.pool_index {
            context.metrics().pools[pool_index].requests.inc();
        }

        result.map_err(RouteError::from)
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use bytes::Bytes;
    use rusty_mcrouter_backend::error::{ProtocolError, RequestError, SendError};
    use rusty_mcrouter_backend::test_support::MockBackend;
    use rusty_mcrouter_protocol::reply::{ErrorReply, GetHit, GetReply, StoreReply, StoreResult};
    use rusty_mcrouter_protocol::test_support::{get, store};

    use crate::context::test_routing_state;
    use crate::{RoutingMetricsLayout, RoutingMetricsShard, RoutingState};

    struct DelayedBackend;

    impl Backend for DelayedBackend {
        fn prepare_send(
            &self,
            _request: Request,
        ) -> std::result::Result<
            impl Future<Output = std::result::Result<Reply, SendError>> + '_,
            SendError,
        > {
            Ok(async {
                tokio::time::sleep(Duration::from_millis(2)).await;
                Ok(Reply::Get(GetReply::Miss))
            })
        }
    }

    async fn execute<B: Backend>(route: &DestinationRoute<B>, request: Request) -> Result<Reply> {
        let state = test_routing_state();
        let context = state.context();
        route.route(&context, request).await
    }

    #[tokio::test]
    async fn attributed_send_records_attempt_and_final_metrics() {
        let layout = RoutingMetricsLayout::new(["pool".to_string()]);
        let metrics = RoutingMetricsShard::new(layout);
        let state = RoutingState::new(Arc::clone(&metrics));
        let route = DestinationRoute::for_pool(MockBackend::miss(), 0);
        let context = state.context();

        let result = route.route(&context, get(b"foo")).await;
        context.finish(&result);

        assert_eq!(metrics.pools[0].requests.load(), 1);
        assert_eq!(metrics.pools[0].completed_requests.load(), 1);
        assert_eq!(metrics.pools[0].final_errors.load(), 0);
    }

    #[tokio::test]
    async fn sendable_attempt_records_elapsed_duration() {
        let layout = RoutingMetricsLayout::new(["pool".to_string()]);
        let metrics = RoutingMetricsShard::new(layout);
        let state = RoutingState::new(Arc::clone(&metrics));
        let route = DestinationRoute::for_pool(DelayedBackend, 0);
        let context = state.context();

        let result = route.route(&context, get(b"foo")).await;
        context.finish(&result);

        assert!(metrics.pools[0].duration_us_sum.load() >= 1_000);
        assert!(metrics.pools[0].total_duration_us_sum.load() >= 1_000);
    }

    #[tokio::test]
    async fn attributed_error_counts_as_final_error() {
        let layout = RoutingMetricsLayout::new(["pool".to_string()]);
        let metrics = RoutingMetricsShard::new(layout);
        let state = RoutingState::new(Arc::clone(&metrics));
        let route = DestinationRoute::for_pool(
            MockBackend::replying(Reply::Error(ErrorReply::Server(None))),
            0,
        );
        let context = state.context();

        let result = route.route(&context, get(b"foo")).await;
        context.finish(&result);

        assert_eq!(metrics.pools[0].requests.load(), 1);
        assert_eq!(metrics.pools[0].completed_requests.load(), 1);
        assert_eq!(metrics.pools[0].final_errors.load(), 1);
    }

    #[tokio::test]
    async fn tko_attempt_records_no_duration_or_final_attribution() {
        let layout = RoutingMetricsLayout::new(["pool".to_string()]);
        let metrics = RoutingMetricsShard::new(layout);
        let state = RoutingState::new(Arc::clone(&metrics));
        let route = DestinationRoute::for_pool(
            MockBackend::failing(SendError::Tko {
                reason: rusty_mcrouter_backend::classify::ResultCode::Timeout,
            }),
            0,
        );
        let context = state.context();

        let result = route.route(&context, get(b"foo")).await;
        context.finish(&result);

        assert!(result.is_err());
        assert_eq!(metrics.pools[0].requests.load(), 1);
        assert_eq!(metrics.pools[0].duration_us_sum.load(), 0);
        assert_eq!(metrics.pools[0].completed_requests.load(), 0);
        assert_eq!(metrics.pools[0].final_errors.load(), 0);
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
