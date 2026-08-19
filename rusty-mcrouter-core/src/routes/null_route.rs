use rusty_mcrouter_protocol::reply::{
    ArithmeticReply, ArithmeticResult, DebugReply, DeleteReply, GetReply, StoreReply, StoreResult,
};
use rusty_mcrouter_protocol::{Reply, Request};

use crate::routes::{Result, Route};
use crate::RouteContext;

pub struct NullRoute;

impl Route for NullRoute {
    async fn route(&self, context: &RouteContext<'_>, request: Request) -> Result<Reply> {
        context.metrics().dev_null_requests.inc();

        Ok(match request {
            Request::Get(_) => Reply::Get(GetReply::Miss),
            Request::Store(request) => Reply::Store(StoreReply::Success(StoreResult {
                cas: Some(0),
                size: Some(request.value.len() as u64),
            })),
            Request::Delete(_) => Reply::Delete(DeleteReply::Success),
            Request::Arithmetic(_) => {
                Reply::Arithmetic(ArithmeticReply::NotFound(ArithmeticResult::default()))
            }
            Request::Debug(_) => Reply::Debug(DebugReply::Miss),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use rusty_mcrouter_protocol::test_support::{delete, get, request, store};

    use crate::context::test_routing_state;
    use crate::{RoutingMetricsLayout, RoutingMetricsShard, RoutingState};

    async fn execute(request: Request) -> Result<Reply> {
        let state = test_routing_state();
        let context = state.context();
        NullRoute.route(&context, request).await
    }

    #[tokio::test]
    async fn returns_miss_for_get() {
        let reply = execute(get(b"foo")).await.unwrap();
        assert_eq!(reply, Reply::Get(GetReply::Miss));
    }

    #[tokio::test]
    async fn returns_synthesized_success_for_store() {
        let reply = execute(store(b"k", b"value")).await.unwrap();
        assert_eq!(
            reply,
            Reply::Store(StoreReply::Success(StoreResult {
                cas: Some(0),
                size: Some(5),
            }))
        );
    }

    #[tokio::test]
    async fn returns_success_for_delete() {
        let reply = execute(delete(b"k")).await.unwrap();
        assert_eq!(reply, Reply::Delete(DeleteReply::Success));
    }

    #[tokio::test]
    async fn returns_not_found_for_arithmetic() {
        let reply = execute(request(b"ma k v\r\n")).await.unwrap();
        assert_eq!(
            reply,
            Reply::Arithmetic(ArithmeticReply::NotFound(ArithmeticResult::default()))
        );
    }

    #[tokio::test]
    async fn returns_miss_for_debug() {
        let reply = execute(request(b"me k\r\n")).await.unwrap();
        assert_eq!(reply, Reply::Debug(DebugReply::Miss));
    }

    #[tokio::test]
    async fn counts_each_invocation() {
        let layout = RoutingMetricsLayout::new(Vec::<String>::new());
        let metrics = RoutingMetricsShard::new(layout);
        let state = RoutingState::new(Arc::clone(&metrics));

        let first = state.context();
        NullRoute.route(&first, get(b"a")).await.unwrap();
        let second = state.context();
        NullRoute.route(&second, get(b"b")).await.unwrap();

        assert_eq!(metrics.dev_null_requests.load(), 2);
    }
}
