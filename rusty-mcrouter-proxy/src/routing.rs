use bytes::Bytes;
use rusty_mcrouter_backend::error::SendError;
use rusty_mcrouter_core::{DynRoute, RouteContext, RouteError, RoutingState};
use rusty_mcrouter_protocol::reply::ErrorReply;
use rusty_mcrouter_protocol::{Reply, Request};

use std::rc::Rc;

pub(crate) async fn route_request(
    route: Rc<dyn DynRoute>,
    routing_state: Rc<RoutingState>,
    request: Request,
) -> Reply {
    let context = routing_state.context();
    let result = route.route_dyn(&context, request).await;
    complete_route(context, result)
}

pub(crate) fn complete_route(
    context: RouteContext<'_>,
    result: Result<Reply, RouteError>,
) -> Reply {
    context.finish(&result);
    result.unwrap_or_else(route_error_reply)
}

fn route_error_reply(error: RouteError) -> Reply {
    let message = match error {
        // mcrouter's TkoReply wording (DestinationRoute.h:177-179).
        RouteError::Backend(SendError::Tko { reason }) => {
            Bytes::from(format!("Server unavailable. Reason: {reason:?}"))
        }
        _ => Bytes::from_static(b"backend unavailable"),
    };

    Reply::Error(ErrorReply::Server(Some(message)))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use rusty_mcrouter_backend::classify::ResultCode;
    use rusty_mcrouter_backend::destination;
    use rusty_mcrouter_backend::error::RequestError;
    use rusty_mcrouter_backend::test_support::{MockBackend, MockBackendFactory};
    use rusty_mcrouter_config::parse;
    use rusty_mcrouter_core::{
        build_route, DestinationRoute, Route, RoutingMetricsLayout, RoutingMetricsShard,
    };
    use rusty_mcrouter_observability_primitives::test_support::noop_sink;
    use rusty_mcrouter_protocol::test_support::{get, server_error};

    async fn boundary_reply(error: SendError) -> Reply {
        let route = DestinationRoute::new(MockBackend::failing(error)).into_dyn();
        let layout = RoutingMetricsLayout::empty();
        let state = RoutingState::new(RoutingMetricsShard::new(layout), noop_sink());
        route_request(route, state, get(b"foo")).await
    }

    #[tokio::test]
    async fn unrecovered_timeout_becomes_server_error_at_boundary() {
        let reply = boundary_reply(SendError::Request(RequestError::Timeout { sent: true })).await;
        assert_eq!(reply, server_error(b"backend unavailable"));
    }

    #[tokio::test]
    async fn tko_fast_fail_reports_the_marking_reason() {
        let reply = boundary_reply(SendError::Tko {
            reason: ResultCode::Timeout,
        })
        .await;
        assert_eq!(reply, server_error(b"Server unavailable. Reason: Timeout"));
    }

    #[tokio::test]
    async fn routed_request_finishes_pool_metrics() {
        let config =
            parse(r#"{"pools": {"pool": {"servers": ["unused:1"]}}, "route": "PoolRoute|pool"}"#)
                .unwrap();
        let pool = config.pool_id("pool").unwrap();
        let layout = RoutingMetricsLayout::new(&config);
        let metrics = RoutingMetricsShard::new(layout);
        let state = RoutingState::new(Arc::clone(&metrics), noop_sink());
        let route = build_route(
            &config,
            &MockBackendFactory::new(),
            &destination::DestinationConfig::default(),
            state.layout(),
        )
        .unwrap();

        route_request(route, state, get(b"foo")).await;

        assert_eq!(metrics.pool(pool).requests.load(), 1);
        assert_eq!(metrics.pool(pool).completed_requests.load(), 1);
        assert_eq!(metrics.pool(pool).final_errors.load(), 0);
    }
}
