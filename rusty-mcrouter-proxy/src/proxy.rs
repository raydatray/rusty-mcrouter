use std::rc::Rc;

use bytes::Bytes;
use rusty_mcrouter_backend::error::SendError;
use rusty_mcrouter_core::{DynRoute, RouteError, RoutingState};
use rusty_mcrouter_protocol::reply::ErrorReply;
use rusty_mcrouter_protocol::Reply;
use tokio::sync::mpsc;

use crate::message::{ProxyMessage, ProxyRequest};

pub struct Proxy {
    // todo - stats/logging will read this; kept for the thread-mode work
    #[allow(dead_code)]
    pub id: usize,
    pub route: Rc<dyn DynRoute>,
    pub routing_state: Rc<RoutingState>,
    pub rx: mpsc::Receiver<ProxyMessage>,
}

impl Proxy {
    pub async fn run(mut self) {
        while let Some(msg) = self.rx.recv().await {
            match msg {
                ProxyMessage::Request(req) => {
                    Self::spawn_request(
                        Rc::clone(&self.route),
                        Rc::clone(&self.routing_state),
                        req,
                    );
                }
                ProxyMessage::Shutdown => break,
            }
        }
    }

    pub fn spawn_request(
        route: Rc<dyn DynRoute>,
        routing_state: Rc<RoutingState>,
        req: ProxyRequest,
    ) {
        tokio::task::spawn_local(async move {
            let context = routing_state.context();
            let result = route.route_dyn(&context, req.request).await;
            context.finish(&result);
            let reply = result.unwrap_or_else(|err| {
                let msg = match &err {
                    // mcrouter's TkoReply wording (verified DestinationRoute.h:177-179)
                    RouteError::Backend(SendError::Tko { reason }) => {
                        Bytes::from(format!("Server unavailable. Reason: {reason:?}"))
                    }
                    _ => Bytes::from_static(b"backend unavailable"),
                };
                Reply::Error(ErrorReply::Server(Some(msg)))
            });

            let _ = req.reply_tx.send(reply);
        });
    }
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
    use rusty_mcrouter_protocol::test_support::get;
    use tokio::sync::oneshot;
    use tokio::task::LocalSet;

    async fn boundary_reply(err: SendError) -> Reply {
        let route = DestinationRoute::new(MockBackend::failing(err)).into_dyn();
        let layout = RoutingMetricsLayout::new(Vec::<String>::new());
        let routing_state = RoutingState::new(RoutingMetricsShard::new(layout));

        let (reply_tx, reply_rx) = oneshot::channel();
        let req = ProxyRequest {
            request: get(b"foo"),
            reply_tx,
        };

        LocalSet::new()
            .run_until(async move {
                Proxy::spawn_request(route, routing_state, req);
                reply_rx.await.unwrap()
            })
            .await
    }

    #[tokio::test]
    async fn unrecovered_timeout_becomes_server_error_at_boundary() {
        let reply = boundary_reply(SendError::Request(RequestError::Timeout { sent: true })).await;
        assert_eq!(
            reply,
            Reply::Error(ErrorReply::Server(Some(Bytes::from_static(
                b"backend unavailable"
            ))))
        );
    }

    #[tokio::test]
    async fn tko_fast_fail_reports_the_marking_reason() {
        let reply = boundary_reply(SendError::Tko {
            reason: ResultCode::Timeout,
        })
        .await;
        assert_eq!(
            reply,
            Reply::Error(ErrorReply::Server(Some(Bytes::from_static(
                b"Server unavailable. Reason: Timeout"
            ))))
        );
    }

    #[tokio::test]
    async fn queued_proxy_request_finishes_pool_metrics() {
        let config =
            parse(r#"{"pools": {"pool": {"servers": ["unused:1"]}}, "route": "PoolRoute|pool"}"#)
                .unwrap();
        let layout = RoutingMetricsLayout::new(["pool".to_string()]);
        let metrics = RoutingMetricsShard::new(layout);
        let routing_state = RoutingState::new(Arc::clone(&metrics));
        let route = build_route(
            &config,
            &MockBackendFactory::new(),
            &destination::Config::default(),
            routing_state.layout(),
        )
        .unwrap();
        let (reply_tx, reply_rx) = oneshot::channel();

        LocalSet::new()
            .run_until(async move {
                Proxy::spawn_request(
                    route,
                    routing_state,
                    ProxyRequest {
                        request: get(b"foo"),
                        reply_tx,
                    },
                );
                reply_rx.await.unwrap()
            })
            .await;

        assert_eq!(metrics.pools[0].requests.load(), 1);
        assert_eq!(metrics.pools[0].completed_requests.load(), 1);
        assert_eq!(metrics.pools[0].final_errors.load(), 0);
    }
}
