use std::rc::Rc;

use bytes::Bytes;
use rusty_mcrouter_core::{DynRoute, RouteError};
use rusty_mcrouter_net::error::SendError;
use rusty_mcrouter_protocol::reply::ErrorReply;
use rusty_mcrouter_protocol::Reply;
use tokio::sync::mpsc;

use crate::proxy::message::{ProxyMessage, ProxyRequest};

pub struct Proxy {
    // todo - stats/logging will read this; kept for the thread-mode work
    #[allow(dead_code)]
    pub id: usize,
    pub route: Rc<dyn DynRoute>,
    pub rx: mpsc::Receiver<ProxyMessage>,
}

impl Proxy {
    pub async fn run(mut self) {
        while let Some(msg) = self.rx.recv().await {
            match msg {
                ProxyMessage::Request(req) => {
                    Self::spawn_request(Rc::clone(&self.route), req);
                }
                ProxyMessage::Shutdown => break,
            }
        }
    }

    pub fn spawn_request(route: Rc<dyn DynRoute>, req: ProxyRequest) {
        tokio::task::spawn_local(async move {
            let reply = route.route_dyn(req.request).await.unwrap_or_else(|err| {
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
    use super::*;
    use rusty_mcrouter_core::{DestinationRoute, Route};
    use rusty_mcrouter_net::classify::ResultCode;
    use rusty_mcrouter_net::error::RequestError;
    use rusty_mcrouter_net::test_support::MockBackend;
    use rusty_mcrouter_protocol::test_support::get;
    use tokio::sync::oneshot;
    use tokio::task::LocalSet;

    async fn boundary_reply(err: SendError) -> Reply {
        let route = DestinationRoute::new(MockBackend::failing(err)).into_dyn();

        let (reply_tx, reply_rx) = oneshot::channel();
        let req = ProxyRequest {
            request: get(b"foo"),
            reply_tx,
        };

        LocalSet::new()
            .run_until(async move {
                Proxy::spawn_request(route, req);
                reply_rx.await.unwrap()
            })
            .await
    }

    #[tokio::test]
    async fn unrecovered_timeout_becomes_server_error_at_boundary() {
        let reply =
            boundary_reply(SendError::Request(RequestError::Timeout { sent: true })).await;
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
}
