use std::rc::Rc;

use bytes::Bytes;
use rusty_mcrouter_core::DynRoute;
use rusty_mcrouter_protocol::Reply;
use tokio::sync::mpsc;

use crate::proxy::message::{ProxyMessage, ProxyRequest};

pub struct Proxy {
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
            let reply = route
                .route_dyn(req.request)
                .await
                .unwrap_or_else(|_| Reply::ServerError(Bytes::from_static(b"backend unavailable")));

            let _ = req.reply_tx.send(reply);
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusty_mcrouter_core::{DestinationRoute, Route};
    use rusty_mcrouter_net::testing::MockBackend;
    use rusty_mcrouter_net::{NetError, TimeoutPhase};
    use rusty_mcrouter_protocol::Request;
    use tokio::sync::oneshot;
    use tokio::task::LocalSet;

    #[tokio::test]
    async fn unrecovered_timeout_becomes_server_error_at_boundary() {
        let route = DestinationRoute::new(MockBackend::failing(NetError::Timeout {
            phase: TimeoutPhase::Reply,
        }))
        .into_dyn();

        let (reply_tx, reply_rx) = oneshot::channel();
        let req = ProxyRequest {
            request: Request::Get {
                key: Bytes::from_static(b"foo"),
            },
            reply_tx,
        };

        let reply = LocalSet::new()
            .run_until(async move {
                Proxy::spawn_request(route, req);
                reply_rx.await.unwrap()
            })
            .await;

        assert_eq!(
            reply,
            Reply::ServerError(Bytes::from_static(b"backend unavailable"))
        );
    }
}
