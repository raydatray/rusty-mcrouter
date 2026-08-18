use bytes::Bytes;
use rusty_mcrouter_protocol::reply::ErrorReply;
use rusty_mcrouter_protocol::{Reply, Request};
use tokio::sync::{mpsc, oneshot};

use crate::message::{ProxyMessage, ProxyRequest};

#[derive(Clone)]
pub struct ProxyHandle {
    id: usize,
    tx: mpsc::Sender<ProxyMessage>,
}

impl ProxyHandle {
    pub fn new(id: usize, tx: mpsc::Sender<ProxyMessage>) -> Self {
        Self { id, tx }
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub async fn send_request(&self, request: Request) -> Reply {
        let (reply_tx, reply_rx) = oneshot::channel();

        if self
            .tx
            .send(ProxyMessage::Request(ProxyRequest { request, reply_tx }))
            .await
            .is_err()
        {
            return server_error(b"proxy unavailable");
        }

        reply_rx
            .await
            .unwrap_or_else(|_| server_error(b"proxy dropped request"))
    }

    // todo - graceful shutdown: unused until the binary handles signals
    #[allow(dead_code)]
    pub async fn shutdown(&self) {
        let _ = self.tx.send(ProxyMessage::Shutdown).await;
    }
}

fn server_error(message: &'static [u8]) -> Reply {
    Reply::Error(ErrorReply::Server(Some(Bytes::from_static(message))))
}
