use bytes::Bytes;
use rusty_mcrouter_protocol::{Reply, Request};
use tokio::sync::{mpsc, oneshot};

use crate::proxy::message::{ProxyMessage, ProxyRequest};

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
            return Reply::ServerError(Bytes::from_static(b"proxy unavailable"));
        }

        reply_rx
            .await
            .unwrap_or_else(|_| Reply::ServerError(Bytes::from_static(b"proxy dropped request")))
    }

    pub async fn shutdown(&self) {
        let _ = self.tx.send(ProxyMessage::Shutdown).await;
    }
}
