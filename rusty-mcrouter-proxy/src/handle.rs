use anyhow::Context;
use bytes::Bytes;
use rusty_mcrouter_protocol::reply::ErrorReply;
use rusty_mcrouter_protocol::{Reply, Request};
use tokio::sync::{mpsc, oneshot};

use crate::{ProxyCommand, ProxyRequest};

#[derive(Clone)]
pub struct ProxyHandle {
    id: usize,
    request_tx: mpsc::Sender<ProxyRequest>,
    command_tx: mpsc::Sender<ProxyCommand>,
}

impl ProxyHandle {
    pub fn new(
        id: usize,
        request_tx: mpsc::Sender<ProxyRequest>,
        command_tx: mpsc::Sender<ProxyCommand>,
    ) -> Self {
        Self {
            id,
            request_tx,
            command_tx,
        }
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub async fn send_request(&self, request: Request) -> Reply {
        let (reply_tx, reply_rx) = oneshot::channel();

        if self
            .request_tx
            .send(ProxyRequest { request, reply_tx })
            .await
            .is_err()
        {
            return server_error(b"proxy unavailable");
        }

        reply_rx
            .await
            .unwrap_or_else(|_| server_error(b"proxy dropped request"))
    }

    pub async fn shutdown(&self) -> anyhow::Result<()> {
        let (acknowledged, acknowledgement) = oneshot::channel();
        self.command_tx
            .send(ProxyCommand::Shutdown { acknowledged })
            .await
            .context("proxy command channel closed")?;
        acknowledgement
            .await
            .context("proxy exited before acknowledging shutdown")
    }

    pub fn shutdown_blocking(&self) -> anyhow::Result<()> {
        let (acknowledged, acknowledgement) = oneshot::channel();
        self.command_tx
            .blocking_send(ProxyCommand::Shutdown { acknowledged })
            .context("proxy command channel closed")?;
        acknowledgement
            .blocking_recv()
            .context("proxy exited before acknowledging shutdown")
    }
}

fn server_error(message: &'static [u8]) -> Reply {
    Reply::Error(ErrorReply::Server(Some(Bytes::from_static(message))))
}
