use anyhow::Context;
use bytes::Bytes;
use rusty_mcrouter_protocol::reply::ErrorReply;
use rusty_mcrouter_protocol::{Reply, Request};
use tokio::sync::{mpsc, oneshot};

use crate::error::Result;
use crate::{FrontendError, ProxyCommand, ProxyInbox, ProxyRequest};

const WORK_CAPACITY: usize = 1024;
const REQUEST_CAPACITY: usize = 1024;
const COMMAND_CAPACITY: usize = 16;

#[derive(Clone)]
pub struct ProxyHandle {
    id: usize,
    request_tx: mpsc::Sender<ProxyRequest>,
    command_tx: mpsc::Sender<ProxyCommand>,
    work_tx: mpsc::Sender<std::net::TcpStream>,
}

impl ProxyHandle {
    pub fn allocate(id: usize) -> (ProxyHandle, ProxyInbox) {
        let (request_tx, request_rx) = mpsc::channel(REQUEST_CAPACITY);
        let (command_tx, command_rx) = mpsc::channel(COMMAND_CAPACITY);
        let (work_tx, work_rx) = mpsc::channel(WORK_CAPACITY);
        (
            ProxyHandle {
                id,
                request_tx,
                command_tx,
                work_tx,
            },
            ProxyInbox {
                work_rx,
                request_rx,
                command_rx,
            },
        )
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

    pub async fn send_connection(&self, stream: std::net::TcpStream) -> Result<()> {
        self.work_tx
            .send(stream)
            .await
            .map_err(|_| FrontendError::WorkerClosed { worker: self.id })
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
