use std::net::TcpStream;
use std::sync::Arc;

use anyhow::Context;
use rusty_mcrouter_config::ConfigDocument;
use rusty_mcrouter_core::BuildError;
use rusty_mcrouter_protocol::{Reply, Request};
use tokio::sync::{
    mpsc::{self, Sender},
    oneshot,
};

use crate::error::Result;
use crate::{ProxyCommand, ProxyError, ProxyInbox, ProxyRequest};

const WORK_CAPACITY: usize = 1024;
const REQUEST_CAPACITY: usize = 1024;
const COMMAND_CAPACITY: usize = 16;

#[derive(Clone)]
pub struct ProxyHandle {
    id: usize,
    request_tx: Sender<ProxyRequest>,
    command_tx: Sender<ProxyCommand>,
    work_tx: Sender<TcpStream>,
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
        rusty_mcrouter_frontend::send_request(&self.request_tx, request).await
    }

    pub fn request_sender(&self) -> Sender<ProxyRequest> {
        self.request_tx.clone()
    }

    pub async fn send_connection(&self, stream: TcpStream) -> Result<()> {
        self.work_tx
            .send(stream)
            .await
            .map_err(|_| ProxyError::WorkerClosed { worker: self.id })
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

    /// Returns without waiting for the outcome, so a caller can enqueue on
    /// every proxy before awaiting any of them.
    pub async fn begin_reconfigure(
        &self,
        generation: u64,
        config: Arc<ConfigDocument>,
    ) -> anyhow::Result<oneshot::Receiver<std::result::Result<(), BuildError>>> {
        let (applied, outcome) = oneshot::channel();
        self.command_tx
            .send(ProxyCommand::Reconfigure {
                generation,
                config,
                applied,
            })
            .await
            .context("proxy command channel closed")?;
        Ok(outcome)
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
