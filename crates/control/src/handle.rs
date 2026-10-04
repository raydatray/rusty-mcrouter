use anyhow::Context;
use tokio::sync::{mpsc, oneshot};

use crate::message::ControlCommand;

const CONTROL_COMMAND_CAPACITY: usize = 16;

pub struct ControlInbox {
    pub(crate) command_rx: mpsc::Receiver<ControlCommand>,
}

#[derive(Clone)]
pub struct ControlHandle {
    command_tx: mpsc::Sender<ControlCommand>,
}

impl ControlHandle {
    pub fn allocate() -> (Self, ControlInbox) {
        let (command_tx, command_rx) = mpsc::channel(CONTROL_COMMAND_CAPACITY);
        (Self { command_tx }, ControlInbox { command_rx })
    }

    pub async fn workers_ready(&self) -> anyhow::Result<()> {
        self.command_tx
            .send(ControlCommand::WorkersReady)
            .await
            .context("control command channel closed")
    }

    pub fn workers_ready_blocking(&self) -> anyhow::Result<()> {
        self.command_tx
            .blocking_send(ControlCommand::WorkersReady)
            .context("control command channel closed")
    }

    pub async fn shutdown(&self) -> anyhow::Result<()> {
        let (acknowledged, acknowledgement) = oneshot::channel();
        self.command_tx
            .send(ControlCommand::Shutdown { acknowledged })
            .await
            .context("control command channel closed")?;
        acknowledgement
            .await
            .context("control runtime exited before acknowledging shutdown")
    }

    pub fn shutdown_blocking(&self) -> anyhow::Result<()> {
        let (acknowledged, acknowledgement) = oneshot::channel();
        self.command_tx
            .blocking_send(ControlCommand::Shutdown { acknowledged })
            .context("control command channel closed")?;
        acknowledgement
            .blocking_recv()
            .context("control runtime exited before acknowledging shutdown")
    }
}
