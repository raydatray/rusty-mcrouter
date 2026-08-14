use std::{sync::Arc, time::Duration};

use rusty_mcrouter_protocol::{Reply, Request};
use tokio::{
    sync::{
        mpsc::{self, error::TrySendError},
        oneshot,
    },
    time::Instant,
};

use crate::{
    destination::{
        client_config::ClientConfig,
        client_connection::ClientConnection,
        types::{ClientCommand, ConnectionCommand, ConnectionEvent, Payload},
    },
    error::SendError,
};

#[derive(Clone)]
pub struct ClientConnectionHandle {
    tx: mpsc::Sender<ConnectionCommand>,
    reply_timeout: Option<Duration>,
}

impl ClientConnectionHandle {
    pub fn spawn(
        addr: Arc<str>,
        cfg: ClientConfig,
        events: Box<dyn Fn(ConnectionEvent)>,
    ) -> ClientConnectionHandle {
        let (tx, rx) = mpsc::channel(cfg.max_pending);
        let reply_timeout = cfg.reply_timeout;

        tokio::task::spawn_local(ClientConnection::new(addr, cfg, rx, events).run());

        ClientConnectionHandle { tx, reply_timeout }
    }

    pub async fn send(&self, request: Request) -> Result<Reply, SendError> {
        self.submit(Payload::Request(request)).await
    }

    pub async fn send_probe(&self) -> Result<Reply, SendError> {
        self.submit(Payload::VersionProbe).await
    }

    async fn submit(&self, payload: Payload) -> Result<Reply, SendError> {
        let (reply_tx, reply_rx) = oneshot::channel();

        let cmd = ClientCommand {
            payload,
            reply_tx,
            deadline: self.reply_timeout.map(|d| Instant::now() + d),
        };

        // fail fast on a full queue
        self.tx
            .try_send(ConnectionCommand::Command(cmd))
            .map_err(|e| match e {
                TrySendError::Full(_) => SendError::Local(crate::error::LocalError::QueueFull),
                TrySendError::Closed(_) => SendError::Local(crate::error::LocalError::Shutdown),
            })?;
        reply_rx
            .await
            .unwrap_or(Err(SendError::Local(crate::error::LocalError::Shutdown)))
    }

    // fire-and-forget from idle sweep - if the queue is full, it is not idle
    fn send_close_idle(&self) {
        let _ = self.tx.send(ConnectionCommand::CloseIdle);
    }
}
