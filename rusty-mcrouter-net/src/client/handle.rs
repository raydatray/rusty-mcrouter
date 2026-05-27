use super::command::ClientCommand;
use super::config::ClientConfig;
use super::connection::ClientConnection;
use crate::{NetError, Result};
use rusty_mcrouter_protocol::{Reply, Request};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::sync::{mpsc, oneshot};

// cloneable producer handle for a single memcache connection.
// all clones share one socket-owning ClientConnection task
#[derive(Clone)]
pub struct Client {
    tx: mpsc::Sender<ClientCommand>,
}

impl Client {
    pub async fn connect(addr: impl ToSocketAddrs) -> Result<Self> {
        Self::connect_with_config(addr, ClientConfig::default()).await
    }

    pub async fn connect_with_config(addr: impl ToSocketAddrs, cfg: ClientConfig) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        let (tx, rx) = mpsc::channel(cfg.max_pending);

        let connection = ClientConnection::new(stream, rx, &cfg);
        tokio::spawn(connection.run());

        Ok(Self { tx })
    }

    pub async fn send(&self, request: Request) -> Result<Reply> {
        let (reply_tx, reply_rx) = oneshot::channel();

        self.tx
            .send(ClientCommand { request, reply_tx })
            .await
            .map_err(|_| NetError::ClientClosed)?;

        match reply_rx.await {
            Ok(result) => result,
            Err(_) => Err(NetError::ClientClosed),
        }
    }
}
