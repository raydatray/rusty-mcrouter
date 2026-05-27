use rusty_mcrouter_protocol::ProtocolError;
use thiserror::Error;

mod client;
mod server;

#[cfg(any(test, feature = "testing"))]
pub mod testing;

pub use crate::client::Client;
pub use crate::server::{serve_worker, Server};

#[derive(Debug, Error)]
pub enum NetError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("protocol error: {0}")]
    Protocol(#[from] ProtocolError),

    #[error("no addresses found")]
    NoAddresses,

    #[error("worker closed: {worker}")]
    WorkerClosed { worker: usize },
}

type Result<T> = std::result::Result<T, NetError>;
