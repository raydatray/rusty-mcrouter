use rusty_mcrouter_protocol::error::ProtocolError;
use thiserror::Error;

pub mod client;
pub mod server;

pub use client::Client;
pub use server::Server;

#[derive(Debug, Error)]
pub enum NetError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("protocol error: {0}")]
    Protocol(#[from] ProtocolError),
}
