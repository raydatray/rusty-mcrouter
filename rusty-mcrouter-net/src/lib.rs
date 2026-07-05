use rusty_mcrouter_protocol::ProtocolError;
use thiserror::Error;

mod backend;
mod client;
mod server;

#[cfg(any(test, feature = "testing"))]
pub mod mock_memcached;
#[cfg(any(test, feature = "testing"))]
pub mod testing;

pub use crate::backend::{Backend, BackendFactory, ClientFactory};
pub use crate::client::{Client, ClientConfig};
pub use crate::server::Server;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimeoutPhase {
    Connect,
    Write,
    Reply,
}

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

    #[error("backend client closed")]
    ClientClosed,

    #[error("{phase:?} timed out")]
    Timeout { phase: TimeoutPhase },
}

// todo - revisit this error type because fucking std::io::Error is not clone
impl Clone for NetError {
    fn clone(&self) -> Self {
        match self {
            NetError::Io(e) => NetError::Io(std::io::Error::new(e.kind(), e.to_string())),
            NetError::Protocol(p) => NetError::Protocol(p.clone()),
            NetError::NoAddresses => NetError::NoAddresses,
            NetError::WorkerClosed { worker } => NetError::WorkerClosed { worker: *worker },
            NetError::ClientClosed => NetError::ClientClosed,
            NetError::Timeout { phase } => NetError::Timeout { phase: *phase },
        }
    }
}

type Result<T> = std::result::Result<T, NetError>;
