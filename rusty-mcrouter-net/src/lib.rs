use rusty_mcrouter_protocol::meta::{MetaReplyDecodeError, MetaRequestEncodeError};
use thiserror::Error;

mod backend;
pub mod classify;
pub mod client;
pub mod error;
mod server;
pub mod tko;

#[cfg(any(test, feature = "testing"))]
pub mod mock_memcached;
#[cfg(any(test, feature = "testing"))]
pub mod test_support;

pub use crate::backend::{Backend, BackendFactory};
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

    /// The request cannot be represented on the wire (for example an empty
    /// backend key). Fails only the offending request, never the connection.
    #[error("request encode error: {0}")]
    Encode(#[from] MetaRequestEncodeError),

    /// The backend sent bytes that do not decode against the pending
    /// expectation. FIFO alignment is untrustworthy: fatal per connection.
    #[error("reply decode error: {0}")]
    Decode(#[from] MetaReplyDecodeError),

    /// The backend sent reply bytes with no request outstanding.
    #[error("protocol desync: {0}")]
    Desync(&'static str),

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
            NetError::Encode(e) => NetError::Encode(e.clone()),
            NetError::Decode(e) => NetError::Decode(e.clone()),
            NetError::Desync(reason) => NetError::Desync(reason),
            NetError::NoAddresses => NetError::NoAddresses,
            NetError::WorkerClosed { worker } => NetError::WorkerClosed { worker: *worker },
            NetError::ClientClosed => NetError::ClientClosed,
            NetError::Timeout { phase } => NetError::Timeout { phase: *phase },
        }
    }
}

type Result<T> = std::result::Result<T, NetError>;
