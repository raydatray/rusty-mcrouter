use thiserror::Error;

mod backend;
pub mod classify;
pub mod client;
pub mod destination;
pub mod error;
mod server;
pub mod tko;

#[cfg(any(test, feature = "testing"))]
pub mod mock_memcached;
#[cfg(any(test, feature = "testing"))]
pub mod test_support;

pub use crate::backend::{
    Backend, BackendFactory, BackendFactoryError, DestinationFactory, PoolHealth,
};
pub use crate::server::Server;

/// Frontend/server-side errors only. The backend path speaks
/// [`error::SendError`] (Clone by construction); this type never crosses the
/// route tree, so it can carry `std::io::Error` without cloning gymnastics.
#[derive(Debug, Error)]
pub enum NetError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("no addresses found")]
    NoAddresses,

    #[error("worker closed: {worker}")]
    WorkerClosed { worker: usize },
}

type Result<T> = std::result::Result<T, NetError>;
