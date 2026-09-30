mod backend;
pub mod classify;
mod connection;
pub mod destination;
pub mod error;
pub mod metrics;
pub mod tko;

#[cfg(any(test, feature = "testing"))]
pub mod mock_memcached;
#[cfg(any(test, feature = "testing"))]
pub mod test_support;

pub use crate::backend::{
    Backend, BackendFactory, DestinationFactory, PoolFailOpen, PoolHealth, PreparedSend,
    TkoRejection,
};
