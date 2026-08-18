mod backend;
pub mod classify;
pub mod client;
pub mod destination;
pub mod error;
pub mod metrics;
pub mod tko;

#[cfg(any(test, feature = "testing"))]
pub mod mock_memcached;
#[cfg(any(test, feature = "testing"))]
pub mod test_support;

pub use crate::backend::{
    Backend, BackendFactory, BackendFactoryError, DestinationFactory, PoolHealth,
};
