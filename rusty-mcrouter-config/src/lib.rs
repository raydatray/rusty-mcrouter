mod document;
mod pool;
mod route;
mod server;

pub use crate::document::{parse, parse_file, ConfigDocument, ConfigError, PoolId};
pub use crate::pool::{PoolConfig, PoolTkoTrackerConfig};
pub use crate::route::{
    FailoverErrorKind, FailoverErrorsConfig, FailoverPolicyConfig, HashConfig, HashFunc,
    RouteConfig,
};
pub use crate::server::ServerConfig;
