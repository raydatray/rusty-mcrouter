use json_comments::StripComments;
use thiserror::Error;

mod document;
mod pool;
mod route;

pub use crate::document::{ConfigDocument, PrefixedRoute, RouteEntry};
pub use crate::pool::{PoolConfig, PoolTkoTrackerConfig};
pub use crate::route::{
    FailoverErrorKind, FailoverErrorsConfig, FailoverPolicyConfig, HashConfig, HashFunc,
    RouteHandleConfig,
};

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("json parse error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("config must define exactly one of `route` or `routes`; both were provided")]
    BothRouteAndRoutes,

    #[error("config must define exactly one of `route` or `routes`; neither was provided")]
    MissingRoute,
}

type Result<T> = std::result::Result<T, ConfigError>;

pub fn parse(input: &str) -> Result<ConfigDocument> {
    let stripped = StripComments::new(input.as_bytes());
    let value = serde_json::from_reader(stripped)?;

    ConfigDocument::from_value(value)
}

pub fn parse_file(path: impl AsRef<std::path::Path>) -> Result<ConfigDocument> {
    let text = std::fs::read_to_string(path)?;
    parse(&text)
}
