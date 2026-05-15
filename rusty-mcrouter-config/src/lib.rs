use json_comments::StripComments;

mod document;
mod error;
mod pool;
mod route;

pub use crate::document::{ConfigDocument, PrefixedRoute, RouteEntry};
pub use crate::error::ConfigError;
pub use crate::pool::PoolConfig;
pub use crate::route::RouteHandleConfig;

pub fn parse(input: &str) -> Result<ConfigDocument, ConfigError> {
    let stripped = StripComments::new(input.as_bytes());
    let value = serde_json::from_reader(stripped)?;

    ConfigDocument::from_value(value)
}

pub fn parse_file(path: impl AsRef<std::path::Path>) -> Result<ConfigDocument, ConfigError> {
    let text = std::fs::read_to_string(path)?;
    parse(&text)
}
