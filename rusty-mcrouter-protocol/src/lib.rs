mod parser;
mod reply;
mod request;
mod wire;

pub use crate::parser::{parse_reply, parse_request};
pub use crate::reply::{Reply, Value};
pub use crate::request::Request;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("malformed protocol: {0}")]
    Malformed(&'static str),

    #[error("key too long: {0} bytes (max 250)")]
    KeyTooLong(usize),

    #[error("invalid key")]
    InvalidKey,
}

type Result<T> = std::result::Result<T, ProtocolError>;
