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
