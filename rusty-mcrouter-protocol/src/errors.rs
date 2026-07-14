use thiserror::Error;

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum KeyError {
    #[error("key must not be empty")]
    Empty,

    #[error("key is {actual} bytes, exceeding the {maximum}-byte limit")]
    TooLong { actual: usize, maximum: usize },
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ParseError {
    #[error("too many flags")]
    TooManyFlags,
}
