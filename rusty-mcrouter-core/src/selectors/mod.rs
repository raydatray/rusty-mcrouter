use thiserror::Error;

mod ch3;
mod crc32;
mod furc;
mod salted;

pub(crate) use ch3::Ch3;
pub(crate) use crc32::Crc32;
pub(crate) use salted::Salted;

pub trait Selector: 'static {
    fn select(&self, routing_key: &[u8]) -> usize;
}

#[derive(Debug, Error)]
pub enum SelectorBuildError {
    #[error("Ch3 pool size {n} is out of range (must be 1..=2^23)")]
    Ch3PoolSizeOutOfRange { n: usize },
}

pub(crate) type Result<T> = std::result::Result<T, SelectorBuildError>;
