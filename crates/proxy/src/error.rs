use thiserror::Error;

#[derive(Debug, Error)]
pub enum FrontendError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("no addresses found")]
    NoAddresses,

    #[error("worker closed: {worker}")]
    WorkerClosed { worker: usize },
}

pub(crate) type Result<T> = std::result::Result<T, FrontendError>;
