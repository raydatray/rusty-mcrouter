use thiserror::Error;

#[derive(Debug, Error)]
pub enum FrontendError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("no addresses found")]
    NoAddresses,

    #[error("worker closed: {worker}")]
    WorkerClosed { worker: usize },

    #[error("request task failed: {0}")]
    RequestTask(#[from] tokio::task::JoinError),
}

pub(crate) type Result<T> = std::result::Result<T, FrontendError>;
