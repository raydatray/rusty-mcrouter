use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProxyError {
    #[error("worker closed: {worker}")]
    WorkerClosed { worker: usize },
}

pub(crate) type Result<T> = std::result::Result<T, ProxyError>;
