use thiserror::Error;

#[derive(Debug, Error)]
pub enum WorkerError {
    #[error("worker closed: {worker}")]
    WorkerClosed { worker: usize },
}

pub(crate) type Result<T> = std::result::Result<T, WorkerError>;
