use std::time::Duration;

#[derive(Clone, Debug)]
pub struct BackendConnectionConfig {
    pub max_pending: usize,
    pub read_buf_initial_capacity: usize,
    pub connect_timeout: Option<Duration>,
    pub connect_timeout_retries: usize,
    pub write_timeout: Option<Duration>,
    pub reply_timeout: Option<Duration>,
}

impl Default for BackendConnectionConfig {
    fn default() -> Self {
        Self {
            max_pending: 1024,
            read_buf_initial_capacity: 4096,
            connect_timeout: Some(Duration::from_millis(1000)),
            connect_timeout_retries: 0,
            write_timeout: Some(Duration::from_millis(1000)),
            reply_timeout: Some(Duration::from_millis(1000)),
        }
    }
}
