use std::time::Duration;

pub struct ClientConfig {
    pub max_pending: usize,
    pub read_buf_initial_capacity: usize,
    pub connect_timeout: Option<Duration>,
    pub connect_timeout_retries: usize,
    pub write_timeout: Option<Duration>,
    pub reply_timeout: Option<Duration>,
    pub read_idle_timeout: Option<Duration>,
}
