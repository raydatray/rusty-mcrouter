use std::time::Duration;

pub struct ClientConfig {
    pub max_pending: usize,
    pub read_buf_initial_capacity: usize,
    pub connect_timeout: Option<Duration>,
    pub write_timeout: Option<Duration>,
    pub reply_timeout: Option<Duration>,
    pub read_idle_timeout: Option<Duration>,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            max_pending: 1024,
            read_buf_initial_capacity: 4096,
            connect_timeout: Some(Duration::from_millis(1000)),
            write_timeout: Some(Duration::from_millis(1000)),
            reply_timeout: Some(Duration::from_millis(1000)),
            read_idle_timeout: Some(Duration::from_millis(2000)),
        }
    }
}
