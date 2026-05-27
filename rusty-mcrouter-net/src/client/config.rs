pub struct ClientConfig {
    pub max_pending: usize,
    pub read_buf_initial_capacity: usize,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            max_pending: 1024,
            read_buf_initial_capacity: 4096,
        }
    }
}
