use std::net::SocketAddr;

#[derive(Clone, Copy, Debug)]
pub struct FrontendConnectionOptions {
    pub read_buf_initial_capacity: usize,
}

impl Default for FrontendConnectionOptions {
    fn default() -> Self {
        Self {
            read_buf_initial_capacity: 4096,
        }
    }
}

pub struct ListenerConfig {
    pub listen_addr: SocketAddr,
    pub use_reuseport: bool,
}
