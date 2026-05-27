use tokio::net::TcpListener;

const READ_BUF_INITIAL_CAPACITY: usize = 4096;

pub struct Server {
    listener: TcpListener,
}
