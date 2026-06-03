use tokio::{
    net::{lookup_host, TcpListener, TcpSocket, ToSocketAddrs},
    sync::mpsc::Sender,
};

use crate::{NetError, Result};

const LISTEN_BACKLOG: u32 = 1024;

pub struct Server {
    listener: TcpListener,
}

impl Server {
    pub async fn bind(addr: impl ToSocketAddrs) -> Result<Self> {
        let listener = TcpListener::bind(addr).await?;

        Ok(Self { listener })
    }

    pub async fn bind_reuseport(addr: impl ToSocketAddrs) -> Result<Self> {
        let listener = lookup_host(addr)
            .await?
            .find_map(|addr| {
                let socket = if addr.is_ipv4() {
                    TcpSocket::new_v4()
                } else {
                    TcpSocket::new_v6()
                }
                .ok()?;

                socket.set_reuseaddr(true).ok()?;
                socket.set_reuseport(true).ok()?;
                socket.bind(addr).ok()?;
                socket.listen(LISTEN_BACKLOG).ok()
            })
            .ok_or(NetError::NoAddresses)?;

        Ok(Self { listener })
    }

    pub fn local_addr(&self) -> Result<std::net::SocketAddr> {
        self.listener.local_addr().map_err(|e| e.into())
    }

    pub async fn accept_and_dispatch(
        self,
        work_txs: Vec<Sender<std::net::TcpStream>>,
    ) -> Result<()> {
        let mut next = 0;
        loop {
            let (tokio_stream, _) = match self.listener.accept().await {
                Ok(pair) => pair,
                Err(e) if is_transient_accept_error(&e) => {
                    // todo - logger
                    eprintln!("transient accept error, continuing: {e}");
                    continue;
                }
                Err(e) => return Err(e.into()),
            };

            let std_stream = tokio_stream.into_std()?;

            // todo - thread modes, accepted sockets are round-robin today; per-request affinity belongs behind a proxy message queue
            let target = next % work_txs.len();
            next = next.wrapping_add(1);

            if work_txs[target].send(std_stream).await.is_err() {
                return Err(NetError::WorkerClosed { worker: target });
            }
        }
    }
}

fn is_transient_accept_error(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::Interrupted | std::io::ErrorKind::ConnectionAborted
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn bind_reuseport_allows_two_binds_on_same_port() {
        let s1 = Server::bind_reuseport("127.0.0.1:0").await.unwrap();
        let addr = s1.listener.local_addr().unwrap();

        let s2 = Server::bind_reuseport(addr).await.unwrap();
        assert_eq!(s2.listener.local_addr().unwrap(), addr);
    }

    #[tokio::test]
    async fn bind_reuseport_plain_bind_on_same_port_fails() {
        let s1 = Server::bind_reuseport("127.0.0.1:0").await.unwrap();
        let addr = s1.listener.local_addr().unwrap();

        match Server::bind(addr).await {
            Ok(_) => {
                panic!("plain bind without SO_REUSEPORT should fail when port is already bound")
            }
            Err(NetError::Io(e)) => assert_eq!(e.kind(), std::io::ErrorKind::AddrInUse),
            Err(other) => panic!("expected io error, got {other:?}"),
        }
    }
}
