use std::net::SocketAddr;

use tokio::net::{lookup_host, TcpListener, TcpSocket};

use crate::{error::Result, FrontendError, ListenerConfig, ProxySet};

const LISTEN_BACKLOG: u32 = 1024;

pub struct Server {
    listener: TcpListener,
}

pub async fn bind_listener(options: ListenerConfig) -> Result<TcpListener> {
    if !options.use_reuseport {
        return TcpListener::bind(options.listen_addr)
            .await
            .map_err(Into::into);
    }
    lookup_host(options.listen_addr)
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
        .ok_or(FrontendError::NoAddresses)
}

impl Server {
    pub fn new(listener: TcpListener) -> Self {
        Self { listener }
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.listener.local_addr().map_err(|e| e.into())
    }

    pub async fn accept_and_dispatch(self, proxies: ProxySet) -> Result<()> {
        let mut next = 0usize;
        loop {
            let (tokio_stream, _) = match self.listener.accept().await {
                Ok(pair) => pair,
                Err(e) if is_transient_accept_error(&e) => continue,
                Err(e) => return Err(e.into()),
            };

            let std_stream = match tokio_stream.into_std() {
                Ok(stream) => stream,
                Err(_) => continue,
            };

            // todo - thread modes, accepted sockets are round-robin today; per-request affinity belongs behind a proxy message queue
            proxies.nth(next).send_connection(std_stream).await?;
            next = next.wrapping_add(1);
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

    async fn bind(addr: SocketAddr, use_reuseport: bool) -> Result<Server> {
        bind_listener(ListenerConfig {
            listen_addr: addr,
            use_reuseport,
        })
        .await
        .map(Server::new)
    }

    #[tokio::test]
    async fn bind_reuseport_allows_two_binds_on_same_port() {
        let s1 = bind("127.0.0.1:0".parse().unwrap(), true).await.unwrap();
        let addr = s1.listener.local_addr().unwrap();

        let s2 = bind(addr, true).await.unwrap();
        assert_eq!(s2.listener.local_addr().unwrap(), addr);
    }

    #[tokio::test]
    async fn bind_reuseport_plain_bind_on_same_port_fails() {
        let s1 = bind("127.0.0.1:0".parse().unwrap(), true).await.unwrap();
        let addr = s1.listener.local_addr().unwrap();

        match bind(addr, false).await {
            Ok(_) => {
                panic!("plain bind without SO_REUSEPORT should fail when port is already bound")
            }
            Err(FrontendError::Io(e)) => assert_eq!(e.kind(), std::io::ErrorKind::AddrInUse),
            Err(other) => panic!("expected io error, got {other:?}"),
        }
    }
}
