use std::{future::Future, rc::Rc};

use bytes::BytesMut;
use rusty_mcrouter_protocol::{parse_request, Reply, Request};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{lookup_host, TcpListener, TcpSocket, ToSocketAddrs},
    sync::mpsc::{self, Sender},
};

use crate::{NetError, Result};

const READ_BUF_INITIAL_CAPACITY: usize = 4096;
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

            let target = next % work_txs.len();
            next = next.wrapping_add(1);

            if work_txs[target].send(std_stream).await.is_err() {
                return Err(NetError::WorkerClosed { worker: target });
            }
        }
    }

    pub async fn serve<F, Fut>(self, handler: F) -> Result<()>
    where
        F: Fn(Request) -> Fut + 'static,
        Fut: Future<Output = Reply> + 'static,
    {
        let (work_tx, work_rx) = mpsc::channel::<std::net::TcpStream>(LISTEN_BACKLOG as usize);

        let dispatch = self.accept_and_dispatch(vec![work_tx]);
        let worker = serve_worker(work_rx, handler);

        let (dispatch_result, _) = tokio::join!(dispatch, worker);
        dispatch_result
    }
}

pub async fn serve_worker<F, Fut>(mut work_rx: mpsc::Receiver<std::net::TcpStream>, handler: F)
where
    F: Fn(Request) -> Fut + 'static,
    Fut: Future<Output = Reply> + 'static,
{
    let handler = Rc::new(handler);
    while let Some(std_stream) = work_rx.recv().await {
        let tokio_stream = match tokio::net::TcpStream::from_std(std_stream) {
            Ok(s) => s,
            Err(e) => {
                //todo - logger
                eprintln!("could not reregister accepted stream on worker runtime: {e}");
                continue;
            }
        };

        let handler = Rc::clone(&handler);
        tokio::task::spawn_local(async move {
            let _ = serve_session(tokio_stream, handler).await;
        });
    }
}

async fn serve_session<F, Fut>(mut stream: tokio::net::TcpStream, handler: Rc<F>) -> Result<()>
where
    F: Fn(Request) -> Fut,
    Fut: Future<Output = Reply>,
{
    let mut buf = BytesMut::with_capacity(READ_BUF_INITIAL_CAPACITY);

    loop {
        // Drain any complete frames already buffered before reading more.
        // A single read can contain multiple pipelined requests.
        while let Some(req) = parse_request(&mut buf)? {
            let reply = (*handler)(req).await;
            let mut out = BytesMut::new();
            reply.serialize_into(&mut out);
            stream.write_all(&out).await?;
        }

        let n = stream.read_buf(&mut buf).await?;
        if n == 0 {
            return Ok(());
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
