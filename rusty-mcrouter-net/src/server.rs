use bytes::BytesMut;
use rusty_mcrouter_protocol::{parse_request, Reply, Request};
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};

use crate::NetError;

const READ_BUF_INITIAL_CAPACITY: usize = 4096;

pub struct Server {
    listener: TcpListener,
}

impl Server {
    pub async fn bind(addr: impl ToSocketAddrs) -> std::io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self { listener })
    }

    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    pub async fn serve<F, Fut>(self, handler: F) -> std::io::Result<()>
    where
        F: Fn(Request) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Reply> + Send + 'static,
    {
        let handler = Arc::new(handler);
        loop {
            let (stream, _) = self.listener.accept().await?;
            let handler = Arc::clone(&handler);
            tokio::spawn(async move {
                let _ = serve_session(stream, handler).await;
            });
        }
    }
}

async fn serve_session<F, Fut>(mut stream: TcpStream, handler: Arc<F>) -> Result<(), NetError>
where
    F: Fn(Request) -> Fut + Send + Sync,
    Fut: Future<Output = Reply> + Send,
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

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use rusty_mcrouter_protocol::Value;

    async fn spawn_server<F, Fut>(handler: F) -> SocketAddr
    where
        F: Fn(Request) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Reply> + Send + 'static,
    {
        let server = Server::bind("127.0.0.1:0").await.unwrap();
        let addr = server.local_addr().unwrap();
        tokio::spawn(server.serve(handler));
        addr
    }

    async fn echo_handler(req: Request) -> Reply {
        let Request::Get { keys } = req else {
            panic!("echo_handler only handles Request::Get");
        };
        Reply::Get {
            hits: keys
                .into_iter()
                .map(|k| Value {
                    key: k,
                    flags: 0,
                    data: Bytes::from_static(b"x"),
                })
                .collect(),
        }
    }

    #[tokio::test]
    async fn server_responds_to_single_get() {
        let addr = spawn_server(echo_handler).await;
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(b"get foo\r\n").await.unwrap();

        let mut buf = vec![0u8; 1024];
        let n = stream.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"VALUE foo 0 1\r\nx\r\nEND\r\n");
    }

    #[tokio::test]
    async fn server_responds_to_multi_key_get() {
        let addr = spawn_server(echo_handler).await;
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(b"get a bb ccc\r\n").await.unwrap();

        let mut buf = vec![0u8; 1024];
        let n = stream.read(&mut buf).await.unwrap();
        assert_eq!(
            &buf[..n],
            b"VALUE a 0 1\r\nx\r\nVALUE bb 0 1\r\nx\r\nVALUE ccc 0 1\r\nx\r\nEND\r\n"
        );
    }

    #[tokio::test]
    async fn server_handles_pipelined_requests_on_same_connection() {
        let addr = spawn_server(echo_handler).await;
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(b"get foo\r\nget bar\r\n").await.unwrap();

        let expected = b"VALUE foo 0 1\r\nx\r\nEND\r\nVALUE bar 0 1\r\nx\r\nEND\r\n";
        let mut received = Vec::new();
        let mut tmp = vec![0u8; 1024];
        while received.len() < expected.len() {
            let n = stream.read(&mut tmp).await.unwrap();
            assert!(n > 0, "server closed before full response");
            received.extend_from_slice(&tmp[..n]);
        }
        assert_eq!(received, expected);
    }

    #[tokio::test]
    async fn server_handles_fragmented_request_across_multiple_writes() {
        let addr = spawn_server(echo_handler).await;
        let mut stream = TcpStream::connect(addr).await.unwrap();

        for chunk in [b"get fo".as_ref(), b"o\r".as_ref(), b"\n".as_ref()] {
            stream.write_all(chunk).await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }

        let mut buf = vec![0u8; 1024];
        let n = stream.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"VALUE foo 0 1\r\nx\r\nEND\r\n");
    }

    #[tokio::test]
    async fn server_serves_two_concurrent_connections_independently() {
        let addr = spawn_server(echo_handler).await;

        let conn_a = tokio::spawn(async move {
            let mut s = TcpStream::connect(addr).await.unwrap();
            s.write_all(b"get aaa\r\n").await.unwrap();
            let mut buf = vec![0u8; 1024];
            let n = s.read(&mut buf).await.unwrap();
            buf[..n].to_vec()
        });
        let conn_b = tokio::spawn(async move {
            let mut s = TcpStream::connect(addr).await.unwrap();
            s.write_all(b"get bbb\r\n").await.unwrap();
            let mut buf = vec![0u8; 1024];
            let n = s.read(&mut buf).await.unwrap();
            buf[..n].to_vec()
        });

        let (a, b) = tokio::join!(conn_a, conn_b);
        assert_eq!(a.unwrap(), b"VALUE aaa 0 1\r\nx\r\nEND\r\n");
        assert_eq!(b.unwrap(), b"VALUE bbb 0 1\r\nx\r\nEND\r\n");
    }

    async fn ack_set_handler(req: Request) -> Reply {
        match req {
            Request::Set { .. }
            | Request::Add { .. }
            | Request::Replace { .. }
            | Request::Append { .. }
            | Request::Prepend { .. } => Reply::Stored,
            Request::Get { .. } => Reply::Get { hits: vec![] },
            Request::Delete { .. } => Reply::Deleted,
            Request::Incr { .. } | Request::Decr { .. } => Reply::NotFound,
        }
    }

    #[tokio::test]
    async fn server_responds_to_set_request() {
        let addr = spawn_server(ack_set_handler).await;
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(b"set foo 0 0 3\r\nbar\r\n").await.unwrap();

        let mut buf = vec![0u8; 1024];
        let n = stream.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"STORED\r\n");
    }

    #[tokio::test]
    async fn server_handles_set_then_get_pipelined() {
        async fn handler(req: Request) -> Reply {
            match req {
                Request::Set { .. }
                | Request::Add { .. }
                | Request::Replace { .. }
                | Request::Append { .. }
                | Request::Prepend { .. } => Reply::Stored,
                Request::Get { keys } => Reply::Get {
                    hits: keys
                        .into_iter()
                        .map(|k| Value {
                            key: k,
                            flags: 0,
                            data: Bytes::from_static(b"x"),
                        })
                        .collect(),
                },
                Request::Delete { .. } => Reply::Deleted,
                Request::Incr { .. } | Request::Decr { .. } => Reply::NotFound,
            }
        }
        let addr = spawn_server(handler).await;
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"set foo 0 0 3\r\nbar\r\nget foo\r\n")
            .await
            .unwrap();

        let expected = b"STORED\r\nVALUE foo 0 1\r\nx\r\nEND\r\n";
        let mut received = Vec::new();
        let mut tmp = vec![0u8; 1024];
        while received.len() < expected.len() {
            let n = stream.read(&mut tmp).await.unwrap();
            assert!(n > 0, "server closed before full response");
            received.extend_from_slice(&tmp[..n]);
        }
        assert_eq!(received, expected);
    }

    #[tokio::test]
    async fn server_handles_set_with_fragmented_body() {
        let addr = spawn_server(ack_set_handler).await;
        let mut stream = TcpStream::connect(addr).await.unwrap();

        let chunks: &[&[u8]] = &[b"set foo 0 0 5\r\n", b"hel", b"lo\r\n"];
        for chunk in chunks {
            stream.write_all(chunk).await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }

        let mut buf = vec![0u8; 1024];
        let n = stream.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"STORED\r\n");
    }

    #[tokio::test]
    async fn server_handler_can_emit_server_error_reply() {
        async fn err_handler(_req: Request) -> Reply {
            Reply::ServerError(Bytes::from_static(b"backend down"))
        }
        let addr = spawn_server(err_handler).await;
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(b"get foo\r\n").await.unwrap();

        let mut buf = vec![0u8; 1024];
        let n = stream.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"SERVER_ERROR backend down\r\n");
    }

    #[tokio::test]
    async fn server_closes_session_on_malformed_request() {
        let addr = spawn_server(echo_handler).await;
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(b"NOTACOMMAND foo\r\n").await.unwrap();

        let mut buf = vec![0u8; 1024];
        let n = stream.read(&mut buf).await.unwrap();
        assert_eq!(n, 0, "expected EOF but got bytes: {:?}", &buf[..n]);
    }

    #[tokio::test]
    async fn server_cleans_up_when_client_disconnects() {
        let addr = spawn_server(echo_handler).await;
        let stream = TcpStream::connect(addr).await.unwrap();
        drop(stream);

        let mut s = TcpStream::connect(addr).await.unwrap();
        s.write_all(b"get foo\r\n").await.unwrap();
        let mut buf = vec![0u8; 1024];
        let n = s.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"VALUE foo 0 1\r\nx\r\nEND\r\n");
    }
}
