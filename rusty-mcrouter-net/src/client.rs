use bytes::BytesMut;
use rusty_mcrouter_protocol::{
    error::ProtocolError, parser::parse_reply, reply::Reply, request::Request,
};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, ToSocketAddrs};

const READ_BUF_INITIAL_CAPACITY: usize = 4096;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("protocol error: {0}")]
    Protocol(#[from] ProtocolError),
}

pub struct Client {
    stream: TcpStream,
    buf: BytesMut,
}

impl Client {
    pub async fn connect(addr: impl ToSocketAddrs) -> std::io::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Self {
            stream,
            buf: BytesMut::with_capacity(READ_BUF_INITIAL_CAPACITY),
        })
    }

    pub async fn send(&mut self, req: &Request) -> Result<Reply, ClientError> {
        let mut send_buf = BytesMut::new();
        req.serialize_into(&mut send_buf);
        self.stream.write_all(&send_buf).await?;

        loop {
            let n = self.stream.read_buf(&mut self.buf).await?;
            if n == 0 {
                return Err(ClientError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "backend closed connection mid-reply",
                )));
            }
            if let Some(reply) = parse_reply(&mut self.buf)? {
                return Ok(reply);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::net::TcpListener;

    fn req(keys: &[&'static [u8]]) -> Request {
        Request::Get {
            keys: keys.iter().map(|k| Bytes::from_static(k)).collect(),
        }
    }

    async fn mock_backend(reply: &'static [u8]) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut consume = vec![0u8; 1024];
            let _ = stream.read(&mut consume).await.unwrap();
            stream.write_all(reply).await.unwrap();
        });
        addr
    }

    async fn mock_backend_chunked(chunks: Vec<&'static [u8]>) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut consume = vec![0u8; 1024];
            let _ = stream.read(&mut consume).await.unwrap();
            for chunk in chunks {
                stream.write_all(chunk).await.unwrap();
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });
        addr
    }

    #[tokio::test]
    async fn send_returns_miss() {
        let addr = mock_backend(b"END\r\n").await;
        let mut client = Client::connect(addr).await.unwrap();
        let reply = client.send(&req(&[b"foo"])).await.unwrap();
        assert_eq!(reply, Reply::Get { hits: vec![] });
    }

    #[tokio::test]
    async fn send_returns_single_hit() {
        let addr = mock_backend(b"VALUE foo 0 3\r\nbar\r\nEND\r\n").await;
        let mut client = Client::connect(addr).await.unwrap();
        let reply = client.send(&req(&[b"foo"])).await.unwrap();
        let Reply::Get { hits } = reply;
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].key.as_ref(), b"foo");
        assert_eq!(hits[0].data.as_ref(), b"bar");
        assert_eq!(hits[0].flags, 0);
    }

    #[tokio::test]
    async fn send_handles_fragmented_response_across_multiple_reads() {
        let addr =
            mock_backend_chunked(vec![b"VALUE foo 0", b" 3\r\nb", b"ar\r\nE", b"ND\r\n"]).await;
        let mut client = Client::connect(addr).await.unwrap();
        let reply = client.send(&req(&[b"foo"])).await.unwrap();
        let Reply::Get { hits } = reply;
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].key.as_ref(), b"foo");
        assert_eq!(hits[0].data.as_ref(), b"bar");
    }

    #[tokio::test]
    async fn send_returns_eof_when_backend_closes_mid_reply() {
        let addr = mock_backend(b"VALUE foo 0 3\r\n").await;
        let mut client = Client::connect(addr).await.unwrap();
        let result = client.send(&req(&[b"foo"])).await;
        assert!(matches!(
            result,
            Err(ClientError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof
        ));
    }

    #[tokio::test]
    async fn send_propagates_protocol_error_on_unknown_reply_line() {
        let addr = mock_backend(b"WAT\r\n").await;
        let mut client = Client::connect(addr).await.unwrap();
        let result = client.send(&req(&[b"foo"])).await;
        assert!(matches!(
            result,
            Err(ClientError::Protocol(ProtocolError::Malformed(
                "expected VALUE or END"
            )))
        ));
    }

    #[tokio::test]
    async fn send_writes_correct_request_bytes_to_backend() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let received = Arc::new(Mutex::new(Vec::<u8>::new()));
        let received_clone = Arc::clone(&received);

        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 1024];
            let n = stream.read(&mut buf).await.unwrap();
            received_clone.lock().unwrap().extend_from_slice(&buf[..n]);
            stream.write_all(b"END\r\n").await.unwrap();
        });

        let mut client = Client::connect(addr).await.unwrap();
        let _ = client.send(&req(&[b"foo", b"bar", b"baz"])).await.unwrap();

        let bytes = received.lock().unwrap().clone();
        assert_eq!(bytes, b"get foo bar baz\r\n");
    }

    #[tokio::test]
    async fn two_sequential_sends_on_same_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 1024];
            let _ = stream.read(&mut buf).await.unwrap();
            stream
                .write_all(b"VALUE k1 0 1\r\nA\r\nEND\r\n")
                .await
                .unwrap();
            let _ = stream.read(&mut buf).await.unwrap();
            stream
                .write_all(b"VALUE k2 0 1\r\nB\r\nEND\r\n")
                .await
                .unwrap();
        });

        let mut client = Client::connect(addr).await.unwrap();

        let r1 = client.send(&req(&[b"k1"])).await.unwrap();
        let Reply::Get { hits: hits1 } = r1;
        assert_eq!(hits1[0].key.as_ref(), b"k1");
        assert_eq!(hits1[0].data.as_ref(), b"A");

        let r2 = client.send(&req(&[b"k2"])).await.unwrap();
        let Reply::Get { hits: hits2 } = r2;
        assert_eq!(hits2[0].key.as_ref(), b"k2");
        assert_eq!(hits2[0].data.as_ref(), b"B");
    }
}
