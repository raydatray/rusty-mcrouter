use std::net::SocketAddr;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Single-shot mock backend.
///
/// Accepts one TCP connection, reads once, writes `reply` once, closes.
/// Suitable only for tests that issue exactly one request.
pub async fn mock_backend(reply: &'static [u8]) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 1024];
        let _ = stream.read(&mut buf).await.unwrap();
        stream.write_all(reply).await.unwrap();
    });
    addr
}

/// Long-lived mock backend that writes `reply` once per `\r\n` terminator
/// observed in the inbound byte stream.
///
/// Suitable for tests that issue many sequential requests on the same
/// `Client`. Replies are emitted immediately as each request arrives,
/// so this does NOT prove pipelining.
pub async fn looping_mock_backend(reply: &'static [u8]) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 4096];
        loop {
            let n = match stream.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(n) => n,
            };
            let replies = count_terminators(&buf[..n]);
            for _ in 0..replies {
                if stream.write_all(reply).await.is_err() {
                    return;
                }
            }
        }
    });
    addr
}

/// Mock backend that proves backend pipelining.
///
/// Reads the input stream until it has observed at least `n_requests`
/// `\r\n` terminators, *then* writes `reply` exactly `n_requests` times.
/// Any client that fails to pipeline will deadlock against this backend:
/// the client waits for a reply, the backend waits for the next request.
pub async fn pipelining_mock_backend(
    reply: &'static [u8],
    n_requests: usize,
) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 4096];
        let mut accumulated = Vec::<u8>::new();

        // Phase 1: read at least `n_requests` complete frames before replying.
        while count_terminators(&accumulated) < n_requests {
            let n = match stream.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(n) => n,
            };
            accumulated.extend_from_slice(&buf[..n]);
        }

        // Phase 2: emit one reply per observed request, in order.
        for _ in 0..n_requests {
            if stream.write_all(reply).await.is_err() {
                return;
            }
        }
    });
    addr
}

fn count_terminators(bytes: &[u8]) -> usize {
    bytes.windows(2).filter(|w| *w == b"\r\n").count()
}
