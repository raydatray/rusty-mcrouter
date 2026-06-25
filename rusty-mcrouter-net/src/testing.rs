use std::net::SocketAddr;
use std::time::Duration;

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

/// One step in a [`scripted_backend`] script.
pub enum Step {
    /// Read at least `n` request terminators before replying — forces the client to pipeline.
    ReadRequests(usize),
    Write(&'static [u8]),
    /// Write one byte at a time, forcing the client to reassemble across partial reads.
    WriteChunked(&'static [u8]),
    Close,
}

/// A scripted single-connection TCP peer for `Client` actor tests.
pub async fn scripted_backend(steps: Vec<Step>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut seen = Vec::<u8>::new();
        let mut buf = vec![0u8; 4096];

        for step in steps {
            match step {
                Step::ReadRequests(n) => {
                    while count_terminators(&seen) < n {
                        match stream.read(&mut buf).await {
                            Ok(0) | Err(_) => return,
                            Ok(k) => seen.extend_from_slice(&buf[..k]),
                        }
                    }
                }
                Step::Write(bytes) => {
                    if stream.write_all(bytes).await.is_err() {
                        return;
                    }
                }
                Step::WriteChunked(bytes) => {
                    for byte in bytes {
                        if stream.write_all(&[*byte]).await.is_err() {
                            return;
                        }
                        let _ = stream.flush().await;
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                }
                Step::Close => return,
            }
        }
    });
    addr
}

fn count_terminators(bytes: &[u8]) -> usize {
    bytes.windows(2).filter(|w| *w == b"\r\n").count()
}
