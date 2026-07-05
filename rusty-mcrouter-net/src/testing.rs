use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rusty_mcrouter_protocol::{Reply, Request};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::{Backend, BackendFactory, NetError, Result};

/// Recording in-process `Backend` double: records every request, answers with a
/// scripted `Reply` (or `NetError` for the failure path). `Clone` shares one
/// recorder via `Arc`; `Send + Sync` so route tests can spawn it.
#[derive(Clone)]
pub struct MockBackend {
    inner: Arc<MockState>,
}

struct MockState {
    response: MockResponse,
    received: Mutex<Vec<Request>>,
}

enum MockResponse {
    Reply(Reply),
    Error(NetError),
}

impl MockBackend {
    pub fn replying(reply: Reply) -> Self {
        Self {
            inner: Arc::new(MockState {
                response: MockResponse::Reply(reply),
                received: Mutex::new(Vec::new()),
            }),
        }
    }

    pub fn failing(err: NetError) -> Self {
        Self {
            inner: Arc::new(MockState {
                response: MockResponse::Error(err),
                received: Mutex::new(Vec::new()),
            }),
        }
    }

    pub fn miss() -> Self {
        Self::replying(Reply::Get { hits: vec![] })
    }

    pub fn received(&self) -> Vec<Request> {
        self.inner.received.lock().unwrap().clone()
    }
}

impl Backend for MockBackend {
    async fn send(&self, req: Request) -> Result<Reply> {
        self.inner.received.lock().unwrap().push(req);
        match &self.inner.response {
            MockResponse::Reply(reply) => Ok(reply.clone()),
            MockResponse::Error(err) => Err(err.clone()),
        }
    }
}

/// A [`BackendFactory`] handing out [`MockBackend`]s without opening sockets;
/// `failing(addr)` drives the builder's `ConnectFailed` path deterministically.
#[derive(Clone, Default)]
pub struct MockBackendFactory {
    reply: Option<Reply>,
    fail_addr: Option<String>,
    connected: Arc<Mutex<Vec<String>>>,
}

impl MockBackendFactory {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn replying(reply: Reply) -> Self {
        Self {
            reply: Some(reply),
            ..Self::default()
        }
    }

    pub fn failing(addr: impl Into<String>) -> Self {
        Self {
            fail_addr: Some(addr.into()),
            ..Self::default()
        }
    }

    pub fn connected(&self) -> Vec<String> {
        self.connected.lock().unwrap().clone()
    }
}

impl BackendFactory for MockBackendFactory {
    type Backend = MockBackend;

    async fn connect(&self, addr: &str) -> Result<MockBackend> {
        if self.fail_addr.as_deref() == Some(addr) {
            return Err(NetError::ClientClosed);
        }
        self.connected.lock().unwrap().push(addr.to_string());
        Ok(MockBackend::replying(
            self.reply.clone().unwrap_or(Reply::Get { hits: vec![] }),
        ))
    }
}

/// One step in a [`scripted_backend`] script.
pub enum Step {
    /// Read at least `n` request terminators before replying — forces the client to pipeline.
    ReadRequests(usize),
    Write(&'static [u8]),
    /// Write one byte at a time, forcing the client to reassemble across partial reads.
    WriteChunked(&'static [u8]),
    /// Hold the connection open without replying, so the client's reply deadline fires.
    Hang,
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
                Step::Hang => std::future::pending::<()>().await,
                Step::Close => return,
            }
        }
    });
    addr
}

fn count_terminators(bytes: &[u8]) -> usize {
    bytes.windows(2).filter(|w| *w == b"\r\n").count()
}
