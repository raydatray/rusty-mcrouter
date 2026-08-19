//! Test doubles and socket/event/time harnesses for this crate and
//! downstream (enable the `testing` feature). Protocol *fixtures* live in
//! `rusty_mcrouter_protocol::test_support`; this module only adds what the
//! net layer needs on top: scripted TCP peers, a LocalSet runner for the
//! spawn_local-based client actor, and a connection-event collector.

use std::cell::RefCell;
use std::future::Future;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rusty_mcrouter_protocol::reply::GetReply;
use rusty_mcrouter_protocol::{Reply, Request};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::backend::{
    Backend, BackendFactory, BackendFactoryError, PoolHealth, PreparedSend, TkoRejection,
};
use crate::client::ConnectionEvent;
use crate::error::SendError;

/// Runs a future inside a fresh `LocalSet`. The client actor is spawned via
/// `spawn_local`, which panics in a bare `#[tokio::test]`; every actor test
/// wraps its body in this.
pub async fn run_local<F: Future>(fut: F) -> F::Output {
    tokio::task::LocalSet::new().run_until(fut).await
}

/// Shared handle to the events a [`ConnectionEvent`] collector has seen.
pub type ConnectionEventLog = Rc<RefCell<Vec<ConnectionEvent>>>;

/// Collector for [`ConnectionEvent`]s: asserting on the exact sequence is
/// how tests distinguish a benign close (`[Up, Closed]`) from health
/// evidence (`[Up, Down(..)]`).
pub fn event_log() -> (Box<dyn Fn(ConnectionEvent)>, ConnectionEventLog) {
    let log = Rc::new(RefCell::new(Vec::new()));
    let sink = {
        let log = Rc::clone(&log);
        Box::new(move |ev| log.borrow_mut().push(ev)) as Box<dyn Fn(ConnectionEvent)>
    };
    (sink, log)
}

/// Recording in-process `Backend` double: records every request, answers with
/// a scripted `Reply` (or `SendError` for the failure path — drives failover
/// and route-behavior tests). `Clone` shares one recorder via `Arc`;
/// `Send + Sync` so route tests can spawn it.
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
    Error(SendError),
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

    pub fn failing(err: SendError) -> Self {
        Self {
            inner: Arc::new(MockState {
                response: MockResponse::Error(err),
                received: Mutex::new(Vec::new()),
            }),
        }
    }

    pub fn miss() -> Self {
        Self::replying(Reply::Get(GetReply::Miss))
    }

    pub fn received(&self) -> Vec<Request> {
        self.inner.received.lock().unwrap().clone()
    }
}

impl Backend for MockBackend {
    fn prepare_send(
        &self,
        req: Request,
    ) -> Result<PreparedSend<impl Future<Output = Result<Reply, SendError>> + '_>, TkoRejection>
    {
        self.inner.received.lock().unwrap().push(req);
        let result = match &self.inner.response {
            MockResponse::Reply(reply) => Ok(reply.clone()),
            MockResponse::Error(SendError::Tko { reason }) => {
                return Err(TkoRejection { reason: *reason })
            }
            MockResponse::Error(err) => Err(err.clone()),
        };
        Ok(PreparedSend::new(async move { result }))
    }
}

/// A [`BackendFactory`] handing out [`MockBackend`]s without opening sockets.
///
/// Two DIFFERENT failure knobs for two different layers:
/// - `failing(addr)`: `make()` errors for that address — drives the route
///   BUILDER's invalid-server path
/// - `MockBackend::failing(SendError)`: `send()` errors — drives
///   failover/route BEHAVIOR tests (in the lazy world, a dead server still
///   builds successfully)
#[derive(Clone, Default)]
pub struct MockBackendFactory {
    reply: Option<Reply>,
    fail_addr: Option<String>,
    made: Arc<Mutex<Vec<String>>>,
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

    /// Every address a backend was made for, in order.
    pub fn made(&self) -> Vec<String> {
        self.made.lock().unwrap().clone()
    }
}

impl BackendFactory for MockBackendFactory {
    type Backend = MockBackend;

    fn make(
        &self,
        server: &str,
        _cfg: &crate::destination::Config,
        _pool: &PoolHealth<'_>,
    ) -> Result<MockBackend, BackendFactoryError> {
        if self.fail_addr.as_deref() == Some(server) {
            return Err(BackendFactoryError::InvalidAddress {
                addr: server.to_string(),
            });
        }
        self.made.lock().unwrap().push(server.to_string());
        Ok(MockBackend::replying(
            self.reply.clone().unwrap_or(Reply::Get(GetReply::Miss)),
        ))
    }
}

/// One step in a [`scripted_backend_serial`] script.
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

/// A scripted TCP peer serving one inner script per accepted connection,
/// strictly in order. `accept_count()` is the assertion that separates
/// "reconnected" from "reused the same connection" — the backbone of the
/// reconnect/idle-close test matrix.
pub struct ScriptedServer {
    pub addr: SocketAddr,
    accepted: Arc<AtomicUsize>,
}

impl ScriptedServer {
    pub fn accept_count(&self) -> usize {
        self.accepted.load(Ordering::SeqCst)
    }
}

pub async fn scripted_backend_serial(scripts: Vec<Vec<Step>>) -> ScriptedServer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let accepted = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&accepted);
    tokio::spawn(async move {
        for script in scripts {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            counter.fetch_add(1, Ordering::SeqCst);
            run_script(&mut stream, script).await;
            // stream drops here => close (unless the script hung forever)
        }
    });
    ScriptedServer { addr, accepted }
}

async fn run_script(stream: &mut TcpStream, steps: Vec<Step>) {
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
}

fn count_terminators(bytes: &[u8]) -> usize {
    bytes.windows(2).filter(|w| *w == b"\r\n").count()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::DownReason;

    #[tokio::test]
    async fn serial_server_accepts_one_connection_per_script() {
        let server = scripted_backend_serial(vec![vec![Step::Close], vec![Step::Close]]).await;

        let c1 = TcpStream::connect(server.addr).await.unwrap();
        drop(c1);
        let c2 = TcpStream::connect(server.addr).await.unwrap();
        drop(c2);

        // accepts happen on the spawned task; give it a beat
        while server.accept_count() < 2 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(server.accept_count(), 2);
    }

    #[tokio::test]
    async fn event_log_records_in_order() {
        let (sink, log) = event_log();
        sink(ConnectionEvent::Up);
        sink(ConnectionEvent::Down(DownReason::Eof));
        sink(ConnectionEvent::Closed);
        assert_eq!(
            *log.borrow(),
            vec![
                ConnectionEvent::Up,
                ConnectionEvent::Down(DownReason::Eof),
                ConnectionEvent::Closed,
            ]
        );
    }
}
