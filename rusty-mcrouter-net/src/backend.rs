use std::future::Future;

use rusty_mcrouter_protocol::{Reply, Request};

use crate::{Client, Result};

/// A backend that sends a request and awaits a reply.
///
/// Generic, not `dyn`: `DestinationRoute<B>` picks the concrete backend at the
/// call site, so there is no boxed future on the hot path. Non-`Send`, mirroring
/// `Route` (the graph is single-threaded `Rc` on a `LocalSet`). Production impl
/// is [`Client`]; tests use `MockBackend`.
pub trait Backend: 'static {
    fn send(&self, req: Request) -> impl Future<Output = Result<Reply>>;
}

impl Backend for Client {
    // Explicit path, not `self.send(...)`, to avoid recursing into this trait method.
    async fn send(&self, req: Request) -> Result<Reply> {
        Client::send(self, req).await
    }
}

/// Constructs a [`Backend`] for a server address — the seam that lets the route
/// builder run over mock backends without opening sockets. The associated
/// `Backend` type keeps dispatch static, with no default hidden behind it.
pub trait BackendFactory {
    type Backend: Backend;

    fn connect(&self, addr: &str) -> impl Future<Output = Result<Self::Backend>>;
}

/// Production factory: opens a pipelined [`Client`] TCP connection.
pub struct ClientFactory;

impl BackendFactory for ClientFactory {
    type Backend = Client;

    async fn connect(&self, addr: &str) -> Result<Client> {
        Client::connect(addr).await
    }
}
