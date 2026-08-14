use std::future::Future;

use rusty_mcrouter_protocol::{Reply, Request};

use crate::Result;

/// A backend that sends a request and awaits a reply.
///
/// Generic, not `dyn`: `DestinationRoute<B>` picks the concrete backend at the
/// call site, so there is no boxed future on the hot path. Non-`Send`, mirroring
/// `Route` (the graph is single-threaded `Rc` on a `LocalSet`).
///
/// The production impl will be `Rc<Destination>` (TKO fast-fail + the
/// reconnecting `ConnectionHandle`); tests use `MockBackend`. Both this trait
/// and `BackendFactory` migrate from `NetError` to `SendError` (and `connect`
/// becomes a sync, I/O-free `make`) when the Destination layer lands.
pub trait Backend: 'static {
    fn send(&self, req: Request) -> impl Future<Output = Result<Reply>>;
}

/// Constructs a [`Backend`] for a server address — the seam that lets the route
/// builder run over mock backends without opening sockets. The associated
/// `Backend` type keeps dispatch static, with no default hidden behind it.
pub trait BackendFactory {
    type Backend: Backend;

    fn connect(&self, addr: &str) -> impl Future<Output = Result<Self::Backend>>;
}
