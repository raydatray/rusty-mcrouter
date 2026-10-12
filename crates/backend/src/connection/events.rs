use std::io;

use crate::error::ConnectError;

/// reported to the owning destination
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectionEvent {
    Up,
    Down(DownReason), // hard TKO
    Closed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DownReason {
    ConnectFailed(ConnectError),
    Stream(io::ErrorKind),
    Eof,
    Protocol,
}

/// synchronous, worker-local handler for connection events
pub struct ConnectionEventHandler(Box<dyn Fn(ConnectionEvent)>);

impl ConnectionEventHandler {
    pub fn new(handler: impl Fn(ConnectionEvent) + 'static) -> Self {
        Self(Box::new(handler))
    }

    pub fn handle(&self, event: ConnectionEvent) {
        (self.0)(event);
    }
}
