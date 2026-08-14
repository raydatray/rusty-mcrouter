use std::io;

use rusty_mcrouter_protocol::{meta::MetaReplyExpectation, Reply, Request};
use tokio::{sync::oneshot, time::Instant};

use crate::error::{ConnectError, SendError};

/// reported to the owning destination
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

pub(crate) enum Payload {
    Request(Request),
    VersionProbe,
}

pub(crate) struct Command {
    pub(crate) payload: Payload,
    pub(crate) reply_tx: oneshot::Sender<Result<Reply, SendError>>,
    pub(crate) deadline: Option<Instant>, // armed at enqueue time
}

pub(crate) enum ConnectionCommand {
    Command(Command),
    CloseIdle,
}

pub(crate) struct Inflight {
    pub(crate) expectation: MetaReplyExpectation,
    pub(crate) reply_tx: Option<oneshot::Sender<Result<Reply, SendError>>>, // none means this timed out
    pub(crate) deadline: Option<Instant>,
}
