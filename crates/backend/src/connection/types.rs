use rusty_mcrouter_protocol::{meta::MetaReplyExpectation, Reply, Request};
use tokio::{sync::oneshot, time::Instant};

use crate::error::SendError;

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
