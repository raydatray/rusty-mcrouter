use crate::Result;
use rusty_mcrouter_protocol::{Reply, Request};
use tokio::sync::oneshot;

// todo - consolidate to enum when we add shutdown or throttle commands
pub(crate) enum ClientCommand {
    Request {
        request: Request,
        reply_tx: oneshot::Sender<Result<Reply>>,
    },
    VersionProbe {
        reply_tx: oneshot::Sender<Result<Reply>>,
    },
}
