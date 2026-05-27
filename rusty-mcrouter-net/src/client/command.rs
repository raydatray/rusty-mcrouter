use crate::Result;
use rusty_mcrouter_protocol::{Reply, Request};
use tokio::sync::oneshot;

// todo - consolidate to enum when we add shutdown or throttle commands
pub(crate) struct ClientCommand {
    pub request: Request,
    pub reply_tx: oneshot::Sender<Result<Reply>>,
}
