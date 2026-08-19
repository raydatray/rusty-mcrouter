use rusty_mcrouter_protocol::{Reply, Request};
use tokio::sync::oneshot;

pub enum ProxyCommand {
    Shutdown { acknowledged: oneshot::Sender<()> },
}

pub struct ProxyRequest {
    pub request: Request,
    pub reply_tx: oneshot::Sender<Reply>,
}
