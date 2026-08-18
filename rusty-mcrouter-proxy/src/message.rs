use rusty_mcrouter_protocol::{Reply, Request};
use tokio::sync::oneshot;

pub enum ProxyMessage {
    Request(ProxyRequest),
    // todo - graceful shutdown: only ProxyHandle::shutdown constructs this
    #[allow(dead_code)]
    Shutdown,
}

pub struct ProxyRequest {
    pub request: Request,
    pub reply_tx: oneshot::Sender<Reply>,
}
