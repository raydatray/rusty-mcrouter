use std::sync::Arc;

use rusty_mcrouter_config::ConfigDocument;
use rusty_mcrouter_core::BuildError;
use rusty_mcrouter_protocol::{Reply, Request};
use tokio::sync::oneshot;

pub enum ProxyCommand {
    Shutdown {
        acknowledged: oneshot::Sender<()>,
    },
    /// `applied` answers after the swap; on error the proxy keeps its graph.
    Reconfigure {
        generation: u64,
        config: Arc<ConfigDocument>,
        applied: oneshot::Sender<Result<(), BuildError>>,
    },
}

pub struct ProxyRequest {
    pub request: Request,
    pub reply_tx: oneshot::Sender<Reply>,
}
