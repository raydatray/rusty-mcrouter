use std::sync::Arc;

use rusty_mcrouter_config::ConfigDocument;
use rusty_mcrouter_core::BuildError;
use tokio::sync::oneshot;

pub enum WorkerCommand {
    Shutdown {
        acknowledged: oneshot::Sender<()>,
    },
    /// `applied` answers after the swap; on error the worker keeps its graph.
    Reconfigure {
        generation: u64,
        config: Arc<ConfigDocument>,
        applied: oneshot::Sender<Result<(), BuildError>>,
    },
}
