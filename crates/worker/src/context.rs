use std::{rc::Rc, sync::Arc};

use rusty_mcrouter_frontend::{FrontendConnectionOptions, FrontendMetricsShard};
use tokio::sync::mpsc::Sender;

use crate::generation::RouteGeneration;
use crate::{RoutedRequest, ThreadMode, WorkerSet};

/// Worker-local routing state and inputs for constructing frontend connections.
pub(crate) struct WorkerContext {
    pub(crate) worker_id: usize,
    pub(crate) routes: Rc<RouteGeneration>,
    pub(crate) workers: WorkerSet,
    pub(crate) thread_mode: ThreadMode,
    pub(crate) metrics: Arc<FrontendMetricsShard>,
    pub(crate) connection_options: FrontendConnectionOptions,
}

impl WorkerContext {
    pub(crate) fn request_sender(&self) -> Sender<RoutedRequest> {
        self.workers
            .choose(self.thread_mode, self.worker_id)
            .request_sender()
    }
}

#[cfg(test)]
impl WorkerContext {
    /// Worker 0 of a one-worker set, using its own request mailbox.
    pub(crate) fn solo(
        handle: crate::WorkerHandle,
        routes: Rc<RouteGeneration>,
        metrics: Arc<FrontendMetricsShard>,
    ) -> Self {
        Self {
            worker_id: handle.id(),
            routes,
            workers: WorkerSet::new(vec![handle]),
            thread_mode: ThreadMode::SameThread,
            metrics,
            connection_options: FrontendConnectionOptions::default(),
        }
    }
}
