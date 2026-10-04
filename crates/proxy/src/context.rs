use std::{rc::Rc, sync::Arc};

use rusty_mcrouter_frontend::{FrontendConnectionOptions, FrontendMetricsShard};
use tokio::sync::mpsc::Sender;

use crate::generation::RouteGeneration;
use crate::{ProxyRequest, ProxySet, ThreadMode};

/// Worker-local routing state and inputs for constructing frontend connections.
pub(crate) struct ProxyContext {
    pub(crate) proxy_id: usize,
    pub(crate) routes: Rc<RouteGeneration>,
    pub(crate) proxies: ProxySet,
    pub(crate) thread_mode: ThreadMode,
    pub(crate) metrics: Arc<FrontendMetricsShard>,
    pub(crate) connection_options: FrontendConnectionOptions,
}

impl ProxyContext {
    pub(crate) fn request_sender(&self) -> Sender<ProxyRequest> {
        self.proxies
            .choose(self.thread_mode, self.proxy_id)
            .request_sender()
    }
}

#[cfg(test)]
impl ProxyContext {
    /// Proxy 0 of a one-proxy set, using its own request mailbox.
    pub(crate) fn solo(
        handle: crate::ProxyHandle,
        routes: Rc<RouteGeneration>,
        metrics: Arc<FrontendMetricsShard>,
    ) -> Self {
        Self {
            proxy_id: handle.id(),
            routes,
            proxies: ProxySet::new(vec![handle]),
            thread_mode: ThreadMode::SameThread,
            metrics,
            connection_options: FrontendConnectionOptions::default(),
        }
    }
}
