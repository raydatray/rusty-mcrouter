use std::{rc::Rc, sync::Arc};

use rusty_mcrouter_protocol::Request;

use crate::generation::RouteSlot;
use crate::routing::RouteTarget;
use crate::{FrontendMetricsShard, ProxySet, ThreadMode};

/// This proxy thread as seen by its connections: identity, routing and
/// frontend metrics. Thread-local; cloning is refcount bumps.
#[derive(Clone)]
pub(crate) struct ProxyContext {
    pub(crate) proxy_id: usize,
    pub(crate) routes: Rc<RouteSlot>,
    pub(crate) proxies: ProxySet,
    pub(crate) thread_mode: ThreadMode,
    pub(crate) metrics: Arc<FrontendMetricsShard>,
}

impl ProxyContext {
    /// A request for this thread is pinned to its current route graph now.
    pub(crate) fn target(&self, request: &Request) -> RouteTarget {
        let handle = self
            .proxies
            .choose(self.thread_mode, self.proxy_id, request);
        if handle.id() == self.proxy_id {
            RouteTarget::Local(self.routes.current())
        } else {
            RouteTarget::Remote(handle)
        }
    }
}

#[cfg(test)]
impl ProxyContext {
    /// Proxy 0 of a one-proxy set, routing on its own thread.
    pub(crate) fn solo(
        handle: crate::ProxyHandle,
        routes: Rc<RouteSlot>,
        metrics: Arc<FrontendMetricsShard>,
    ) -> Self {
        Self {
            proxy_id: handle.id(),
            routes,
            proxies: ProxySet::new(vec![handle]),
            thread_mode: ThreadMode::SameThread,
            metrics,
        }
    }
}
