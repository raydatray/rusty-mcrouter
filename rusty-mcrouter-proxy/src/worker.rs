use std::{rc::Rc, sync::Arc};

use rusty_mcrouter_core::{DynRoute, RoutingState};
use tokio::sync::mpsc;

use crate::{
    config::ThreadMode, connection::Connection, proxy_set::ProxySet, FrontendMetricsShard,
};

/// a proxy thread's socket-handoff loop:
/// - drains the per-thread socket queue
/// - re-registers each handed-off socket on this runtime
/// - spawns a `Connection` task per accepted connection.
pub struct ConnectionWorker {
    current_id: usize,
    local_route: Rc<dyn DynRoute>,
    proxies: ProxySet,
    mode: ThreadMode,
    metrics: Arc<FrontendMetricsShard>,
    routing_state: Rc<RoutingState>,
    work_rx: mpsc::Receiver<std::net::TcpStream>,
}

impl ConnectionWorker {
    pub fn new(
        current_id: usize,
        local_route: Rc<dyn DynRoute>,
        proxies: ProxySet,
        mode: ThreadMode,
        metrics: Arc<FrontendMetricsShard>,
        routing_state: Rc<RoutingState>,
        work_rx: mpsc::Receiver<std::net::TcpStream>,
    ) -> Self {
        Self {
            current_id,
            local_route,
            proxies,
            mode,
            metrics,
            routing_state,
            work_rx,
        }
    }

    pub async fn run(mut self) {
        while let Some(std_stream) = self.work_rx.recv().await {
            let tokio_stream = match tokio::net::TcpStream::from_std(std_stream) {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(worker = self.current_id, error = %e, "could not reregister accepted stream on worker runtime");
                    continue;
                }
            };

            let connection = Connection::new(
                tokio_stream,
                self.current_id,
                Rc::clone(&self.local_route),
                Rc::clone(&self.routing_state),
                self.proxies.clone(),
                self.mode,
                Arc::clone(&self.metrics),
            );

            let metrics = Arc::clone(&self.metrics);
            metrics.client_connections.inc();

            tokio::task::spawn_local(async move {
                if let Err(e) = connection.run().await {
                    tracing::warn!(error = %e, "connection error");
                }
                metrics.client_connections.dec();
            });
        }
    }
}
