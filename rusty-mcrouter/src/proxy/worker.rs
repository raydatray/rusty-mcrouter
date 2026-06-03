use std::rc::Rc;

use rusty_mcrouter_core::DynRoute;
use tokio::sync::mpsc;

use crate::proxy::{config::ThreadMode, connection::Connection, proxy_set::ProxySet};

/// a proxy thread's socket-handoff loop:
/// - drains the per-thread socket queue
/// - re-registers each handed-off socket on this runtime
/// - spawns a `Connection` task per accepted connection.
pub struct ConnectionWorker {
    current_id: usize,
    local_route: Rc<dyn DynRoute>,
    proxies: ProxySet,
    mode: ThreadMode,
    work_rx: mpsc::Receiver<std::net::TcpStream>,
}

impl ConnectionWorker {
    pub fn new(
        current_id: usize,
        local_route: Rc<dyn DynRoute>,
        proxies: ProxySet,
        mode: ThreadMode,
        work_rx: mpsc::Receiver<std::net::TcpStream>,
    ) -> Self {
        Self {
            current_id,
            local_route,
            proxies,
            mode,
            work_rx,
        }
    }

    pub async fn run(mut self) {
        while let Some(std_stream) = self.work_rx.recv().await {
            let tokio_stream = match tokio::net::TcpStream::from_std(std_stream) {
                Ok(s) => s,
                Err(e) => {
                    // todo - logger
                    eprintln!("could not reregister accepted stream on worker runtime: {e}");
                    continue;
                }
            };

            let connection = Connection::new(
                tokio_stream,
                self.current_id,
                Rc::clone(&self.local_route),
                self.proxies.clone(),
                self.mode,
            );

            tokio::task::spawn_local(async move {
                if let Err(e) = connection.run().await {
                    // todo - logger
                    eprintln!("connection error: {e}");
                }
            });
        }
    }
}
