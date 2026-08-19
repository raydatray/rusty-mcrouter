use std::net::SocketAddr;

use rusty_mcrouter_proxy::{proxy_thread_main, ProxyHandle, ProxyThreadConfig};

use crate::control::{ExitNotifier, ProcessEvent};

pub struct ProxyThread {
    handle: ProxyHandle,
    join: Option<std::thread::JoinHandle<anyhow::Result<()>>>,
}

impl ProxyThread {
    pub fn spawn(
        handle: ProxyHandle,
        config: ProxyThreadConfig,
        process_events: std::sync::mpsc::Sender<ProcessEvent>,
    ) -> anyhow::Result<(Self, Option<SocketAddr>)> {
        let proxy_id = config.proxy_id;
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        let join = std::thread::Builder::new()
            .name(format!("proxy-{proxy_id}"))
            .spawn(move || {
                let _exit =
                    ExitNotifier::new(process_events, ProcessEvent::ProxyExited { id: proxy_id });
                proxy_thread_main(config, ready_tx)
            })?;

        let bound_addr = match ready_rx.recv() {
            Ok(result) => result?,
            Err(_) => anyhow::bail!("proxy-{proxy_id} died during startup"),
        };

        Ok((
            Self {
                handle,
                join: Some(join),
            },
            bound_addr,
        ))
    }

    pub fn shutdown(mut self) -> anyhow::Result<()> {
        let shutdown = if self
            .join
            .as_ref()
            .is_some_and(std::thread::JoinHandle::is_finished)
        {
            Ok(())
        } else {
            self.handle.shutdown_blocking()
        };
        let joined = self
            .join
            .take()
            .expect("proxy thread exists")
            .join()
            .map_err(|_| anyhow::anyhow!("proxy thread panicked"))?;
        shutdown.and(joined)
    }
}
