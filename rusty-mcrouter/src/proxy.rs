use std::net::SocketAddr;
use std::sync::Arc;

use rusty_mcrouter_core::RoutingMetricsLayout;
use rusty_mcrouter_observability::EventSender;
use rusty_mcrouter_proxy::{
    proxy_thread_main, ListenerConfig, ProxyHandle, ProxySet, ProxyShards, ProxyShared,
    ProxyThreadConfig,
};

use crate::control::{ProcessEvent, Supervisor};

pub struct ProxyThread {
    handle: ProxyHandle,
    join: Option<std::thread::JoinHandle<anyhow::Result<()>>>,
}

impl ProxyThread {
    pub fn spawn(
        handle: ProxyHandle,
        config: ProxyThreadConfig,
        supervisor: &Supervisor,
    ) -> anyhow::Result<(Self, Option<SocketAddr>)> {
        let proxy_id = config.proxy_id;
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        let exit = supervisor.exit_notifier(ProcessEvent::ProxyExited { id: proxy_id });
        let join = std::thread::Builder::new()
            .name(format!("proxy-{proxy_id}"))
            .spawn(move || {
                let _exit = exit;
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

pub struct ProxyFleetConfig {
    pub num_proxies: usize,
    pub num_listening_sockets: usize,
    pub listen_addr: SocketAddr,
    pub shared: Arc<ProxyShared>,
    pub events: EventSender,
}

pub struct ProxyFleet {
    threads: Vec<ProxyThread>,
    shards: Vec<ProxyShards>,
    bound_addr: SocketAddr,
}

impl ProxyFleet {
    /// on failure shuts down what it already started
    pub fn spawn(cfg: ProxyFleetConfig, supervisor: &Supervisor) -> anyhow::Result<Self> {
        let routing_layout = RoutingMetricsLayout::new(&cfg.shared.config);
        let (handles, inboxes): (Vec<_>, Vec<_>) =
            (0..cfg.num_proxies).map(ProxyHandle::allocate).unzip();
        let proxies = ProxySet::new(handles.clone());
        // created here so the scrape sources hold the same Arcs the threads write
        let shards: Vec<_> = (0..cfg.num_proxies)
            .map(|_| ProxyShards::new(Arc::clone(&routing_layout)))
            .collect();

        let use_reuseport = cfg.num_listening_sockets > 1;
        let mut threads = Vec::with_capacity(cfg.num_proxies);
        let mut bound_addr: Option<SocketAddr> = None;

        for (proxy_id, ((handle, inbox), shards)) in
            handles.into_iter().zip(inboxes).zip(&shards).enumerate()
        {
            let listener = (proxy_id < cfg.num_listening_sockets).then_some(ListenerConfig {
                listen_addr: cfg.listen_addr,
                use_reuseport,
            });
            let thread_cfg = ProxyThreadConfig {
                proxy_id,
                inbox,
                shards: shards.clone(),
                shared: Arc::clone(&cfg.shared),
                proxies: proxies.clone(),
                listener,
                routing_events: cfg.events.sink(),
                events: cfg.events.sink(),
            };

            match ProxyThread::spawn(handle, thread_cfg, supervisor) {
                Ok((thread, addr)) => {
                    if let Some(addr) = addr {
                        bound_addr.get_or_insert(addr);
                    }
                    threads.push(thread);
                }
                Err(error) => {
                    let _ = shutdown_all(threads);
                    return Err(error);
                }
            }
        }

        // threads keep their own clones; the queues stay open until they exit
        drop((proxies, cfg.events));

        let bound_addr = match bound_addr {
            Some(addr) => addr,
            None => {
                let _ = shutdown_all(threads);
                anyhow::bail!("no proxy thread reported a bound address");
            }
        };

        Ok(Self {
            threads,
            shards,
            bound_addr,
        })
    }

    pub fn bound_addr(&self) -> SocketAddr {
        self.bound_addr
    }

    pub fn shards(&self) -> Vec<ProxyShards> {
        self.shards.clone()
    }

    pub fn shutdown(self) -> anyhow::Result<()> {
        shutdown_all(self.threads)
    }
}

fn shutdown_all(threads: Vec<ProxyThread>) -> anyhow::Result<()> {
    let mut first_error = None;
    for thread in threads {
        if let Err(error) = thread.shutdown() {
            first_error.get_or_insert(error);
        }
    }
    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}
