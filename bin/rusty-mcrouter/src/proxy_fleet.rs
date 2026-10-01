use std::net::SocketAddr;
use std::sync::{mpsc::sync_channel, Arc};
use std::thread::{Builder, JoinHandle};

use anyhow::Context;
use rusty_mcrouter_observability::EventSender;
use rusty_mcrouter_proxy::{
    proxy_thread_main, ListenerConfig, ProxyHandle, ProxyInbox, ProxySet, ProxyShards, ProxyShared,
    ProxyThreadConfig,
};

use crate::control::{ProcessEvent, Supervisor};

pub struct ProxyThread {
    handle: ProxyHandle,
    join: Option<JoinHandle<anyhow::Result<()>>>,
}

impl ProxyThread {
    pub fn spawn(
        handle: ProxyHandle,
        config: ProxyThreadConfig,
        supervisor: &Supervisor,
    ) -> anyhow::Result<(Self, Option<SocketAddr>)> {
        let proxy_id = config.proxy_id;
        let (ready_tx, ready_rx) = sync_channel(1);
        let exit = supervisor.exit_notifier(ProcessEvent::ProxyExited { id: proxy_id });
        let join = Builder::new()
            .name(format!("proxy-{proxy_id}"))
            .spawn(move || {
                let _exit = exit;
                proxy_thread_main(config, ready_tx)
            })?;

        let started = ready_rx
            .recv()
            .with_context(|| format!("proxy-{proxy_id} died during startup"))
            .and_then(|result| result);
        let bound_addr = match started {
            Ok(addr) => addr,
            Err(error) => {
                let _ = join.join();
                return Err(error);
            }
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
        let shutdown = if self.join.as_ref().is_some_and(JoinHandle::is_finished) {
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

pub struct ProxyWorkerInputs {
    pub handle: ProxyHandle,
    pub inbox: ProxyInbox,
    pub shards: ProxyShards,
}

pub struct ProxyFleetConfig {
    pub workers: Vec<ProxyWorkerInputs>,
    pub num_listening_sockets: usize,
    pub listen_addr: SocketAddr,
    pub shared: Arc<ProxyShared>,
    pub events: EventSender,
}

pub struct ProxyFleet {
    threads: Vec<ProxyThread>,
    bound_addr: SocketAddr,
}

impl ProxyFleet {
    /// on failure shuts down what it already started
    pub fn spawn(cfg: ProxyFleetConfig, supervisor: &Supervisor) -> anyhow::Result<Self> {
        let proxies = ProxySet::new(
            cfg.workers
                .iter()
                .map(|worker| worker.handle.clone())
                .collect(),
        );

        let use_reuseport = cfg.num_listening_sockets > 1;
        let mut threads = Vec::with_capacity(cfg.workers.len());
        let mut bound_addr: Option<SocketAddr> = None;

        for (proxy_id, worker) in cfg.workers.into_iter().enumerate() {
            let ProxyWorkerInputs {
                handle,
                inbox,
                shards,
            } = worker;
            let listener = (proxy_id < cfg.num_listening_sockets).then_some(ListenerConfig {
                listen_addr: cfg.listen_addr,
                use_reuseport,
            });
            let thread_cfg = ProxyThreadConfig {
                proxy_id,
                inbox,
                shards,
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
            bound_addr,
        })
    }

    pub fn bound_addr(&self) -> SocketAddr {
        self.bound_addr
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
