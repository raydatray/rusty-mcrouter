use std::net::SocketAddr;
use std::sync::{
    mpsc::{sync_channel, SyncSender},
    Arc,
};
use std::thread::{Builder, JoinHandle};

use anyhow::Context;
use rusty_mcrouter_observability::EventSender;
use rusty_mcrouter_proxy::{
    ListenerConfig, ProxyHandle, ProxyInbox, ProxySet, ProxyShards, ProxyShared, ProxyThreadSetup,
    ProxyWorker,
};

use crate::control::{ProcessEvent, Supervisor};
use crate::startup::report_startup;

pub struct ProxyWorkerResources {
    pub handle: ProxyHandle,
    pub inbox: ProxyInbox,
    pub shards: ProxyShards,
}

pub struct ProxyFleetSetup {
    pub workers: Vec<ProxyWorkerResources>,
    pub num_listening_sockets: usize,
    pub listen_addr: SocketAddr,
    pub shared: Arc<ProxyShared>,
    pub events: EventSender,
}

/// External lifetime owner; its handle is a cleanup capability, not a runtime input.
pub struct ProxyThreadOwner {
    handle: ProxyHandle,
    join: Option<JoinHandle<anyhow::Result<()>>>,
}

pub struct ProxyFleet {
    threads: Vec<ProxyThreadOwner>,
    bound_addr: SocketAddr,
}

impl ProxyThreadOwner {
    pub fn spawn(
        handle: ProxyHandle,
        setup: ProxyThreadSetup,
        supervisor: &Supervisor,
    ) -> anyhow::Result<(Self, Option<SocketAddr>)> {
        let proxy_id = setup.proxy_id;
        let (ready_tx, ready_rx) = sync_channel(1);
        let exit = supervisor.exit_notifier(ProcessEvent::ProxyExited { id: proxy_id });
        let join = Builder::new()
            .name(format!("proxy-{proxy_id}"))
            .spawn(move || {
                let _exit = exit;
                proxy_thread_main(setup, ready_tx)
            })?;

        let owner = Self {
            handle,
            join: Some(join),
        };
        let bound_addr = ready_rx
            .recv()
            .with_context(|| format!("proxy-{proxy_id} died during startup"))
            .and_then(|result| result)?;

        Ok((owner, bound_addr))
    }

    pub fn shutdown(mut self) -> anyhow::Result<()> {
        self.stop()
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        let Some(join) = self.join.take() else {
            return Ok(());
        };
        let shutdown = if join.is_finished() {
            Ok(())
        } else {
            self.handle.shutdown_blocking()
        };
        let joined = join
            .join()
            .map_err(|_| anyhow::anyhow!("proxy thread panicked"))?;
        shutdown.and(joined)
    }
}

impl Drop for ProxyThreadOwner {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

impl ProxyFleet {
    /// on failure shuts down what it already started
    pub fn spawn(setup: ProxyFleetSetup, supervisor: &Supervisor) -> anyhow::Result<Self> {
        let proxies = ProxySet::new(
            setup
                .workers
                .iter()
                .map(|worker| worker.handle.clone())
                .collect(),
        );

        let use_reuseport = setup.num_listening_sockets > 1;
        let spawn_worker = |(proxy_id, worker): (usize, ProxyWorkerResources)| {
            let ProxyWorkerResources {
                handle,
                inbox,
                shards,
            } = worker;
            let listener = (proxy_id < setup.num_listening_sockets).then_some(ListenerConfig {
                listen_addr: setup.listen_addr,
                use_reuseport,
            });
            let thread_cfg = ProxyThreadSetup {
                proxy_id,
                inbox,
                shards,
                shared: Arc::clone(&setup.shared),
                proxies: proxies.clone(),
                listener,
                routing_events: setup.events.sink(),
                events: setup.events.sink(),
            };

            ProxyThreadOwner::spawn(handle, thread_cfg, supervisor)
        };

        let (threads, addresses): (Vec<_>, Vec<_>) = setup
            .workers
            .into_iter()
            .enumerate()
            .map(spawn_worker)
            .collect::<anyhow::Result<_>>()?;

        // threads keep their own clones; the queues stay open until they exit
        drop((proxies, setup.events));

        let bound_addr = addresses
            .into_iter()
            .flatten()
            .next()
            .context("no proxy thread reported a bound address")?;

        Ok(Self {
            threads,
            bound_addr,
        })
    }

    pub fn bound_addr(&self) -> SocketAddr {
        self.bound_addr
    }

    pub fn shutdown(self) -> anyhow::Result<()> {
        let mut outcome = Ok(());

        for thread in self.threads {
            let stopped = thread.shutdown();
            outcome = outcome.and(stopped);
        }

        outcome
    }
}

fn proxy_thread_main(
    setup: ProxyThreadSetup,
    ready_tx: SyncSender<anyhow::Result<Option<SocketAddr>>>,
) -> anyhow::Result<()> {
    let prepared = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .context("create proxy executor")
        .and_then(|executor| {
            let local = tokio::task::LocalSet::new();
            let worker = local.block_on(&executor, ProxyWorker::build(setup))?;
            Ok((executor, local, worker))
        });
    let (executor, local, worker) =
        report_startup(prepared, ready_tx, |(_, _, worker)| worker.bound_addr())?;
    local.block_on(&executor, worker.run())
}

#[cfg(test)]
mod tests {
    use std::{cell::Cell, net::TcpListener, time::Duration};

    use rusty_mcrouter_backend::{
        destination::DestinationMetricsRegistry,
        tko::{DestTokenAllocator, TkoTrackerMap},
    };
    use rusty_mcrouter_observability::{channel, ControlMetrics};
    use rusty_mcrouter_proxy::ThreadMode;

    use super::*;

    #[test]
    fn failed_collection_joins_previously_started_proxies() {
        let (events, _consumer) = channel(8, Arc::new(ControlMetrics::default()));
        let shared = |document: &str| {
            Arc::new(ProxyShared {
                tokens: Arc::new(DestTokenAllocator::new()),
                config: Arc::new(crate::config::parse(document.as_bytes()).unwrap()),
                tko_map: TkoTrackerMap::new(events.sink()),
                destinations: DestinationMetricsRegistry::new(),
                defaults: Default::default(),
                root_route_options: Default::default(),
                sweep_interval: Duration::ZERO,
                thread_mode: ThreadMode::SameThread,
                connection_options: Default::default(),
            })
        };
        let configurations = [
            shared(r#"{ "route": "NullRoute" }"#),
            shared(r#"{ "routes": { "/a/b/": "NullRoute" } }"#),
        ];
        let (handles, inboxes): (Vec<_>, Vec<_>) = (0..2).map(ProxyHandle::allocate).unzip();
        let peers = ProxySet::new(handles.clone());
        let supervisor = Supervisor::new();
        let bound = Cell::new(None);
        let result = handles
            .into_iter()
            .zip(inboxes)
            .zip(configurations)
            .enumerate()
            .map(|(proxy_id, ((handle, inbox), shared))| {
                ProxyThreadOwner::spawn(
                    handle,
                    ProxyThreadSetup {
                        proxy_id,
                        inbox,
                        shards: ProxyShards::new(),
                        shared,
                        proxies: peers.clone(),
                        listener: (proxy_id == 0).then_some(ListenerConfig {
                            listen_addr: "127.0.0.1:0".parse().unwrap(),
                            use_reuseport: false,
                        }),
                        routing_events: events.sink(),
                        events: events.sink(),
                    },
                    &supervisor,
                )
                .inspect(|(_, address)| {
                    if address.is_some() {
                        bound.set(*address);
                    }
                })
            })
            .collect::<anyhow::Result<Vec<_>>>();
        assert!(result.is_err());
        TcpListener::bind(bound.get().expect("first proxy never started"))
            .expect("failed collection left an earlier proxy listening");
    }
}
