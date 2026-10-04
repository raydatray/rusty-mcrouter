use std::net::SocketAddr;
use std::sync::{
    mpsc::{sync_channel, SyncSender},
    Arc,
};
use std::thread::{Builder, JoinHandle};

use anyhow::Context;
use rusty_mcrouter_frontend::ListenerConfig;
use rusty_mcrouter_observability::EventSender;
use rusty_mcrouter_worker::{
    Worker, WorkerHandle, WorkerInbox, WorkerSet, WorkerSetup, WorkerShards, WorkerShared,
};

use crate::lifecycle::{report_startup, ProcessEvent, Supervisor};

pub struct WorkerResources {
    pub handle: WorkerHandle,
    pub inbox: WorkerInbox,
    pub shards: WorkerShards,
}

pub struct WorkerFleetSetup {
    pub workers: Vec<WorkerResources>,
    pub num_listening_sockets: usize,
    pub listen_addr: SocketAddr,
    pub shared: Arc<WorkerShared>,
    pub events: EventSender,
}

/// External lifetime owner; its handle is a cleanup capability, not a runtime input.
pub struct WorkerThreadOwner {
    handle: WorkerHandle,
    join: Option<JoinHandle<anyhow::Result<()>>>,
}

pub struct WorkerFleet {
    threads: Vec<WorkerThreadOwner>,
    bound_addr: SocketAddr,
}

impl WorkerThreadOwner {
    pub fn spawn(
        handle: WorkerHandle,
        setup: WorkerSetup,
        supervisor: &Supervisor,
    ) -> anyhow::Result<(Self, Option<SocketAddr>)> {
        let worker_id = setup.worker_id;
        let (ready_tx, ready_rx) = sync_channel(1);
        let exit = supervisor.exit_notifier(ProcessEvent::WorkerExited { id: worker_id });
        let join = Builder::new()
            .name(format!("worker-{worker_id}"))
            .spawn(move || {
                let _exit = exit;
                worker_thread_main(setup, ready_tx)
            })?;

        let owner = Self {
            handle,
            join: Some(join),
        };
        let bound_addr = ready_rx
            .recv()
            .with_context(|| format!("worker-{worker_id} died during startup"))
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
            .map_err(|_| anyhow::anyhow!("worker thread panicked"))?;
        shutdown.and(joined)
    }
}

impl Drop for WorkerThreadOwner {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

impl WorkerFleet {
    /// on failure shuts down what it already started
    pub fn spawn(setup: WorkerFleetSetup, supervisor: &Supervisor) -> anyhow::Result<Self> {
        let workers = WorkerSet::new(
            setup
                .workers
                .iter()
                .map(|worker| worker.handle.clone())
                .collect(),
        );

        let use_reuseport = setup.num_listening_sockets > 1;
        let spawn_worker = |(worker_id, worker): (usize, WorkerResources)| {
            let WorkerResources {
                handle,
                inbox,
                shards,
            } = worker;
            let listener = (worker_id < setup.num_listening_sockets).then_some(ListenerConfig {
                listen_addr: setup.listen_addr,
                use_reuseport,
            });
            let thread_cfg = WorkerSetup {
                worker_id,
                inbox,
                shards,
                shared: Arc::clone(&setup.shared),
                workers: workers.clone(),
                listener,
                routing_events: setup.events.sink(),
                events: setup.events.sink(),
            };

            WorkerThreadOwner::spawn(handle, thread_cfg, supervisor)
        };

        let (threads, addresses): (Vec<_>, Vec<_>) = setup
            .workers
            .into_iter()
            .enumerate()
            .map(spawn_worker)
            .collect::<anyhow::Result<_>>()?;

        // threads keep their own clones; the queues stay open until they exit
        drop((workers, setup.events));

        let bound_addr = addresses
            .into_iter()
            .flatten()
            .next()
            .context("no worker thread reported a bound address")?;

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

fn worker_thread_main(
    setup: WorkerSetup,
    ready_tx: SyncSender<anyhow::Result<Option<SocketAddr>>>,
) -> anyhow::Result<()> {
    let prepared = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .context("create worker executor")
        .and_then(|executor| {
            let local = tokio::task::LocalSet::new();
            let worker = local.block_on(&executor, Worker::build(setup))?;
            Ok((executor, local, worker))
        });
    let (executor, local, worker) =
        report_startup(prepared, ready_tx, |(_, _, worker)| worker.bound_addr())?;
    local.block_on(&executor, worker.run())
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::{cell::Cell, net::TcpListener, time::Duration};

    use rusty_mcrouter_backend::{
        destination::DestinationMetricsRegistry,
        tko::{DestTokenAllocator, TkoTrackerMap},
    };
    use rusty_mcrouter_observability::{channel, ControlMetrics};
    use rusty_mcrouter_protocol::RequestKind;
    use rusty_mcrouter_worker::ThreadMode;

    use super::*;

    #[test]
    fn frontend_connection_submits_to_a_remote_worker_thread() {
        let (events, _consumer) = channel(8, Arc::new(ControlMetrics::default()));
        let shared = |document: &str| {
            Arc::new(WorkerShared {
                tokens: Arc::new(DestTokenAllocator::new()),
                config: Arc::new(crate::config::parse(document.as_bytes()).unwrap()),
                tko_map: TkoTrackerMap::new(events.sink()),
                destinations: DestinationMetricsRegistry::new(),
                defaults: Default::default(),
                root_route_options: Default::default(),
                sweep_interval: Duration::ZERO,
                thread_mode: ThreadMode::FixedRemote { worker_id: 1 },
                connection_options: Default::default(),
            })
        };
        let (local_handle, local_inbox) = WorkerHandle::allocate(0);
        let (remote_handle, remote_inbox) = WorkerHandle::allocate(1);
        let workers = WorkerSet::new(vec![local_handle.clone(), remote_handle.clone()]);
        let local_shards = WorkerShards::new();
        let remote_shards = WorkerShards::new();
        let supervisor = Supervisor::new();

        let (local, address) = WorkerThreadOwner::spawn(
            local_handle,
            WorkerSetup {
                worker_id: 0,
                inbox: local_inbox,
                shards: local_shards.clone(),
                shared: shared(r#"{"route": "ErrorRoute|local"}"#),
                workers: workers.clone(),
                listener: Some(ListenerConfig {
                    listen_addr: "127.0.0.1:0".parse().unwrap(),
                    use_reuseport: false,
                }),
                routing_events: events.sink(),
                events: events.sink(),
            },
            &supervisor,
        )
        .unwrap();
        let (remote, _) = WorkerThreadOwner::spawn(
            remote_handle,
            WorkerSetup {
                worker_id: 1,
                inbox: remote_inbox,
                shards: remote_shards.clone(),
                shared: shared(r#"{"route": "ErrorRoute|remote"}"#),
                workers,
                listener: None,
                routing_events: events.sink(),
                events: events.sink(),
            },
            &supervisor,
        )
        .unwrap();

        let mut client = std::net::TcpStream::connect(address.unwrap()).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        client.write_all(b"mg key v\r\n").unwrap();
        let expected = b"SERVER_ERROR remote\r\n";
        let mut reply = vec![0u8; expected.len()];
        client.read_exact(&mut reply).unwrap();
        assert_eq!(reply, expected);
        assert_eq!(
            local_shards.frontend.requests[RequestKind::Get as usize].load(),
            1
        );
        assert_eq!(
            remote_shards.frontend.requests[RequestKind::Get as usize].load(),
            0
        );

        local.shutdown().unwrap();
        remote.shutdown().unwrap();
    }

    #[test]
    fn failed_collection_joins_previously_started_workers() {
        let (events, _consumer) = channel(8, Arc::new(ControlMetrics::default()));
        let shared = |document: &str| {
            Arc::new(WorkerShared {
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
        let (handles, inboxes): (Vec<_>, Vec<_>) = (0..2).map(WorkerHandle::allocate).unzip();
        let peers = WorkerSet::new(handles.clone());
        let supervisor = Supervisor::new();
        let bound = Cell::new(None);
        let result = handles
            .into_iter()
            .zip(inboxes)
            .zip(configurations)
            .enumerate()
            .map(|(worker_id, ((handle, inbox), shared))| {
                WorkerThreadOwner::spawn(
                    handle,
                    WorkerSetup {
                        worker_id,
                        inbox,
                        shards: WorkerShards::new(),
                        shared,
                        workers: peers.clone(),
                        listener: (worker_id == 0).then_some(ListenerConfig {
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
        TcpListener::bind(bound.get().expect("first worker never started"))
            .expect("failed collection left an earlier worker listening");
    }
}
