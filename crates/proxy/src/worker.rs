use std::{net::SocketAddr, rc::Rc, sync::Arc, time::Duration};

use anyhow::Context;
use rusty_mcrouter_backend::destination;
use rusty_mcrouter_frontend::{bind_listener, Server};

use crate::context::ProxyContext;
use crate::generation::{GenerationBuilder, GenerationSetup};
use crate::runtime::{BackgroundTasks, ProxyRuntime};
use crate::{
    ProxyInbox, ProxySet, ProxyThreadSetup, WorkerEvent, WorkerEventRecord, WorkerEventSink,
};

/// Thread-local worker state, constructed inside its owner's Tokio LocalSet.
pub struct ProxyWorker {
    bound_addr: Option<SocketAddr>,
    context: ProxyContext,
    builder: GenerationBuilder,
    inbox: ProxyInbox,
    server: Option<Server>,
    destinations: Rc<destination::Map>,
    sweep_interval: Duration,
    events: WorkerEventSink,
}

impl ProxyWorker {
    pub async fn build(setup: ProxyThreadSetup) -> anyhow::Result<Self> {
        let server = match setup.listener {
            Some(listener) => {
                let addr = listener.listen_addr;
                let listener = bind_listener(listener)
                    .await
                    .with_context(|| format!("bind({addr}) failed"))?;
                Some(Server::new(listener))
            }
            None => None,
        };
        let bound_addr = server.as_ref().map(Server::local_addr).transpose()?;
        let destinations = destination::Map::new(destination::DestinationMapSetup {
            tko_map: Arc::clone(&setup.shared.tko_map),
            assembler: destination::DestinationAssembler::new(
                destination::DestinationAssemblerSetup {
                    tokens: Arc::clone(&setup.shared.tokens),
                    metrics: Arc::clone(&setup.shared.destinations),
                    shard_metrics: setup.shards.backend,
                },
            ),
        });
        let builder = GenerationBuilder::new(GenerationSetup {
            destinations: Rc::clone(&destinations),
            defaults: setup.shared.defaults.clone(),
            root_options: setup.shared.root_route_options.clone(),
            metrics: setup.shards.routing,
            events: setup.routing_events,
        });
        let routes = builder
            .build(1, &setup.shared.config)
            .context("build_route failed")?;
        let context = ProxyContext {
            proxy_id: setup.proxy_id,
            routes,
            proxies: setup.proxies,
            thread_mode: setup.shared.thread_mode,
            metrics: setup.shards.frontend,
            connection_options: setup.shared.connection_options,
        };
        Ok(Self {
            bound_addr,
            context,
            builder,
            inbox: setup.inbox,
            server,
            destinations,
            sweep_interval: setup.shared.sweep_interval,
            events: setup.events,
        })
    }

    pub fn bound_addr(&self) -> Option<SocketAddr> {
        self.bound_addr
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let proxy_id = self.context.proxy_id;
        self.events.emit(WorkerEventRecord {
            proxy_id,
            event: WorkerEvent::Started,
        });
        let tasks = BackgroundTasks {
            listener: self.server.map(|server| {
                let proxies = self.context.proxies.clone();
                tokio::task::spawn_local(accept_and_dispatch(server, proxies))
            }),
            sweep: self.destinations.spawn_idle_sweep(self.sweep_interval),
        };
        let runtime = ProxyRuntime::new(self.context, self.builder, self.inbox, tasks);
        let outcome = runtime.run().await;
        self.events.emit(WorkerEventRecord {
            proxy_id,
            event: WorkerEvent::Stopped,
        });
        outcome
    }
}

/// Distribute accepted sockets to workers. Listener transport belongs to the
/// frontend; placement and mailbox delivery belong to the proxy fleet.
async fn accept_and_dispatch(server: Server, proxies: ProxySet) -> anyhow::Result<()> {
    let mut next = 0usize;
    loop {
        let stream = server.accept().await?;
        proxies.nth(next).send_connection(stream).await?;
        next = next.wrapping_add(1);
    }
}
