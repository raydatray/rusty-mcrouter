use std::{
    rc::{Rc, Weak},
    sync::Arc,
};

use crate::{
    connection::{
        BackendConnectionConfig, Connection, ConnectionHandle, ConnectionResources, ConnectionSetup,
    },
    destination::{
        Destination, DestinationConfig, DestinationKey, DestinationMetricsRegistry,
        DestinationSetup,
    },
    metrics::BackendMetricsShard,
    tko::{DestTokenAllocator, TkoTracker},
};

pub struct DestinationAssemblerSetup {
    pub tokens: Arc<DestTokenAllocator>,
    pub metrics: Arc<DestinationMetricsRegistry>,
    pub shard_metrics: Arc<BackendMetricsShard>,
}

pub struct DestinationAssembler {
    tokens: Arc<DestTokenAllocator>,
    metrics: Arc<DestinationMetricsRegistry>,
    shard_metrics: Arc<BackendMetricsShard>,
}

impl DestinationAssembler {
    pub fn new(setup: DestinationAssemblerSetup) -> Self {
        Self {
            tokens: setup.tokens,
            metrics: setup.metrics,
            shard_metrics: setup.shard_metrics,
        }
    }

    pub fn spawn_destination(
        &self,
        key: DestinationKey,
        options: DestinationConfig,
        tracker: Arc<TkoTracker>,
    ) -> Rc<Destination> {
        let metrics = self.metrics.metrics_for(&tracker);
        let token = self.tokens.allocate();
        Rc::new_cyclic(|weak: &Weak<Destination>| {
            let weak = weak.clone();
            let events = Box::new(move |event| {
                if let Some(destination) = weak.upgrade() {
                    destination.on_conn_event(event);
                }
            });
            let connection_options = BackendConnectionConfig {
                connect_timeout: Some(options.connect_timeout),
                connect_timeout_retries: options.connect_timeout_retries,
                write_timeout: Some(options.reply_timeout),
                reply_timeout: Some(options.reply_timeout),
                ..BackendConnectionConfig::default()
            };
            let (handle, inbox) = ConnectionHandle::allocate(&connection_options);
            let connection = Connection::new(ConnectionSetup {
                addr: Arc::clone(&key.addr),
                options: connection_options,
                inbox,
                events,
                metrics: Arc::clone(&self.shard_metrics),
            });
            Destination::new(DestinationSetup {
                key,
                options,
                token,
                probe_seed: token.probe_seed(),
                tracker,
                connection: ConnectionResources {
                    handle,
                    task: tokio::task::spawn_local(connection.run()),
                },
                metrics,
                shard_metrics: Arc::clone(&self.shard_metrics),
            })
        })
    }
}
