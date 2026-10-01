use std::{
    rc::{Rc, Weak},
    sync::Arc,
};

use crate::{
    connection::{
        BackendConnectionConfig, Connection, ConnectionHandle, ConnectionResources, ConnectionSetup,
    },
    destination::{
        Destination, DestinationConfig, DestinationKey, DestinationMetrics, DestinationSetup,
    },
    metrics::BackendMetricsShard,
    tko::{DestToken, TkoTracker},
};

pub struct DestinationAssembler {
    shard_metrics: Arc<BackendMetricsShard>,
}

impl DestinationAssembler {
    pub fn new(shard_metrics: Arc<BackendMetricsShard>) -> Self {
        Self { shard_metrics }
    }

    pub fn spawn_destination(
        &self,
        key: DestinationKey,
        options: DestinationConfig,
        tracker: Arc<TkoTracker>,
        metrics: Arc<DestinationMetrics>,
    ) -> Rc<Destination> {
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
                token: DestToken::allocate(),
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
