use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;

use rusty_mcrouter_protocol::{Reply, Request};
use thiserror::Error;

use crate::classify::ResultCode;
use crate::destination::{self, Destination, DestinationKey, Map};
use crate::error::SendError;
use crate::tko::FailOpenThresholds;

pub trait Backend: 'static {
    fn prepare_send(
        &self,
        request: Request,
    ) -> Result<PreparedSend<impl Future<Output = Result<Reply, SendError>> + '_>, TkoRejection>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TkoRejection {
    pub reason: ResultCode,
}

impl From<TkoRejection> for SendError {
    fn from(rejection: TkoRejection) -> Self {
        Self::Tko {
            reason: rejection.reason,
        }
    }
}

#[must_use = "a prepared send must be sent. it should never be discarded"]
pub struct PreparedSend<F> {
    future: F,
}

impl<F> PreparedSend<F>
where
    F: Future<Output = Result<Reply, SendError>>,
{
    pub fn new(future: F) -> Self {
        Self { future }
    }

    pub async fn send(self) -> Result<Reply, SendError> {
        self.future.await
    }
}

/// Pool identity the factory needs to resolve the fail-open gate.
pub struct PoolHealth<'a> {
    pub pool_name: &'a str,
    /// Resolved fail-open thresholds, if the pool configured a tko_tracker
    /// block. None = no gate (the default). Threshold resolution (num vs
    /// percent) is the route builder's job.
    pub fail_open: Option<FailOpenThresholds>,
}

impl PoolHealth<'_> {
    /// The common case: a pool with no fail-open gate.
    pub fn ungated(pool_name: &str) -> PoolHealth<'_> {
        PoolHealth {
            pool_name,
            fail_open: None,
        }
    }
}

#[derive(Debug, Error)]
pub enum BackendFactoryError {
    #[error("invalid server address {addr:?}")]
    InvalidAddress { addr: String },
}

/// Constructs a [`Backend`] for a server address — the seam that lets the
/// route builder run over mock backends without opening sockets.
///
/// SYNC and I/O-free: connections are lazy, so building a route graph over a
/// dead server succeeds — the server just starts life failing (and TKOs).
/// Address shape is the only thing that can fail at build time.
pub trait BackendFactory {
    type Backend: Backend;

    fn make(
        &self,
        server: &str,
        cfg: &destination::DestinationConfig,
        pool: &PoolHealth<'_>,
    ) -> Result<Self::Backend, BackendFactoryError>;
}

/// Production factory: one per proxy thread, deduping through the thread's
/// destination map (and, transitively, sharing TKO trackers across threads).
pub struct DestinationFactory {
    map: Rc<Map>,
}

impl DestinationFactory {
    pub fn new(map: Rc<Map>) -> DestinationFactory {
        DestinationFactory { map }
    }
}

impl BackendFactory for DestinationFactory {
    type Backend = Rc<Destination>;

    fn make(
        &self,
        server: &str,
        cfg: &destination::DestinationConfig,
        pool: &PoolHealth<'_>,
    ) -> Result<Rc<Destination>, BackendFactoryError> {
        let valid = server
            .rsplit_once(':')
            .is_some_and(|(host, port)| !host.is_empty() && port.parse::<u16>().is_ok());
        if !valid {
            return Err(BackendFactoryError::InvalidAddress {
                addr: server.into(),
            });
        }

        let gate = pool.fail_open.map(|thresholds| {
            self.map
                .tko_map()
                .pool_tracker_for(pool.pool_name, thresholds)
        });
        let key = DestinationKey {
            addr: Arc::from(server),
            reply_timeout: cfg.reply_timeout,
        };
        Ok(self.map.destination(key, cfg, gate))
    }
}

#[cfg(test)]
mod tests {
    use rusty_mcrouter_protocol::reply::GetReply;
    use rusty_mcrouter_protocol::test_support::get;

    use super::*;
    use crate::classify::ResultCode;
    use crate::destination::DestinationMetricsRegistry;
    use crate::metrics::BackendMetricsShard;
    use crate::test_support::{run_local, scripted_backend_serial, Step};
    use crate::tko::{DestToken, TkoTrackerMap};

    fn factory() -> (Arc<TkoTrackerMap>, DestinationFactory) {
        let tko = TkoTrackerMap::with_sink(crate::tko::TkoEventSink::new(|_| {}));
        let factory = DestinationFactory::new(Map::new(
            Arc::clone(&tko),
            BackendMetricsShard::new(),
            DestinationMetricsRegistry::new(),
        ));
        (tko, factory)
    }

    /// Route-shaped consumption: a generic fn constrained on Backend driving
    /// the production impl end to end.
    async fn through_trait<B: Backend>(backend: &B, req: Request) -> Result<Reply, SendError> {
        match backend.prepare_send(req) {
            Ok(prepared) => prepared.send().await,
            Err(rejection) => Err(rejection.into()),
        }
    }

    #[tokio::test]
    async fn factory_makes_lazy_destinations_and_dedups() {
        run_local(async {
            let (_tko, factory) = factory();
            let cfg = destination::DestinationConfig::default();
            let pool = PoolHealth::ungated("pool");

            // no listener at this addr: make still succeeds (lazy)
            let a = factory.make("127.0.0.1:9", &cfg, &pool).unwrap();
            let b = factory.make("127.0.0.1:9", &cfg, &pool).unwrap();
            assert!(Rc::ptr_eq(&a, &b), "factory must dedup through the map");
        })
        .await;
    }

    #[tokio::test]
    async fn invalid_addresses_fail_at_build_time() {
        run_local(async {
            let (_tko, factory) = factory();
            let cfg = destination::DestinationConfig::default();
            let pool = PoolHealth::ungated("pool");

            for addr in ["localhost", "host:notaport", "host:99999", ":11211", ""] {
                let result = factory.make(addr, &cfg, &pool);
                assert!(
                    matches!(result, Err(BackendFactoryError::InvalidAddress { .. })),
                    "{addr:?} must be rejected"
                );
            }
        })
        .await;
    }

    /// Gated pool: the factory resolves the gate through the shared registry
    /// and attaches it. Proven via capacity: enter=1, first server's mark
    /// consumes the slot, second server's mark is refused.
    #[tokio::test]
    async fn gated_pool_attaches_fail_open_gate() {
        run_local(async {
            let (_tko, factory) = factory();
            let cfg = destination::DestinationConfig::default();
            let pool = PoolHealth {
                pool_name: "gated",
                fail_open: Some(FailOpenThresholds { enter: 1, exit: 1 }),
            };

            let a = factory.make("127.0.0.1:9", &cfg, &pool).unwrap();
            let b = factory.make("127.0.0.1:10", &cfg, &pool).unwrap();

            assert!(a
                .tracker()
                .record_hard_failure(DestToken::allocate(), ResultCode::ConnectError));
            assert!(
                !b.tracker()
                    .record_hard_failure(DestToken::allocate(), ResultCode::ConnectError),
                "both servers must share the pool's one-slot gate"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn rc_destination_implements_backend() {
        run_local(async {
            let server =
                scripted_backend_serial(vec![vec![Step::ReadRequests(1), Step::Write(b"EN\r\n")]])
                    .await;
            let (_tko, factory) = factory();
            let backend = factory
                .make(
                    &server.addr.to_string(),
                    &destination::DestinationConfig::default(),
                    &PoolHealth::ungated("pool"),
                )
                .unwrap();

            let reply = through_trait(&backend, get(b"key")).await.unwrap();
            assert_eq!(reply, Reply::Get(GetReply::Miss));
        })
        .await;
    }
}
