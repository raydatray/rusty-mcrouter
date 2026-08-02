mod destination_route;
mod error_route;
mod failover_route;
mod null_route;
mod pool_route;
mod selection_route;

pub use destination_route::DestinationRoute;
pub use error_route::ErrorRoute;
pub use failover_route::FailoverRoute;
pub use null_route::NullRoute;
pub use pool_route::PoolRoute;

use std::{future::Future, pin::Pin, rc::Rc};

use rusty_mcrouter_net::NetError;
use rusty_mcrouter_protocol::{Reply, Request};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RouteError {
    #[error("backend error: {0}")]
    Backend(#[from] NetError),

    #[error("selector returned index {idx} but pool has {len} children")]
    SelectorOutOfRange { idx: usize, len: usize },
}

pub type Result<T> = std::result::Result<T, RouteError>;

pub trait Route: 'static {
    fn route(&self, req: Request) -> impl Future<Output = Result<Reply>>;

    fn into_dyn(self) -> Rc<dyn DynRoute>
    where
        Self: Sized,
    {
        Rc::new(self)
    }

    fn rc_into_dyn(self: Rc<Self>) -> Rc<dyn DynRoute>
    where
        Self: Sized,
    {
        self
    }
}

/// `'static` so callers (the connection's in-flight set, cross-thread proxy
/// tasks) can store the future without borrowing the route graph: the future
/// owns an `Rc` to its route instead.
pub type RouteFuture = Pin<Box<dyn Future<Output = Result<Reply>>>>;

pub trait DynRoute: 'static {
    fn route_dyn(self: Rc<Self>, req: Request) -> RouteFuture;
}

impl<R: Route> DynRoute for R {
    fn route_dyn(self: Rc<Self>, req: Request) -> RouteFuture {
        Box::pin(async move { <R as Route>::route(&self, req).await })
    }
}
