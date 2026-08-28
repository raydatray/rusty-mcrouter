mod destination_route;
mod error_route;
mod failover_route;
mod null_route;
mod pool_route;
mod root_route;
mod selection_route;

pub use destination_route::DestinationRoute;
pub use error_route::ErrorRoute;
pub use failover_route::FailoverRoute;
pub use null_route::NullRoute;
pub use pool_route::PoolRoute;
pub use root_route::RootRoute;

use std::{future::Future, pin::Pin, rc::Rc};

use rusty_mcrouter_backend::error::SendError;
use rusty_mcrouter_protocol::{Reply, Request};
use thiserror::Error;

use crate::RouteContext;

#[derive(Debug, Error)]
pub enum RouteError {
    #[error("backend error: {0}")]
    Backend(#[from] SendError),

    #[error("selector returned index {idx} but pool has {len} children")]
    SelectorOutOfRange { idx: usize, len: usize },

    #[error("request did not match a configured route")]
    NoRoute,
}

pub type Result<T> = std::result::Result<T, RouteError>;

pub trait Route: 'static {
    fn route(
        &self,
        context: &RouteContext,
        request: Request,
    ) -> impl Future<Output = Result<Reply>>;

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

pub type RouteFuture<'a> = Pin<Box<dyn Future<Output = Result<Reply>> + 'a>>;

pub trait DynRoute: 'static {
    fn route_dyn<'a>(&'a self, context: &'a RouteContext, request: Request) -> RouteFuture<'a>;
}

impl<R> DynRoute for R
where
    R: Route,
{
    fn route_dyn<'a>(&'a self, context: &'a RouteContext, request: Request) -> RouteFuture<'a> {
        Box::pin(<R as Route>::route(self, context, request))
    }
}
