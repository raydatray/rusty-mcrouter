use std::{future::Future, pin::Pin, sync::Arc};

use rusty_mcrouter_net::NetError;
use rusty_mcrouter_protocol::{reply::Reply, request::Request};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RouteError {
    #[error("backend error: {0}")]
    Backend(#[from] NetError),
}

pub trait Route: Send + Sync + 'static {
    fn route(&self, req: Request) -> impl Future<Output = Result<Reply, RouteError>> + Send;

    fn into_dyn(self) -> Arc<dyn DynRoute>
    where
        Self: Sized,
    {
        Arc::new(self)
    }

    fn arc_into_dyn(self: Arc<Self>) -> Arc<dyn DynRoute>
    where
        Self: Sized,
    {
        self
    }
}

pub type RouteFuture<'a> = Pin<Box<dyn Future<Output = Result<Reply, RouteError>> + Send + 'a>>;

pub trait DynRoute: Send + Sync + 'static {
    fn route_dyn<'a>(&'a self, req: Request) -> RouteFuture<'a>;
}

impl<R: Route> DynRoute for R {
    fn route_dyn<'a>(&'a self, req: Request) -> RouteFuture<'a> {
        Box::pin(<R as Route>::route(self, req))
    }
}
