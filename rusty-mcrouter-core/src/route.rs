use std::{future::Future, pin::Pin, rc::Rc};

use rusty_mcrouter_net::NetError;
use rusty_mcrouter_protocol::{Reply, Request};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RouteError {
    #[error("backend error: {0}")]
    Backend(#[from] NetError),
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

pub type RouteFuture<'a> = Pin<Box<dyn Future<Output = Result<Reply>> + 'a>>;

pub trait DynRoute: 'static {
    fn route_dyn<'a>(&'a self, req: Request) -> RouteFuture<'a>;
}

impl<R: Route> DynRoute for R {
    fn route_dyn<'a>(&'a self, req: Request) -> RouteFuture<'a> {
        Box::pin(<R as Route>::route(self, req))
    }
}
