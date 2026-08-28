use std::rc::Rc;

use rusty_mcrouter_backend::Backend;
use rusty_mcrouter_protocol::{Reply, Request};

use crate::routes::selection_route::SelectionRoute;
use crate::routes::{DestinationRoute, DynRoute, Result, Route};
use crate::selectors::Selector;
use crate::RouteContext;

pub struct PoolRoute {
    inner: SelectionRoute,
}

impl PoolRoute {
    pub fn new<B>(destinations: Vec<Rc<DestinationRoute<B>>>, selector: Box<dyn Selector>) -> Self
    where
        B: Backend,
    {
        let children = destinations
            .into_iter()
            .map(|d| d as Rc<dyn DynRoute>)
            .collect();

        Self {
            inner: SelectionRoute::new(children, selector),
        }
    }
}

impl Route for PoolRoute {
    async fn route(&self, context: &RouteContext, request: Request) -> Result<Reply> {
        self.inner.route(context, request).await
    }
}
