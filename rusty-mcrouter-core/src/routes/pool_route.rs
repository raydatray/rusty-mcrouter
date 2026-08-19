use std::rc::Rc;

use rusty_mcrouter_backend::Backend;
use rusty_mcrouter_protocol::{Reply, Request};

use crate::selectors::Selector;
use crate::RouteContext;

use super::{
    destination_route::DestinationRoute, selection_route::SelectionRoute, DynRoute, Result, Route,
};

pub struct PoolRoute {
    pool_name: String,
    inner: SelectionRoute,
}

impl PoolRoute {
    pub fn new<B: Backend>(
        pool_name: impl Into<String>,
        destinations: Vec<Rc<DestinationRoute<B>>>,
        selector: Box<dyn Selector>,
    ) -> Self {
        let children = destinations
            .into_iter()
            .map(|d| d as Rc<dyn DynRoute>)
            .collect();

        Self {
            pool_name: pool_name.into(),
            inner: SelectionRoute::new(children, selector),
        }
    }

    pub fn pool_name(&self) -> &str {
        &self.pool_name
    }
}

impl Route for PoolRoute {
    async fn route(&self, context: &RouteContext<'_>, request: Request) -> Result<Reply> {
        self.inner.route(context, request).await
    }
}
