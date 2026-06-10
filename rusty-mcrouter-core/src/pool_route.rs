use std::rc::Rc;

use rusty_mcrouter_protocol::{Reply, Request};

use crate::{
    destination_route::DestinationRoute,
    route::{Result, Route},
    selection_route::SelectionRoute,
    selectors::Selector,
    DynRoute,
};

pub struct PoolRoute {
    pool_name: String,
    inner: SelectionRoute,
}

impl PoolRoute {
    pub fn new(
        pool_name: impl Into<String>,
        destinations: Vec<Rc<DestinationRoute>>,
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
    async fn route(&self, req: Request) -> Result<Reply> {
        self.inner.route(req).await
    }
}
