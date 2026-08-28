use rusty_mcrouter_protocol::{Reply, Request};

use crate::{
    route_target_map::RouteTargetMap,
    routes::{Result, Route, RouteError},
    RouteContext,
};

pub struct RootRoute {
    route_targets: RouteTargetMap,
}

impl RootRoute {
    pub(crate) fn new(route_targets: RouteTargetMap) -> Self {
        Self { route_targets }
    }
}

impl Route for RootRoute {
    async fn route(&self, context: &RouteContext, request: Request) -> Result<Reply> {
        let target = self
            .route_targets
            .resolve(request.key().routing_prefix(), request.key().routing_key())
            .first()
            .ok_or(RouteError::NoRoute)?;

        target.route_dyn(context, request).await
    }
}
