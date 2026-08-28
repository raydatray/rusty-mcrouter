use std::rc::Rc;

use rusty_mcrouter_protocol::{Reply, Request};

use crate::{
    route_target_map::RouteTargetMap,
    routes::{Result, Route, RouteError},
    DynRoute, RouteContext,
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
        if let Some(targets) = self
            .route_targets
            .get_targets_fast(request.key().routing_prefix(), request.key().routing_key())
        {
            return route_to_all(context, targets, request).await;
        }

        let targets = self.route_targets.get_targets_slow(
            request
                .key()
                .routing_prefix()
                .expect("slow path requires a routing prefix"),
            request.key().routing_key(),
        );

        route_to_all(context, &targets, request).await
    }
}

async fn route_to_all(
    context: &RouteContext,
    targets: &[Rc<dyn DynRoute>],
    request: Request,
) -> Result<Reply> {
    let (primary, secondaries) = targets.split_first().ok_or(RouteError::NoRoute)?;

    for secondary in secondaries {
        context.spawn_background(Rc::clone(secondary), request.clone());
    }

    primary.route_dyn(context, request).await
}
