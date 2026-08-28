use std::{collections::HashMap, rc::Rc};

use rusty_mcrouter_config::RoutingPrefix;

use crate::{route_policy_map::RoutePolicyMap, DynRoute};

#[derive(Clone, Debug)]
pub(crate) struct RootRouteOptions {
    pub(crate) default_route: RoutingPrefix,
    pub(crate) send_invalid_to_default: bool,
}

impl Default for RootRouteOptions {
    fn default() -> Self {
        Self {
            default_route: "/././"
                .parse()
                .expect("static default routing prefix is valid"),
            send_invalid_to_default: false,
        }
    }
}

pub(crate) struct RouteTargetMap {
    default_route_map: Rc<RoutePolicyMap>,
    by_route: HashMap<Box<[u8]>, Rc<RoutePolicyMap>>,
    send_invalid_to_default: bool,
}

impl RouteTargetMap {
    pub(crate) fn new(
        options: &RootRouteOptions,
        default_route_map: Rc<RoutePolicyMap>,
        by_route: HashMap<Box<[u8]>, Rc<RoutePolicyMap>>,
    ) -> Self {
        Self {
            default_route_map,
            by_route,
            send_invalid_to_default: options.send_invalid_to_default,
        }
    }

    pub(crate) fn resolve(
        &self,
        routing_prefix: Option<&[u8]>,
        routing_key: &[u8],
    ) -> &[Rc<dyn DynRoute>] {
        match routing_prefix {
            None => self.default_route_map.targets(routing_key),
            Some(prefix) => self
                .by_route
                .get(prefix)
                .map(|route_map| route_map.targets(routing_key))
                .filter(|targets| !targets.is_empty())
                .or_else(|| {
                    self.send_invalid_to_default
                        .then(|| self.default_route_map.targets(routing_key))
                })
                .unwrap_or_default(),
        }
    }
}
