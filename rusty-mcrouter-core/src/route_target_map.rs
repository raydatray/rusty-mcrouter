use std::rc::Rc;

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
    default_prefix: Box<[u8]>,
    default_route_map: Rc<RoutePolicyMap>,
    send_invalid_to_default: bool,
}

impl RouteTargetMap {
    pub(crate) fn new(options: &RootRouteOptions, default_route_map: Rc<RoutePolicyMap>) -> Self {
        Self {
            default_prefix: options.default_route.as_bytes().into(),
            default_route_map,
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
            Some(prefix) if prefix == self.default_prefix.as_ref() => {
                self.default_route_map.targets(routing_key)
            }
            Some(_) if self.send_invalid_to_default => self.default_route_map.targets(routing_key),
            Some(_) => &[],
        }
    }
}
