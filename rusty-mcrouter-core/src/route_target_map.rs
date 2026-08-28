use std::{collections::HashMap, rc::Rc};

use rusty_mcrouter_config::RoutingPrefix;

use crate::{route_policy_map::RoutePolicyMap, DynRoute};

#[derive(Clone, Debug)]
pub struct RootRouteOptions {
    pub default_route: RoutingPrefix,
    pub send_invalid_to_default: bool,
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
    by_region: HashMap<Box<[u8]>, Rc<RoutePolicyMap>>,
    all_routes: Rc<RoutePolicyMap>,
    send_invalid_to_default: bool,
}

struct RoutingPrefixRef<'a> {
    region: &'a [u8],
    cluster: &'a [u8],
}

impl<'a> RoutingPrefixRef<'a> {
    fn parse(prefix: &'a [u8]) -> Option<Self> {
        let body = prefix.strip_prefix(b"/")?.strip_suffix(b"/")?;
        let separator = body.iter().position(|byte| *byte == b'/')?;
        let region = &body[..separator];
        let cluster = &body[separator + 1..];

        (!region.is_empty() && !cluster.is_empty() && !cluster.contains(&b'/'))
            .then_some(Self { region, cluster })
    }
}

impl RouteTargetMap {
    pub(crate) fn new(
        options: &RootRouteOptions,
        default_route_map: Rc<RoutePolicyMap>,
        by_route: HashMap<Box<[u8]>, Rc<RoutePolicyMap>>,
        by_region: HashMap<Box<[u8]>, Rc<RoutePolicyMap>>,
        all_routes: Rc<RoutePolicyMap>,
    ) -> Self {
        Self {
            default_route_map,
            by_route,
            by_region,
            all_routes,
            send_invalid_to_default: options.send_invalid_to_default,
        }
    }

    pub(crate) fn get_targets_fast<'a>(
        &'a self,
        routing_prefix: Option<&[u8]>,
        routing_key: &[u8],
    ) -> Option<&'a [Rc<dyn DynRoute>]> {
        let Some(prefix) = routing_prefix else {
            return Some(self.default_route_map.targets(routing_key));
        };

        let targets = if prefix == b"/*/*/" {
            self.all_routes.targets(routing_key)
        } else {
            let parsed = RoutingPrefixRef::parse(prefix)?;

            if parsed.cluster == b"*" && !parsed.region.contains(&b'*') {
                self.by_region
                    .get(parsed.region)
                    .map(|route_map| route_map.targets(routing_key))
                    .unwrap_or_default()
            } else if !prefix.contains(&b'*') {
                self.by_route
                    .get(prefix)
                    .map(|route_map| route_map.targets(routing_key))
                    .unwrap_or_default()
            } else {
                return None;
            }
        };

        Some(self.with_invalid_fallback(targets, routing_key))
    }

    fn with_invalid_fallback<'a>(
        &'a self,
        targets: &'a [Rc<dyn DynRoute>],
        routing_key: &[u8],
    ) -> &'a [Rc<dyn DynRoute>] {
        if targets.is_empty() && self.send_invalid_to_default {
            self.default_route_map.targets(routing_key)
        } else {
            targets
        }
    }
}
