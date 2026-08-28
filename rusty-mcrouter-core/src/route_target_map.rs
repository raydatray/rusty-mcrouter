use std::{collections::HashMap, rc::Rc};

use rusty_mcrouter_config::RoutingPrefix;

use crate::{route_pattern, route_policy_map::RoutePolicyMap, DynRoute};

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
    fallback_by_region: HashMap<Box<[u8]>, Rc<RoutePolicyMap>>,
    ordered_routes: Vec<ConfiguredRoute>,
    all_routes: Rc<RoutePolicyMap>,
    send_invalid_to_default: bool,
}

struct ConfiguredRoute {
    prefix: Box<[u8]>,
    route_map: Rc<RoutePolicyMap>,
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
        let default_prefix: Box<[u8]> = options.default_route.as_bytes().into();
        let mut ordered_routes = by_route
            .iter()
            .map(|(prefix, route_map)| ConfiguredRoute {
                prefix: prefix.clone(),
                route_map: Rc::clone(route_map),
            })
            .collect::<Vec<_>>();
        ordered_routes.sort_by(|left, right| left.prefix.cmp(&right.prefix));
        let default_index = ordered_routes
            .iter()
            .position(|route| route.prefix == default_prefix)
            .expect("default route map was checked during route construction");
        let default = ordered_routes.remove(default_index);
        ordered_routes.insert(0, default);

        let fallback_by_region = by_route
            .iter()
            .filter_map(|(prefix, route_map)| {
                let parsed = RoutingPrefixRef::parse(prefix)?;
                (parsed.cluster == b"fallback").then(|| {
                    (
                        parsed.region.to_vec().into_boxed_slice(),
                        Rc::clone(route_map),
                    )
                })
            })
            .collect();

        Self {
            default_route_map,
            by_route,
            by_region,
            fallback_by_region,
            ordered_routes,
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
        } else if !prefix.contains(&b'*') {
            let route_map = self.by_route.get(prefix).or_else(|| {
                RoutingPrefixRef::parse(prefix)
                    .and_then(|parsed| self.fallback_by_region.get(parsed.region))
            });
            route_map
                .map(|route_map| route_map.targets(routing_key))
                .unwrap_or_default()
        } else {
            let parsed = RoutingPrefixRef::parse(prefix)?;
            if parsed.cluster == b"*" && !parsed.region.contains(&b'*') {
                self.by_region
                    .get(parsed.region)
                    .map(|route_map| route_map.targets(routing_key))
                    .unwrap_or_default()
            } else {
                return None;
            }
        };

        Some(self.with_invalid_fallback(targets, routing_key))
    }

    pub(crate) fn get_targets_slow(
        &self,
        pattern: &[u8],
        routing_key: &[u8],
    ) -> Vec<Rc<dyn DynRoute>> {
        let mut targets = Vec::new();

        for configured in &self.ordered_routes {
            if route_pattern::matches(pattern, &configured.prefix) {
                append_unique(&mut targets, configured.route_map.targets(routing_key));
            }
        }

        if targets.is_empty() && self.send_invalid_to_default {
            append_unique(&mut targets, self.default_route_map.targets(routing_key));
        }

        targets
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

fn append_unique(targets: &mut Vec<Rc<dyn DynRoute>>, candidates: &[Rc<dyn DynRoute>]) {
    for candidate in candidates {
        if !targets.iter().any(|target| Rc::ptr_eq(target, candidate)) {
            targets.push(Rc::clone(candidate));
        }
    }
}
