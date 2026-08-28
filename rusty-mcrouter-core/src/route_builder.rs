use std::{
    collections::{BTreeMap, HashMap},
    rc::Rc,
    time::Duration,
};

use rusty_mcrouter_backend::tko::FailOpenThresholds;
use rusty_mcrouter_backend::{destination, Backend, BackendFactory, PoolFailOpen, PoolHealth};
use rusty_mcrouter_config::{
    ConfigDocument, FailoverErrorsConfig, FailoverPolicyConfig, HashConfig, HashFunc, PoolConfig,
    PoolId, PrefixSelectorConfig, RootRouteConfig, RouteConfig, RouteDefinition,
    RouteSelectorConfig, RoutingPrefix,
};
use thiserror::Error;

use crate::{
    failover::{code_of_kind, FailoverErrors, FailoverPolicy, InOrderPolicy, LeastFailuresPolicy},
    prefix_selector::{PrefixPolicy, PrefixSelector},
    route_policy_map::RoutePolicyMap,
    route_target_map::{RootRouteOptions, RouteTargetMap},
    routes::{
        DestinationRoute, DynRoute, ErrorRoute, FailoverRoute, NullRoute, PoolRoute, RootRoute,
        Route,
    },
    selectors::{Ch3, Crc32, Salted, Selector},
    RoutingMetricsLayout,
};

#[derive(Debug, Error)]
pub enum BuildError {
    #[error("pool `{name}` is missing from the routing metrics layout")]
    PoolMissingFromMetricsLayout { name: String },

    #[error("invalid default route: {prefix}")]
    DefaultRouteMissing { prefix: String },
}

type Result<T> = std::result::Result<T, BuildError>;

/// Builds the route graph from `config`, constructing backends via `factory`
/// (production: `&DestinationFactory`; tests: `&MockBackendFactory`).
///
/// SYNC and I/O-free: backends are lazy, so building over a dead server
/// succeeds — it just starts life failing (and TKOs). `defaults` carries the
/// router-level destination config; pools override it via `server_timeout` /
/// `connect_timeout`.
pub fn build_route<F>(
    config: &ConfigDocument,
    factory: &F,
    defaults: &destination::DestinationConfig,
    metrics_layout: &RoutingMetricsLayout,
) -> Result<Rc<dyn DynRoute>>
where
    F: BackendFactory,
{
    build_route_with_options(
        config,
        factory,
        defaults,
        metrics_layout,
        &RootRouteOptions::default(),
    )
}

pub fn build_route_with_options<F>(
    config: &ConfigDocument,
    factory: &F,
    defaults: &destination::DestinationConfig,
    metrics_layout: &RoutingMetricsLayout,
    root_options: &RootRouteOptions,
) -> Result<Rc<dyn DynRoute>>
where
    F: BackendFactory,
{
    let mut route_builder = RouteBuilder::new(config, factory, defaults, metrics_layout);
    route_builder.build_root(config.root(), root_options)
}

struct RouteBuilder<'a, F>
where
    F: BackendFactory,
{
    config: &'a ConfigDocument,
    factory: &'a F,
    defaults: &'a destination::DestinationConfig,
    metrics_layout: &'a RoutingMetricsLayout,
    pool_cache: BTreeMap<PoolId, Vec<Rc<DestinationRoute<F::Backend>>>>,
}

impl<'a, F> RouteBuilder<'a, F>
where
    F: BackendFactory,
{
    fn new(
        config: &'a ConfigDocument,
        factory: &'a F,
        defaults: &'a destination::DestinationConfig,
        metrics_layout: &'a RoutingMetricsLayout,
    ) -> Self {
        Self {
            config,
            factory,
            defaults,
            metrics_layout,
            pool_cache: BTreeMap::new(),
        }
    }

    fn build_root(
        &mut self,
        root: &RootRouteConfig,
        options: &RootRouteOptions,
    ) -> Result<Rc<dyn DynRoute>> {
        let selectors: BTreeMap<RoutingPrefix, Rc<PrefixSelector>> = match root {
            RootRouteConfig::Single(config) => {
                let selector = self.build_prefix_selector(config, &mut HashMap::new())?;
                BTreeMap::from([(options.default_route.clone(), selector)])
            }
            RootRouteConfig::Routes(configs) => self.build_route_selectors(configs)?,
        };
        let default_selector = selectors
            .get(&options.default_route)
            .cloned()
            .ok_or_else(|| BuildError::DefaultRouteMissing {
                prefix: options.default_route.to_string(),
            })?;
        let mut all_selectors = vec![Rc::clone(&default_selector)];
        let mut region_selectors = BTreeMap::<Vec<u8>, Vec<Rc<PrefixSelector>>>::new();
        region_selectors
            .entry(options.default_route.region().to_vec())
            .or_default()
            .push(Rc::clone(&default_selector));

        for (prefix, selector) in &selectors {
            if prefix == &options.default_route {
                continue;
            }
            all_selectors.push(Rc::clone(selector));
            region_selectors
                .entry(prefix.region().to_vec())
                .or_default()
                .push(Rc::clone(selector));
        }

        let all_routes = Rc::new(RoutePolicyMap::new(&all_selectors));
        let by_region = region_selectors
            .into_iter()
            .map(|(region, selectors)| {
                (
                    region.into_boxed_slice(),
                    Rc::new(RoutePolicyMap::new(&selectors)),
                )
            })
            .collect::<HashMap<_, _>>();
        let by_route = selectors
            .into_iter()
            .map(|(prefix, selector)| {
                (
                    prefix.as_bytes().into(),
                    Rc::new(RoutePolicyMap::new(&[selector])),
                )
            })
            .collect::<HashMap<_, _>>();
        let route_map = by_route
            .get(options.default_route.as_bytes())
            .cloned()
            .expect("default selector was checked above");
        let targets = RouteTargetMap::new(options, route_map, by_route, by_region, all_routes);

        Ok(RootRoute::new(targets).into_dyn())
    }

    fn build_route_selectors(
        &mut self,
        configs: &[RouteSelectorConfig],
    ) -> Result<BTreeMap<RoutingPrefix, Rc<PrefixSelector>>> {
        let (mut selectors, mut route_cache) = (BTreeMap::new(), HashMap::new());

        for config in configs {
            let selector = self.build_prefix_selector(config.selector(), &mut route_cache)?;
            for alias in config.aliases() {
                selectors.insert(alias.clone(), Rc::clone(&selector));
            }
        }

        Ok(selectors)
    }

    fn build_prefix_selector(
        &mut self,
        config: &PrefixSelectorConfig,
        route_cache: &mut HashMap<String, Rc<dyn DynRoute>>,
    ) -> Result<Rc<PrefixSelector>> {
        let mut policies = Vec::with_capacity(config.policies().len());
        for (prefix, child) in config.policies() {
            policies.push(PrefixPolicy::new(
                prefix.as_bytes().to_vec(),
                self.build_definition(child, route_cache)?,
            ));
        }

        let wildcard = config
            .wildcard()
            .map(|child| self.build_definition(child, route_cache))
            .transpose()?;

        Ok(Rc::new(PrefixSelector::new(policies, wildcard)))
    }

    fn build_definition(
        &mut self,
        definition: &RouteDefinition,
        route_cache: &mut HashMap<String, Rc<dyn DynRoute>>,
    ) -> Result<Rc<dyn DynRoute>> {
        if let Some(cached) = definition.cache_key().and_then(|key| route_cache.get(key)) {
            return Ok(Rc::clone(cached));
        }

        let route = self.build_handle(definition.route())?;
        if let Some(key) = definition.cache_key() {
            route_cache.insert(key.to_string(), Rc::clone(&route));
        }
        Ok(route)
    }

    fn build_handle(&mut self, handle: &RouteConfig) -> Result<Rc<dyn DynRoute>> {
        match handle {
            RouteConfig::NullRoute => Ok(NullRoute.into_dyn()),

            RouteConfig::ErrorRoute { message } => Ok(ErrorRoute::new(message.clone()).into_dyn()),

            RouteConfig::PoolRoute { pool, hash } => {
                let destinations = self.get_or_build_destinations(*pool)?;
                Ok(build_pool_handle(hash, destinations))
            }

            RouteConfig::FailoverRoute {
                children,
                failover_errors,
                failover_policy,
            } => {
                let mut built = Vec::with_capacity(children.len());
                for child in children {
                    built.push(self.build_handle(child)?);
                }
                let errors = build_failover_errors(failover_errors);
                let (policy, max_error_tries) = build_failover_policy(failover_policy, built.len());
                Ok(FailoverRoute::new(built, errors, policy, max_error_tries).into_dyn())
            }
        }
    }

    fn get_or_build_destinations(
        &mut self,
        pool_id: PoolId,
    ) -> Result<Vec<Rc<DestinationRoute<F::Backend>>>> {
        if let Some(cached) = self.pool_cache.get(&pool_id) {
            return Ok(cached.clone());
        }

        let pool_config = self.config.pool(pool_id);
        let pool_name = pool_config.name();

        if self.metrics_layout.pool_name(pool_id) != Some(pool_name) {
            return Err(BuildError::PoolMissingFromMetricsLayout {
                name: pool_name.to_string(),
            });
        }

        let dest_cfg = pool_destination_config(self.defaults, pool_config);
        let pool_health = PoolHealth {
            fail_open: pool_config.tko_tracker().map(|config| PoolFailOpen {
                id: pool_id,
                name: pool_name,
                thresholds: FailOpenThresholds {
                    enter: config.enter(),
                    exit: config.exit(),
                },
            }),
        };

        let mut destinations = Vec::with_capacity(pool_config.servers().len());

        for server in pool_config.servers() {
            let backend = self.factory.make(server, &dest_cfg, &pool_health);
            destinations.push(Rc::new(DestinationRoute::<F::Backend>::for_pool(
                backend, pool_id,
            )));
        }

        self.pool_cache.insert(pool_id, destinations.clone());

        Ok(destinations)
    }
}

/// Pool-level overrides on the router defaults.
///
/// THE D2 GUARDRAIL (verified mcrouter behavior, McRouteHandleProvider-inl.h
/// :197-205): a pool `server_timeout` override drags `connect_timeout` down
/// with it unless `connect_timeout` is explicitly set. Without this,
/// connect_timeout > reply_timeout lets callers overshoot their deadline
/// while a connect is pending.
fn pool_destination_config(
    defaults: &destination::DestinationConfig,
    pool: &PoolConfig,
) -> destination::DestinationConfig {
    let mut cfg = defaults.clone();
    if let Some(ms) = pool.server_timeout_ms() {
        cfg.reply_timeout = Some(Duration::from_millis(ms));
        cfg.connect_timeout = Some(Duration::from_millis(ms));
    }
    if let Some(ms) = pool.connect_timeout_ms() {
        cfg.connect_timeout = Some(Duration::from_millis(ms));
    }
    cfg
}

fn build_pool_handle<B>(
    hash: &HashConfig,
    destinations: Vec<Rc<DestinationRoute<B>>>,
) -> Rc<dyn DynRoute>
where
    B: Backend,
{
    let selector = build_selector(hash, destinations.len());
    let route = PoolRoute::new(destinations, selector);

    route.into_dyn()
}

fn build_selector(hash: &HashConfig, n: usize) -> Box<dyn Selector> {
    let base: Box<dyn Selector> = match hash.func {
        HashFunc::Ch3 => Box::new(Ch3::new(n)),
        HashFunc::Crc32 => Box::new(Crc32::new(n)),
    };

    match &hash.salt {
        Some(salt) => Box::new(Salted::new(base, salt.clone().into_bytes())),
        None => base,
    }
}

fn build_failover_errors(cfg: &FailoverErrorsConfig) -> FailoverErrors {
    let codes = |kinds: &Vec<rusty_mcrouter_config::FailoverErrorKind>| {
        kinds.iter().copied().map(code_of_kind).collect::<Vec<_>>()
    };
    match cfg {
        FailoverErrorsConfig::Default => FailoverErrors::default(),
        FailoverErrorsConfig::All(kinds) => {
            let mapped = codes(kinds);
            FailoverErrors::new(Some(mapped.clone()), Some(mapped.clone()), Some(mapped))
        }
        FailoverErrorsConfig::PerOp {
            gets,
            updates,
            deletes,
        } => FailoverErrors::new(
            gets.as_ref().map(codes),
            updates.as_ref().map(codes),
            deletes.as_ref().map(codes),
        ),
    }
}

/// Returns the policy plus its non-TKO error budget. Neither currently
/// configured policy limits errors separately: least-failures max_tries caps
/// its candidate sequence inside the policy.
fn build_failover_policy(cfg: &FailoverPolicyConfig, n: usize) -> (Box<dyn FailoverPolicy>, usize) {
    match cfg {
        FailoverPolicyConfig::InOrder => (Box::new(InOrderPolicy), usize::MAX),
        FailoverPolicyConfig::LeastFailures { max_tries } => (
            Box::new(LeastFailuresPolicy::new(n, *max_tries)),
            usize::MAX,
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use rusty_mcrouter_backend::test_support::MockBackendFactory;
    use rusty_mcrouter_config::parse;
    use rusty_mcrouter_observability_primitives::test_support::noop_sink;
    use rusty_mcrouter_protocol::test_support::{get, get_miss, server_error};
    use rusty_mcrouter_protocol::{Reply, Request};

    use crate::{RoutingMetricsShard, RoutingState};

    fn defaults() -> destination::DestinationConfig {
        destination::DestinationConfig::default()
    }

    struct BuiltRoute {
        route: Rc<dyn DynRoute>,
        #[allow(dead_code)]
        metrics: Arc<RoutingMetricsShard>,
        state: Rc<RoutingState>,
    }

    fn build<F>(cfg: &ConfigDocument, factory: &F) -> Result<BuiltRoute>
    where
        F: BackendFactory,
    {
        let layout = RoutingMetricsLayout::new(cfg);
        let route = build_route(cfg, factory, &defaults(), &layout)?;
        let metrics = RoutingMetricsShard::new(layout);
        let state = RoutingState::new(Arc::clone(&metrics), noop_sink());
        Ok(BuiltRoute {
            route,
            metrics,
            state,
        })
    }

    async fn execute(fixture: &BuiltRoute, request: Request) -> crate::routes::Result<Reply> {
        let context = fixture.state.context();
        let result = fixture.route.route_dyn(&context, request).await;
        context.finish(&result);
        result
    }

    #[tokio::test]
    async fn builds_null_route_from_bare_string() {
        let cfg = parse(r#"{"route": "NullRoute"}"#).unwrap();
        let route = build(&cfg, &MockBackendFactory::new()).unwrap();
        let reply = execute(&route, get(b"foo")).await.unwrap();
        assert_eq!(reply, get_miss());
    }

    #[tokio::test]
    async fn builds_null_route_from_object_form() {
        let cfg = parse(r#"{"route": {"type": "NullRoute"}}"#).unwrap();
        let route = build(&cfg, &MockBackendFactory::new()).unwrap();
        let reply = execute(&route, get(b"foo")).await.unwrap();
        assert_eq!(reply, get_miss());
    }

    #[tokio::test]
    async fn builds_error_route_from_object_with_message() {
        let cfg = parse(r#"{"route": {"type": "ErrorRoute", "message": "boom"}}"#).unwrap();
        let route = build(&cfg, &MockBackendFactory::new()).unwrap();
        let reply = execute(&route, get(b"foo")).await.unwrap();
        assert_eq!(reply, server_error(b"boom"));
    }

    #[tokio::test]
    async fn builds_error_route_from_shorthand_with_message_arg() {
        let cfg = parse(r#"{"route": "ErrorRoute|nope"}"#).unwrap();
        let route = build(&cfg, &MockBackendFactory::new()).unwrap();
        let reply = execute(&route, get(b"foo")).await.unwrap();
        assert_eq!(reply, server_error(b"nope"));
    }

    #[tokio::test]
    async fn prefix_selector_uses_longest_policy_then_wildcard() {
        let cfg = parse(
            r#"{
                "route": {
                    "type": "PrefixSelectorRoute",
                    "policies": {
                        "key:": "ErrorRoute|short",
                        "key:specific:": "ErrorRoute|long"
                    },
                    "wildcard": "NullRoute"
                }
            }"#,
        )
        .unwrap();
        let route = build(&cfg, &MockBackendFactory::new()).unwrap();

        assert_eq!(
            execute(&route, get(b"key:specific:1")).await.unwrap(),
            server_error(b"long")
        );
        assert_eq!(
            execute(&route, get(b"key:other")).await.unwrap(),
            server_error(b"short")
        );
        assert_eq!(execute(&route, get(b"other")).await.unwrap(), get_miss());
    }

    #[tokio::test]
    async fn prefix_selector_uses_routing_key_for_default_prefix() {
        let cfg = parse(
            r#"{
                "route": {
                    "type": "PrefixSelectorRoute",
                    "policies": { "key": "ErrorRoute|matched" },
                    "wildcard": "NullRoute"
                }
            }"#,
        )
        .unwrap();
        let route = build(&cfg, &MockBackendFactory::new()).unwrap();

        let reply = execute(&route, get(b"/././key|#|suffix")).await.unwrap();
        assert_eq!(reply, server_error(b"matched"));
    }

    #[tokio::test]
    async fn singular_root_rejects_unknown_routing_prefix() {
        let cfg = parse(r#"{"route": "NullRoute"}"#).unwrap();
        let route = build(&cfg, &MockBackendFactory::new()).unwrap();

        assert!(matches!(
            execute(&route, get(b"/other/cluster/key")).await,
            Err(crate::RouteError::NoRoute)
        ));
    }

    #[tokio::test]
    async fn policies_only_selector_rejects_unmatched_key() {
        let cfg = parse(
            r#"{
                "route": {
                    "type": "PrefixSelectorRoute",
                    "policies": { "matched:": "NullRoute" }
                }
            }"#,
        )
        .unwrap();
        let route = build(&cfg, &MockBackendFactory::new()).unwrap();

        assert!(matches!(
            execute(&route, get(b"unmatched:key")).await,
            Err(crate::RouteError::NoRoute)
        ));
    }

    #[tokio::test]
    async fn plural_routes_select_exact_aliases_and_default() {
        let cfg = parse(
            r#"{
                "routes": {
                    "/././": "ErrorRoute|default",
                    "/other/cluster/": "ErrorRoute|other"
                }
            }"#,
        )
        .unwrap();
        let route = build(&cfg, &MockBackendFactory::new()).unwrap();

        assert_eq!(
            execute(&route, get(b"key")).await.unwrap(),
            server_error(b"default")
        );
        assert_eq!(
            execute(&route, get(b"/other/cluster/key")).await.unwrap(),
            server_error(b"other")
        );
        assert!(matches!(
            execute(&route, get(b"/unknown/cluster/key")).await,
            Err(crate::RouteError::NoRoute)
        ));
    }

    #[tokio::test]
    async fn later_duplicate_route_alias_wins() {
        let cfg = parse(
            r#"{
                "routes": [
                    { "aliases": ["/././"], "route": "ErrorRoute|first" },
                    { "aliases": ["/././"], "route": "ErrorRoute|last" }
                ]
            }"#,
        )
        .unwrap();
        let route = build(&cfg, &MockBackendFactory::new()).unwrap();

        assert_eq!(
            execute(&route, get(b"key")).await.unwrap(),
            server_error(b"last")
        );
    }

    #[test]
    fn plural_routes_require_the_default_alias_at_build_time() {
        let cfg = parse(r#"{ "routes": { "/other/cluster/": "NullRoute" } }"#).unwrap();
        let layout = RoutingMetricsLayout::new(&cfg);
        let error = build_route(&cfg, &MockBackendFactory::new(), &defaults(), &layout)
            .err()
            .expect("missing default route should fail the build");

        assert!(matches!(
            error,
            BuildError::DefaultRouteMissing { ref prefix } if prefix == "/././"
        ));
    }

    #[tokio::test]
    async fn custom_root_options_select_default_and_invalid_fallback() {
        let cfg = parse(
            r#"{
                "routes": {
                    "/a/a/": "ErrorRoute|a",
                    "/b/b/": "ErrorRoute|b"
                }
            }"#,
        )
        .unwrap();
        let layout = RoutingMetricsLayout::new(&cfg);
        let options = RootRouteOptions {
            default_route: "/b/b/".parse().unwrap(),
            send_invalid_to_default: true,
        };
        let route = build_route_with_options(
            &cfg,
            &MockBackendFactory::new(),
            &defaults(),
            &layout,
            &options,
        )
        .unwrap();
        let metrics = RoutingMetricsShard::new(layout);
        let fixture = BuiltRoute {
            route,
            state: RoutingState::new(Arc::clone(&metrics), noop_sink()),
            metrics,
        };

        assert_eq!(
            execute(&fixture, get(b"key")).await.unwrap(),
            server_error(b"b")
        );
        assert_eq!(
            execute(&fixture, get(b"/a/a/key")).await.unwrap(),
            server_error(b"a")
        );
        assert_eq!(
            execute(&fixture, get(b"/missing/route/key")).await.unwrap(),
            server_error(b"b")
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn fast_wildcards_keep_default_route_primary() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let cfg = parse(
                    r#"{
                        "routes": {
                            "/us/a/": "ErrorRoute|primary",
                            "/us/b/": "ErrorRoute|secondary",
                            "/eu/c/": "ErrorRoute|remote"
                        }
                    }"#,
                )
                .unwrap();
                let layout = RoutingMetricsLayout::new(&cfg);
                let options = RootRouteOptions {
                    default_route: "/us/a/".parse().unwrap(),
                    send_invalid_to_default: false,
                };
                let route = build_route_with_options(
                    &cfg,
                    &MockBackendFactory::new(),
                    &defaults(),
                    &layout,
                    &options,
                )
                .unwrap();
                let metrics = RoutingMetricsShard::new(layout);
                let fixture = BuiltRoute {
                    route,
                    state: RoutingState::new(Arc::clone(&metrics), noop_sink()),
                    metrics,
                };

                assert_eq!(
                    execute(&fixture, get(b"/us/*/key")).await.unwrap(),
                    server_error(b"primary")
                );
                assert_eq!(
                    execute(&fixture, get(b"/*/*/key")).await.unwrap(),
                    server_error(b"primary")
                );
            })
            .await;
    }

    #[tokio::test]
    async fn unknown_exact_route_uses_regional_fallback_only() {
        let cfg = parse(
            r#"{
                "routes": {
                    "/eu/a/": "ErrorRoute|default",
                    "/us/a/": {
                        "type": "PrefixSelectorRoute",
                        "policies": { "matched:": "ErrorRoute|exact" }
                    },
                    "/us/fallback/": "ErrorRoute|fallback"
                }
            }"#,
        )
        .unwrap();
        let layout = RoutingMetricsLayout::new(&cfg);
        let options = RootRouteOptions {
            default_route: "/eu/a/".parse().unwrap(),
            send_invalid_to_default: false,
        };
        let route = build_route_with_options(
            &cfg,
            &MockBackendFactory::new(),
            &defaults(),
            &layout,
            &options,
        )
        .unwrap();
        let metrics = RoutingMetricsShard::new(layout);
        let fixture = BuiltRoute {
            route,
            state: RoutingState::new(Arc::clone(&metrics), noop_sink()),
            metrics,
        };

        assert_eq!(
            execute(&fixture, get(b"/us/missing/key")).await.unwrap(),
            server_error(b"fallback")
        );
        assert!(matches!(
            execute(&fixture, get(b"/us/a/unmatched:key")).await,
            Err(crate::RouteError::NoRoute)
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn arbitrary_wildcard_uses_slow_path_with_default_primary() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let cfg = parse(
                    r#"{
                        "routes": {
                            "/uk/preprod/": "ErrorRoute|secondary",
                            "/us/dev/": "ErrorRoute|unmatched",
                            "/us/prod/": "ErrorRoute|primary"
                        }
                    }"#,
                )
                .unwrap();
                let layout = RoutingMetricsLayout::new(&cfg);
                let options = RootRouteOptions {
                    default_route: "/us/prod/".parse().unwrap(),
                    send_invalid_to_default: false,
                };
                let route = build_route_with_options(
                    &cfg,
                    &MockBackendFactory::new(),
                    &defaults(),
                    &layout,
                    &options,
                )
                .unwrap();
                let metrics = RoutingMetricsShard::new(layout);
                let fixture = BuiltRoute {
                    route,
                    state: RoutingState::new(Arc::clone(&metrics), noop_sink()),
                    metrics,
                };

                assert_eq!(
                    execute(&fixture, get(b"/u*/*prod/key")).await.unwrap(),
                    server_error(b"primary")
                );
            })
            .await;
    }

    #[tokio::test]
    async fn builds_pool_route_from_shorthand() {
        let json = r#"{"pools": {"P": {"servers": ["unused:1"]}}, "route": "PoolRoute|P"}"#;
        let cfg = parse(json).unwrap();
        let route = build(&cfg, &MockBackendFactory::new()).unwrap();
        let reply = execute(&route, get(b"foo")).await.unwrap();
        assert_eq!(reply, get_miss());
    }

    #[tokio::test]
    async fn builds_pool_route_from_object_form() {
        let json = r#"{"pools": {"P": {"servers": ["unused:1"]}}, "route": {"type": "PoolRoute", "pool": "P"}}"#;
        let cfg = parse(json).unwrap();
        let route = build(&cfg, &MockBackendFactory::new()).unwrap();
        let reply = execute(&route, get(b"foo")).await.unwrap();
        assert_eq!(reply, get_miss());
    }

    #[test]
    fn errors_when_pool_is_missing_from_metrics_layout() {
        let cfg = parse(r#"{"pools": {"P": {"servers": ["unused:1"]}}, "route": "PoolRoute|P"}"#)
            .unwrap();
        let layout = RoutingMetricsLayout::empty();
        let err = build_route(&cfg, &MockBackendFactory::new(), &defaults(), &layout)
            .err()
            .expect("build should fail");
        assert!(matches!(
            err,
            BuildError::PoolMissingFromMetricsLayout { ref name } if name == "P"
        ));
    }

    #[tokio::test]
    async fn builds_failover_route_with_pool_children() {
        let json = r#"{"pools": {"A": {"servers": ["a:1"]}, "B": {"servers": ["b:1"]}}, "route": {"type": "FailoverRoute", "children": ["PoolRoute|A", "PoolRoute|B"]}}"#;
        let cfg = parse(json).unwrap();
        let route = build(&cfg, &MockBackendFactory::new()).unwrap();
        let reply = execute(&route, get(b"foo")).await.unwrap();
        assert_eq!(reply, get_miss());
    }

    #[tokio::test]
    async fn builds_nested_failover() {
        let json = r#"{"pools": {"A": {"servers": ["a:1"]}, "B": {"servers": ["b:1"]}}, "route": {"type": "FailoverRoute", "children": [{"type": "FailoverRoute", "children": ["PoolRoute|A"]}, "PoolRoute|B"]}}"#;
        let cfg = parse(json).unwrap();
        let route = build(&cfg, &MockBackendFactory::new()).unwrap();
        let reply = execute(&route, get(b"foo")).await.unwrap();
        assert_eq!(reply, get_miss());
    }

    #[tokio::test]
    async fn failover_route_surfaces_last_error_when_all_children_fail() {
        let json = r#"{"pools": {"A": {"servers": ["a:1"]}, "B": {"servers": ["b:1"]}}, "route": {"type": "FailoverRoute", "children": ["PoolRoute|A", "PoolRoute|B"]}}"#;
        let cfg = parse(json).unwrap();
        let factory = MockBackendFactory::replying(server_error(b"down"));
        let route = build(&cfg, &factory).unwrap();
        let reply = execute(&route, get(b"foo")).await.unwrap();
        assert_eq!(reply, server_error(b"down"));
    }

    #[test]
    fn pool_referenced_twice_shares_destinations() {
        let factory = MockBackendFactory::new();
        let json = r#"{"pools": {"P": {"servers": ["unused:1"]}}, "route": "PoolRoute|P"}"#;
        let cfg = parse(json).unwrap();
        let d = defaults();
        let layout = RoutingMetricsLayout::new(&cfg);
        let mut builder = RouteBuilder::new(&cfg, &factory, &d, &layout);
        let pool = cfg.pool_id("P").unwrap();
        let d1 = builder.get_or_build_destinations(pool).unwrap();
        let d2 = builder.get_or_build_destinations(pool).unwrap();
        assert!(
            Rc::ptr_eq(&d1[0], &d2[0]),
            "destinations should be shared across references"
        );
    }

    // ── pool config derivation ───────────────────────────────────────────

    fn pool_json(json: &str) -> PoolConfig {
        let document = parse(&format!(
            r#"{{"pools":{{"test":{json}}},"route":"NullRoute"}}"#
        ))
        .unwrap();
        document.pool_by_name("test").unwrap().clone()
    }

    /// The D2 guardrail: a pool server_timeout drags connect_timeout down
    /// with it, so a latency-critical pool can't accidentally wait out a
    /// router-default connect while its caller's deadline has passed.
    #[test]
    fn pool_server_timeout_drags_connect_timeout() {
        let pool = pool_json(r#"{ "servers": ["a:1"], "server_timeout": 200 }"#);
        let cfg = pool_destination_config(&defaults(), &pool);
        assert_eq!(cfg.reply_timeout, Some(Duration::from_millis(200)));
        assert_eq!(cfg.connect_timeout, Some(Duration::from_millis(200)));
    }

    #[test]
    fn explicit_pool_connect_timeout_wins() {
        let pool =
            pool_json(r#"{ "servers": ["a:1"], "server_timeout": 200, "connect_timeout": 50 }"#);
        let cfg = pool_destination_config(&defaults(), &pool);
        assert_eq!(cfg.reply_timeout, Some(Duration::from_millis(200)));
        assert_eq!(cfg.connect_timeout, Some(Duration::from_millis(50)));
    }

    #[test]
    fn pool_without_overrides_keeps_router_defaults() {
        let pool = pool_json(r#"{ "servers": ["a:1"] }"#);
        let cfg = pool_destination_config(&defaults(), &pool);
        assert_eq!(cfg.reply_timeout, defaults().reply_timeout);
        assert_eq!(cfg.connect_timeout, defaults().connect_timeout);
    }
}
