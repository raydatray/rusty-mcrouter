use std::{collections::BTreeMap, rc::Rc, time::Duration};

use rusty_mcrouter_backend::tko::FailOpenThresholds;
use rusty_mcrouter_backend::{
    destination, Backend, BackendFactory, BackendFactoryError, PoolHealth,
};
use rusty_mcrouter_config::{
    ConfigDocument, FailoverErrorsConfig, FailoverPolicyConfig, HashConfig, HashFunc, PoolConfig,
    RouteEntry, RouteHandleConfig,
};
use thiserror::Error;

use crate::{
    failover::{code_of_kind, FailoverErrors, FailoverPolicy, InOrderPolicy, LeastFailuresPolicy},
    routes::{DestinationRoute, DynRoute, ErrorRoute, FailoverRoute, NullRoute, PoolRoute, Route},
    selectors::{Ch3, Crc32, Salted, Selector, SelectorBuildError},
    RoutingMetricsLayout,
};

#[derive(Debug, Error)]
pub enum BuildError {
    #[error("pool `{name}` is referenced by a route but not defined in `pools`")]
    PoolNotFound { name: String },

    #[error("pool `{name}` has zero servers; refusing to construct empty PoolRoute")]
    EmptyPool { name: String },

    #[error("pool `{name}` is missing from the routing metrics layout")]
    PoolMissingFromMetricsLayout { name: String },

    #[error("FailoverRoute has zero children; refusing to construct an empty failover")]
    EmptyFailover,

    #[error("invalid server `{server}` in pool `{pool}`: {source}")]
    InvalidServer {
        pool: String,
        server: String,
        #[source]
        source: BackendFactoryError,
    },

    #[error("`PoolRoute|...` shorthand requires exactly 1 arg, got {got}")]
    PoolRouteShorthandArity { got: usize },

    // todo - remove these as we add routes
    #[error("route type `{kind}` is not implemented")]
    RouteTypeNotImplemented { kind: String },

    #[error("`routes` (with prefix aliases) is not implemented ")]
    PrefixRoutingNotImplemented,

    #[error("unresolved reference `{name}`: not a known route type, and named_handles resolution is not implemented")]
    UnresolvedReference { name: String },

    #[error(transparent)]
    Selector(#[from] SelectorBuildError),
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
    let entry = match &config.route {
        RouteEntry::Single(handle) => handle,
        RouteEntry::Prefixed(_) => return Err(BuildError::PrefixRoutingNotImplemented),
    };

    let mut route_builder = RouteBuilder::new(config, factory, defaults, metrics_layout);
    route_builder.build_handle(entry)
}

struct RouteBuilder<'a, F>
where
    F: BackendFactory,
{
    config: &'a ConfigDocument,
    factory: &'a F,
    defaults: &'a destination::DestinationConfig,
    metrics_layout: &'a RoutingMetricsLayout,
    pool_cache: BTreeMap<String, Vec<Rc<DestinationRoute<F::Backend>>>>,
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

    fn build_handle(&mut self, handle: &RouteHandleConfig) -> Result<Rc<dyn DynRoute>> {
        match handle {
            RouteHandleConfig::NullRoute => Ok(NullRoute.into_dyn()),

            RouteHandleConfig::ErrorRoute { message } => {
                Ok(ErrorRoute::new(message.clone()).into_dyn())
            }

            RouteHandleConfig::PoolRoute { pool, hash } => {
                let destinations = self.get_or_build_destinations(pool)?;
                build_pool_handle(pool, hash, destinations)
            }

            RouteHandleConfig::FailoverRoute {
                children,
                failover_errors,
                failover_policy,
            } => {
                let mut built = Vec::with_capacity(children.len());
                for child in children {
                    built.push(self.build_handle(child)?);
                }
                if built.is_empty() {
                    return Err(BuildError::EmptyFailover);
                }
                let errors = build_failover_errors(failover_errors);
                let (policy, max_error_tries) = build_failover_policy(failover_policy, built.len());
                FailoverRoute::new(built, errors, policy, max_error_tries)
                    .map(Route::into_dyn)
                    .ok_or(BuildError::EmptyFailover)
            }

            RouteHandleConfig::Reference(name) => match name.as_str() {
                "NullRoute" => Ok(NullRoute.into_dyn()),
                "ErrorRoute" => Ok(ErrorRoute::new(None).into_dyn()),
                _ => Err(BuildError::UnresolvedReference { name: name.clone() }),
            },

            RouteHandleConfig::Shorthand { kind, args } => match kind.as_str() {
                "NullRoute" => Ok(NullRoute.into_dyn()),
                "ErrorRoute" => Ok(ErrorRoute::new(args.first().cloned()).into_dyn()),
                "PoolRoute" => {
                    if args.len() != 1 {
                        return Err(BuildError::PoolRouteShorthandArity { got: args.len() });
                    }
                    let destinations = self.get_or_build_destinations(&args[0])?;
                    build_pool_handle(&args[0], &HashConfig::default(), destinations)
                }
                other => Err(BuildError::RouteTypeNotImplemented {
                    kind: other.to_string(),
                }),
            },

            RouteHandleConfig::Unknown { kind, .. } => {
                Err(BuildError::RouteTypeNotImplemented { kind: kind.clone() })
            }
        }
    }

    fn get_or_build_destinations(
        &mut self,
        pool_name: &str,
    ) -> Result<Vec<Rc<DestinationRoute<F::Backend>>>> {
        if let Some(cached) = self.pool_cache.get(pool_name) {
            return Ok(cached.clone());
        }

        let pool_config =
            self.config
                .pools
                .get(pool_name)
                .ok_or_else(|| BuildError::PoolNotFound {
                    name: pool_name.to_string(),
                })?;

        if pool_config.servers.is_empty() {
            return Err(BuildError::EmptyPool {
                name: pool_name.to_string(),
            });
        }

        let pool_index = self
            .metrics_layout
            .pool_metrics_index(pool_name)
            .ok_or_else(|| BuildError::PoolMissingFromMetricsLayout {
                name: pool_name.to_string(),
            })?;

        let dest_cfg = pool_destination_config(self.defaults, pool_config);
        let pool_health = PoolHealth {
            pool_name,
            fail_open: pool_config.tko_tracker.map(|config| FailOpenThresholds {
                enter: config.enter(),
                exit: config.exit(),
            }),
        };

        let mut destinations = Vec::with_capacity(pool_config.servers.len());

        for server in &pool_config.servers {
            let backend = self
                .factory
                .make(server.as_str(), &dest_cfg, &pool_health)
                .map_err(|source| BuildError::InvalidServer {
                    pool: pool_name.to_string(),
                    server: server.clone(),
                    source,
                })?;
            destinations.push(Rc::new(DestinationRoute::<F::Backend>::for_pool(
                backend, pool_index,
            )));
        }

        self.pool_cache
            .insert(pool_name.to_string(), destinations.clone());

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
    if let Some(ms) = pool.server_timeout_ms {
        cfg.reply_timeout = Some(Duration::from_millis(ms));
        cfg.connect_timeout = Some(Duration::from_millis(ms));
    }
    if let Some(ms) = pool.connect_timeout_ms {
        cfg.connect_timeout = Some(Duration::from_millis(ms));
    }
    cfg
}

fn build_pool_handle<B>(
    pool_name: &str,
    hash: &HashConfig,
    destinations: Vec<Rc<DestinationRoute<B>>>,
) -> Result<Rc<dyn DynRoute>>
where
    B: Backend,
{
    let selector = build_selector(hash, destinations.len())?;
    let route = PoolRoute::new(pool_name, destinations, selector);

    Ok(route.into_dyn())
}

fn build_selector(hash: &HashConfig, n: usize) -> Result<Box<dyn Selector>> {
    let base: Box<dyn Selector> = match hash.func {
        HashFunc::Ch3 => Box::new(Ch3::new(n)?),
        HashFunc::Crc32 => Box::new(Crc32::new(n)),
    };

    Ok(match &hash.salt {
        Some(salt) => Box::new(Salted::new(base, salt.clone().into_bytes())),
        None => base,
    })
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
        let layout = RoutingMetricsLayout::new(cfg.pools.keys().cloned());
        let route = build_route(cfg, factory, &defaults(), &layout)?;
        let metrics = RoutingMetricsShard::new(layout);
        let state = RoutingState::new(Arc::clone(&metrics), noop_sink());
        Ok(BuiltRoute {
            route,
            metrics,
            state,
        })
    }

    fn expect_err<F>(cfg: &ConfigDocument, factory: &F) -> BuildError
    where
        F: BackendFactory,
    {
        match build(cfg, factory) {
            Err(e) => e,
            Ok(_) => panic!("expected build_route to fail, but it succeeded"),
        }
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
    fn errors_when_pool_not_found() {
        let cfg = parse(r#"{"route": "PoolRoute|missing"}"#).unwrap();
        let err = expect_err(&cfg, &MockBackendFactory::new());
        assert!(matches!(err, BuildError::PoolNotFound { ref name } if name == "missing"));
    }

    #[test]
    fn errors_when_pool_has_zero_servers() {
        let cfg = parse(r#"{"pools": {"E": {"servers": []}}, "route": "PoolRoute|E"}"#).unwrap();
        let err = expect_err(&cfg, &MockBackendFactory::new());
        assert!(matches!(err, BuildError::EmptyPool { ref name } if name == "E"));
    }

    #[test]
    fn errors_when_pool_is_missing_from_metrics_layout() {
        let cfg = parse(r#"{"pools": {"P": {"servers": ["unused:1"]}}, "route": "PoolRoute|P"}"#)
            .unwrap();
        let layout = RoutingMetricsLayout::new(Vec::<String>::new());
        let err = build_route(&cfg, &MockBackendFactory::new(), &defaults(), &layout)
            .err()
            .expect("build should fail");
        assert!(matches!(
            err,
            BuildError::PoolMissingFromMetricsLayout { ref name } if name == "P"
        ));
    }

    #[test]
    fn errors_on_unknown_object_route_type() {
        let cfg = parse(r#"{"route": {"type": "AllSyncRoute", "children": []}}"#).unwrap();
        let err = expect_err(&cfg, &MockBackendFactory::new());
        assert!(matches!(
            err,
            BuildError::RouteTypeNotImplemented { ref kind } if kind == "AllSyncRoute"
        ));
    }

    #[test]
    fn errors_on_unknown_shorthand_kind() {
        let cfg = parse(r#"{"route": "AllSyncRoute|x"}"#).unwrap();
        let err = expect_err(&cfg, &MockBackendFactory::new());
        assert!(matches!(
            err,
            BuildError::RouteTypeNotImplemented { ref kind } if kind == "AllSyncRoute"
        ));
    }

    #[test]
    fn errors_on_empty_failover_children() {
        let cfg = parse(r#"{"route": {"type": "FailoverRoute", "children": []}}"#).unwrap();
        let err = expect_err(&cfg, &MockBackendFactory::new());
        assert!(matches!(err, BuildError::EmptyFailover));
    }

    #[test]
    fn empty_least_failures_returns_build_error_instead_of_panicking() {
        let cfg = parse(
            r#"{"route": {"type": "FailoverRoute", "children": [], "failover_policy": {"type": "LeastFailuresPolicy", "max_tries": 1}}}"#,
        )
        .unwrap();
        let err = expect_err(&cfg, &MockBackendFactory::new());
        assert!(matches!(err, BuildError::EmptyFailover));
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
    fn errors_on_unresolved_bare_reference() {
        let cfg = parse(r#"{"route": "route:made-up"}"#).unwrap();
        let err = expect_err(&cfg, &MockBackendFactory::new());
        assert!(matches!(
            err,
            BuildError::UnresolvedReference { ref name } if name == "route:made-up"
        ));
    }

    #[test]
    fn errors_on_prefixed_routes() {
        let json = r#"{"pools": {"A": {"servers": ["x:1"]}}, "routes": [{"aliases": ["/a/"], "route": "PoolRoute|A"}]}"#;
        let cfg = parse(json).unwrap();
        let err = expect_err(&cfg, &MockBackendFactory::new());
        assert!(matches!(err, BuildError::PrefixRoutingNotImplemented));
    }

    #[test]
    fn errors_on_pool_route_shorthand_with_wrong_arity() {
        let cfg = parse(r#"{"route": "PoolRoute|a|b"}"#).unwrap();
        let err = expect_err(&cfg, &MockBackendFactory::new());
        assert!(matches!(
            err,
            BuildError::PoolRouteShorthandArity { got: 2 }
        ));
    }

    #[test]
    fn errors_on_invalid_server_with_clear_message() {
        let cfg =
            parse(r#"{"pools": {"P": {"servers": ["127.0.0.1:1"]}}, "route": "PoolRoute|P"}"#)
                .unwrap();
        let err = expect_err(&cfg, &MockBackendFactory::failing("127.0.0.1:1"));
        let BuildError::InvalidServer { pool, server, .. } = &err else {
            panic!("expected InvalidServer, got {err:?}");
        };
        assert_eq!(pool, "P");
        assert_eq!(server, "127.0.0.1:1");
    }

    #[test]
    fn pool_referenced_twice_shares_destinations() {
        let factory = MockBackendFactory::new();
        let json = r#"{"pools": {"P": {"servers": ["unused:1"]}}, "route": "PoolRoute|P"}"#;
        let cfg = parse(json).unwrap();
        let d = defaults();
        let layout = RoutingMetricsLayout::new(cfg.pools.keys().cloned());
        let mut builder = RouteBuilder::new(&cfg, &factory, &d, &layout);
        let d1 = builder.get_or_build_destinations("P").unwrap();
        let d2 = builder.get_or_build_destinations("P").unwrap();
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
        document.pools["test"].clone()
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
