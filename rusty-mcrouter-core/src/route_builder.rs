use std::{collections::BTreeMap, rc::Rc};

use rusty_mcrouter_config::{
    ConfigDocument, FailoverErrorsConfig, FailoverPolicyConfig, HashConfig, HashFunc, RouteEntry,
    RouteHandleConfig,
};
use rusty_mcrouter_net::{Backend, BackendFactory, NetError};
use thiserror::Error;

use crate::{
    failover::{FailoverErrors, FailoverPolicy, InOrderPolicy, LeastFailuresPolicy},
    routes::{DestinationRoute, DynRoute, ErrorRoute, FailoverRoute, NullRoute, PoolRoute, Route},
    selectors::{Ch3, Crc32, Salted, Selector, SelectorBuildError},
};

#[derive(Debug, Error)]
pub enum BuildError {
    #[error("pool `{name}` is referenced by a route but not defined in `pools`")]
    PoolNotFound { name: String },

    #[error("pool `{name}` has zero servers; refusing to construct empty PoolRoute")]
    EmptyPool { name: String },

    #[error("FailoverRoute has zero children; refusing to construct an empty failover")]
    EmptyFailover,

    #[error("failed to connect to backend `{server}` of pool `{pool}`: {source}")]
    ConnectFailed {
        pool: String,
        server: String,
        #[source]
        source: NetError,
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
/// (production: `&ClientFactory`; tests: `&MockBackendFactory`, no sockets).
pub async fn build_route<F: BackendFactory>(
    config: &ConfigDocument,
    factory: &F,
) -> Result<Rc<dyn DynRoute>> {
    let entry = match &config.route {
        RouteEntry::Single(handle) => handle,
        RouteEntry::Prefixed(_) => return Err(BuildError::PrefixRoutingNotImplemented),
    };

    let mut route_builder = RouteBuilder::new(config, factory);
    route_builder.build_handle(entry).await
}

struct RouteBuilder<'a, F: BackendFactory> {
    config: &'a ConfigDocument,
    factory: &'a F,
    pool_cache: BTreeMap<String, Vec<Rc<DestinationRoute<F::Backend>>>>,
}

impl<'a, F: BackendFactory> RouteBuilder<'a, F> {
    fn new(config: &'a ConfigDocument, factory: &'a F) -> Self {
        Self {
            config,
            factory,
            pool_cache: BTreeMap::new(),
        }
    }

    async fn build_handle(&mut self, handle: &RouteHandleConfig) -> Result<Rc<dyn DynRoute>> {
        match handle {
            RouteHandleConfig::NullRoute => Ok(NullRoute.into_dyn()),

            RouteHandleConfig::ErrorRoute { message } => {
                Ok(ErrorRoute::new(message.clone()).into_dyn())
            }

            RouteHandleConfig::PoolRoute { pool, hash } => {
                let destinations = self.get_or_build_destinations(pool).await?;
                build_pool_handle(pool, hash, destinations)
            }

            RouteHandleConfig::FailoverRoute {
                children,
                failover_errors,
                failover_policy,
            } => {
                let mut built = Vec::with_capacity(children.len());
                for child in children {
                    built.push(Box::pin(self.build_handle(child)).await?);
                }
                let errors = build_failover_errors(failover_errors);
                let policy = build_failover_policy(failover_policy, built.len());
                FailoverRoute::new(built, errors, policy)
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
                    let destinations = self.get_or_build_destinations(&args[0]).await?;
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

    async fn get_or_build_destinations(
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

        let mut destinations = Vec::with_capacity(pool_config.servers.len());

        for server in &pool_config.servers {
            // todo - this is an eager connect and will fail if any backend is down, this should become lazy
            let backend = self
                .factory
                .connect(server.as_str())
                .await
                .map_err(|source| BuildError::ConnectFailed {
                    pool: pool_name.to_string(),
                    server: server.clone(),
                    source,
                })?;
            destinations.push(Rc::new(DestinationRoute::<F::Backend>::new(backend)));
        }

        self.pool_cache
            .insert(pool_name.to_string(), destinations.clone());

        Ok(destinations)
    }
}

fn build_pool_handle<B: Backend>(
    pool_name: &str,
    hash: &HashConfig,
    destinations: Vec<Rc<DestinationRoute<B>>>,
) -> Result<Rc<dyn DynRoute>> {
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
    match cfg {
        FailoverErrorsConfig::Default => FailoverErrors::default(),
        FailoverErrorsConfig::All(kinds) => FailoverErrors::new(
            Some(kinds.clone()),
            Some(kinds.clone()),
            Some(kinds.clone()),
        ),
        FailoverErrorsConfig::PerOp {
            gets,
            updates,
            deletes,
        } => FailoverErrors::new(gets.clone(), updates.clone(), deletes.clone()),
    }
}

fn build_failover_policy(cfg: &FailoverPolicyConfig, n: usize) -> Box<dyn FailoverPolicy> {
    match cfg {
        FailoverPolicyConfig::InOrder => Box::new(InOrderPolicy),
        FailoverPolicyConfig::LeastFailures { max_tries } => {
            Box::new(LeastFailuresPolicy::new(n, *max_tries))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::req_get;
    use bytes::Bytes;
    use rusty_mcrouter_config::parse;
    use rusty_mcrouter_net::testing::MockBackendFactory;
    use rusty_mcrouter_protocol::Reply;

    async fn expect_err<F: BackendFactory>(cfg: &ConfigDocument, factory: &F) -> BuildError {
        match build_route(cfg, factory).await {
            Err(e) => e,
            Ok(_) => panic!("expected build_route to fail, but it succeeded"),
        }
    }

    #[tokio::test]
    async fn builds_null_route_from_bare_string() {
        let cfg = parse(r#"{"route": "NullRoute"}"#).unwrap();
        let route = build_route(&cfg, &MockBackendFactory::new()).await.unwrap();
        let reply = route.route_dyn(req_get(b"foo")).await.unwrap();
        assert_eq!(reply, Reply::Get { hits: vec![] });
    }

    #[tokio::test]
    async fn builds_null_route_from_object_form() {
        let cfg = parse(r#"{"route": {"type": "NullRoute"}}"#).unwrap();
        let route = build_route(&cfg, &MockBackendFactory::new()).await.unwrap();
        let reply = route.route_dyn(req_get(b"foo")).await.unwrap();
        assert_eq!(reply, Reply::Get { hits: vec![] });
    }

    #[tokio::test]
    async fn builds_error_route_from_object_with_message() {
        let cfg = parse(r#"{"route": {"type": "ErrorRoute", "message": "boom"}}"#).unwrap();
        let route = build_route(&cfg, &MockBackendFactory::new()).await.unwrap();
        let reply = route.route_dyn(req_get(b"foo")).await.unwrap();
        assert_eq!(reply, Reply::ServerError(Bytes::from_static(b"boom")));
    }

    #[tokio::test]
    async fn builds_error_route_from_shorthand_with_message_arg() {
        let cfg = parse(r#"{"route": "ErrorRoute|nope"}"#).unwrap();
        let route = build_route(&cfg, &MockBackendFactory::new()).await.unwrap();
        let reply = route.route_dyn(req_get(b"foo")).await.unwrap();
        assert_eq!(reply, Reply::ServerError(Bytes::from_static(b"nope")));
    }

    #[tokio::test]
    async fn builds_pool_route_from_shorthand() {
        let json = r#"{"pools": {"P": {"servers": ["unused:1"]}}, "route": "PoolRoute|P"}"#;
        let cfg = parse(json).unwrap();
        let route = build_route(&cfg, &MockBackendFactory::new()).await.unwrap();
        let reply = route.route_dyn(req_get(b"foo")).await.unwrap();
        assert_eq!(reply, Reply::Get { hits: vec![] });
    }

    #[tokio::test]
    async fn builds_pool_route_from_object_form() {
        let json = r#"{"pools": {"P": {"servers": ["unused:1"]}}, "route": {"type": "PoolRoute", "pool": "P"}}"#;
        let cfg = parse(json).unwrap();
        let route = build_route(&cfg, &MockBackendFactory::new()).await.unwrap();
        let reply = route.route_dyn(req_get(b"foo")).await.unwrap();
        assert_eq!(reply, Reply::Get { hits: vec![] });
    }

    #[tokio::test]
    async fn errors_when_pool_not_found() {
        let cfg = parse(r#"{"route": "PoolRoute|missing"}"#).unwrap();
        let err = expect_err(&cfg, &MockBackendFactory::new()).await;
        assert!(matches!(err, BuildError::PoolNotFound { ref name } if name == "missing"));
    }

    #[tokio::test]
    async fn errors_when_pool_has_zero_servers() {
        let cfg = parse(r#"{"pools": {"E": {"servers": []}}, "route": "PoolRoute|E"}"#).unwrap();
        let err = expect_err(&cfg, &MockBackendFactory::new()).await;
        assert!(matches!(err, BuildError::EmptyPool { ref name } if name == "E"));
    }

    #[tokio::test]
    async fn errors_on_unknown_object_route_type() {
        let cfg = parse(r#"{"route": {"type": "AllSyncRoute", "children": []}}"#).unwrap();
        let err = expect_err(&cfg, &MockBackendFactory::new()).await;
        assert!(matches!(
            err,
            BuildError::RouteTypeNotImplemented { ref kind } if kind == "AllSyncRoute"
        ));
    }

    #[tokio::test]
    async fn errors_on_unknown_shorthand_kind() {
        let cfg = parse(r#"{"route": "AllSyncRoute|x"}"#).unwrap();
        let err = expect_err(&cfg, &MockBackendFactory::new()).await;
        assert!(matches!(
            err,
            BuildError::RouteTypeNotImplemented { ref kind } if kind == "AllSyncRoute"
        ));
    }

    #[tokio::test]
    async fn errors_on_empty_failover_children() {
        let cfg = parse(r#"{"route": {"type": "FailoverRoute", "children": []}}"#).unwrap();
        let err = expect_err(&cfg, &MockBackendFactory::new()).await;
        assert!(matches!(err, BuildError::EmptyFailover));
    }

    #[tokio::test]
    async fn builds_failover_route_with_pool_children() {
        let json = r#"{"pools": {"A": {"servers": ["a:1"]}, "B": {"servers": ["b:1"]}}, "route": {"type": "FailoverRoute", "children": ["PoolRoute|A", "PoolRoute|B"]}}"#;
        let cfg = parse(json).unwrap();
        let route = build_route(&cfg, &MockBackendFactory::new()).await.unwrap();
        let reply = route.route_dyn(req_get(b"foo")).await.unwrap();
        assert_eq!(reply, Reply::Get { hits: vec![] });
    }

    #[tokio::test]
    async fn builds_nested_failover() {
        let json = r#"{"pools": {"A": {"servers": ["a:1"]}, "B": {"servers": ["b:1"]}}, "route": {"type": "FailoverRoute", "children": [{"type": "FailoverRoute", "children": ["PoolRoute|A"]}, "PoolRoute|B"]}}"#;
        let cfg = parse(json).unwrap();
        let route = build_route(&cfg, &MockBackendFactory::new()).await.unwrap();
        let reply = route.route_dyn(req_get(b"foo")).await.unwrap();
        assert_eq!(reply, Reply::Get { hits: vec![] });
    }

    #[tokio::test]
    async fn failover_route_surfaces_last_error_when_all_children_fail() {
        let json = r#"{"pools": {"A": {"servers": ["a:1"]}, "B": {"servers": ["b:1"]}}, "route": {"type": "FailoverRoute", "children": ["PoolRoute|A", "PoolRoute|B"]}}"#;
        let cfg = parse(json).unwrap();
        let factory = MockBackendFactory::replying(Reply::ServerError(Bytes::from_static(b"down")));
        let route = build_route(&cfg, &factory).await.unwrap();
        let reply = route.route_dyn(req_get(b"foo")).await.unwrap();
        assert_eq!(reply, Reply::ServerError(Bytes::from_static(b"down")));
    }

    #[tokio::test]
    async fn errors_on_unresolved_bare_reference() {
        let cfg = parse(r#"{"route": "route:made-up"}"#).unwrap();
        let err = expect_err(&cfg, &MockBackendFactory::new()).await;
        assert!(matches!(
            err,
            BuildError::UnresolvedReference { ref name } if name == "route:made-up"
        ));
    }

    #[tokio::test]
    async fn errors_on_prefixed_routes() {
        let json = r#"{"pools": {"A": {"servers": ["x:1"]}}, "routes": [{"aliases": ["/a/"], "route": "PoolRoute|A"}]}"#;
        let cfg = parse(json).unwrap();
        let err = expect_err(&cfg, &MockBackendFactory::new()).await;
        assert!(matches!(err, BuildError::PrefixRoutingNotImplemented));
    }

    #[tokio::test]
    async fn errors_on_pool_route_shorthand_with_wrong_arity() {
        let cfg = parse(r#"{"route": "PoolRoute|a|b"}"#).unwrap();
        let err = expect_err(&cfg, &MockBackendFactory::new()).await;
        assert!(matches!(
            err,
            BuildError::PoolRouteShorthandArity { got: 2 }
        ));
    }

    #[tokio::test]
    async fn errors_on_connect_failure_with_clear_message() {
        let cfg =
            parse(r#"{"pools": {"P": {"servers": ["127.0.0.1:1"]}}, "route": "PoolRoute|P"}"#)
                .unwrap();
        let err = expect_err(&cfg, &MockBackendFactory::failing("127.0.0.1:1")).await;
        let BuildError::ConnectFailed { pool, server, .. } = &err else {
            panic!("expected ConnectFailed, got {err:?}");
        };
        assert_eq!(pool, "P");
        assert_eq!(server, "127.0.0.1:1");
    }

    #[tokio::test]
    async fn pool_referenced_twice_shares_destinations() {
        let factory = MockBackendFactory::new();
        let json = r#"{"pools": {"P": {"servers": ["unused:1"]}}, "route": "PoolRoute|P"}"#;
        let cfg = parse(json).unwrap();
        let mut builder = RouteBuilder::new(&cfg, &factory);
        let d1 = builder.get_or_build_destinations("P").await.unwrap();
        let d2 = builder.get_or_build_destinations("P").await.unwrap();
        assert!(
            Rc::ptr_eq(&d1[0], &d2[0]),
            "destinations should be shared across references"
        );
    }
}
