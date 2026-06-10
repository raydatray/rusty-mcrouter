use std::{collections::BTreeMap, rc::Rc};

use rusty_mcrouter_config::{ConfigDocument, HashConfig, HashFunc, RouteEntry, RouteHandleConfig};
use rusty_mcrouter_net::{Client, NetError};
use thiserror::Error;

use crate::{
    routes::{DestinationRoute, DynRoute, ErrorRoute, NullRoute, PoolRoute, Route},
    selectors::{Ch3, Crc32, Salted, Selector, SelectorBuildError},
};

#[derive(Debug, Error)]
pub enum BuildError {
    #[error("pool `{name}` is referenced by a route but not defined in `pools`")]
    PoolNotFound { name: String },

    #[error("pool `{name}` has zero servers; refusing to construct empty PoolRoute")]
    EmptyPool { name: String },

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

pub async fn build_route(config: &ConfigDocument) -> Result<Rc<dyn DynRoute>> {
    let entry = match &config.route {
        RouteEntry::Single(handle) => handle,
        RouteEntry::Prefixed(_) => return Err(BuildError::PrefixRoutingNotImplemented),
    };

    let mut route_builder = RouteBuilder::new(config);
    route_builder.build_handle(entry).await
}

struct RouteBuilder<'a> {
    config: &'a ConfigDocument,
    pool_cache: BTreeMap<String, Vec<Rc<DestinationRoute>>>,
}

impl<'a> RouteBuilder<'a> {
    fn new(config: &'a ConfigDocument) -> Self {
        Self {
            config,
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
    ) -> Result<Vec<Rc<DestinationRoute>>> {
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
            let client = Client::connect(server.as_str()).await.map_err(|source| {
                BuildError::ConnectFailed {
                    pool: pool_name.to_string(),
                    server: server.clone(),
                    source,
                }
            })?;
            destinations.push(Rc::new(DestinationRoute::new(client)));
        }

        self.pool_cache
            .insert(pool_name.to_string(), destinations.clone());

        Ok(destinations)
    }
}

fn build_pool_handle(
    pool_name: &str,
    hash: &HashConfig,
    destinations: Vec<Rc<DestinationRoute>>,
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

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use rusty_mcrouter_config::parse;
    use rusty_mcrouter_net::testing::mock_backend;
    use rusty_mcrouter_protocol::{Reply, Request};

    fn req_get(keys: &[&'static [u8]]) -> Request {
        Request::Get {
            keys: keys.iter().map(|k| Bytes::from_static(k)).collect(),
        }
    }

    async fn expect_err(cfg: &ConfigDocument) -> BuildError {
        match build_route(cfg).await {
            Err(e) => e,
            Ok(_) => panic!("expected build_route to fail, but it succeeded"),
        }
    }

    #[tokio::test]
    async fn builds_null_route_from_bare_string() {
        let cfg = parse(r#"{"route": "NullRoute"}"#).unwrap();
        let route = build_route(&cfg).await.unwrap();
        let reply = route.route_dyn(req_get(&[b"foo"])).await.unwrap();
        assert_eq!(reply, Reply::Get { hits: vec![] });
    }

    #[tokio::test]
    async fn builds_null_route_from_object_form() {
        let cfg = parse(r#"{"route": {"type": "NullRoute"}}"#).unwrap();
        let route = build_route(&cfg).await.unwrap();
        let reply = route.route_dyn(req_get(&[b"foo"])).await.unwrap();
        assert_eq!(reply, Reply::Get { hits: vec![] });
    }

    #[tokio::test]
    async fn builds_error_route_from_object_with_message() {
        let cfg = parse(r#"{"route": {"type": "ErrorRoute", "message": "boom"}}"#).unwrap();
        let route = build_route(&cfg).await.unwrap();
        let reply = route.route_dyn(req_get(&[b"foo"])).await.unwrap();
        assert_eq!(reply, Reply::ServerError(Bytes::from_static(b"boom")));
    }

    #[tokio::test]
    async fn builds_error_route_from_shorthand_with_message_arg() {
        let cfg = parse(r#"{"route": "ErrorRoute|nope"}"#).unwrap();
        let route = build_route(&cfg).await.unwrap();
        let reply = route.route_dyn(req_get(&[b"foo"])).await.unwrap();
        assert_eq!(reply, Reply::ServerError(Bytes::from_static(b"nope")));
    }

    #[tokio::test]
    async fn builds_pool_route_from_shorthand() {
        let addr = mock_backend(b"END\r\n").await;
        let json =
            format!(r#"{{"pools": {{"P": {{"servers": ["{addr}"]}}}}, "route": "PoolRoute|P"}}"#);
        let cfg = parse(&json).unwrap();
        let route = build_route(&cfg).await.unwrap();
        let reply = route.route_dyn(req_get(&[b"foo"])).await.unwrap();
        assert_eq!(reply, Reply::Get { hits: vec![] });
    }

    #[tokio::test]
    async fn builds_pool_route_from_object_form() {
        let addr = mock_backend(b"END\r\n").await;
        let json = format!(
            r#"{{"pools": {{"P": {{"servers": ["{addr}"]}}}}, "route": {{"type": "PoolRoute", "pool": "P"}}}}"#
        );
        let cfg = parse(&json).unwrap();
        let route = build_route(&cfg).await.unwrap();
        let reply = route.route_dyn(req_get(&[b"foo"])).await.unwrap();
        assert_eq!(reply, Reply::Get { hits: vec![] });
    }

    #[tokio::test]
    async fn errors_when_pool_not_found() {
        let cfg = parse(r#"{"route": "PoolRoute|missing"}"#).unwrap();
        let err = expect_err(&cfg).await;
        assert!(matches!(err, BuildError::PoolNotFound { ref name } if name == "missing"));
    }

    #[tokio::test]
    async fn errors_when_pool_has_zero_servers() {
        let cfg = parse(r#"{"pools": {"E": {"servers": []}}, "route": "PoolRoute|E"}"#).unwrap();
        let err = expect_err(&cfg).await;
        assert!(matches!(err, BuildError::EmptyPool { ref name } if name == "E"));
    }

    #[tokio::test]
    async fn errors_on_unknown_object_route_type() {
        let cfg = parse(r#"{"route": {"type": "FailoverRoute", "children": []}}"#).unwrap();
        let err = expect_err(&cfg).await;
        assert!(matches!(
            err,
            BuildError::RouteTypeNotImplemented { ref kind } if kind == "FailoverRoute"
        ));
    }

    #[tokio::test]
    async fn errors_on_unknown_shorthand_kind() {
        let cfg = parse(r#"{"route": "FailoverRoute|x"}"#).unwrap();
        let err = expect_err(&cfg).await;
        assert!(matches!(
            err,
            BuildError::RouteTypeNotImplemented { ref kind } if kind == "FailoverRoute"
        ));
    }

    #[tokio::test]
    async fn errors_on_unresolved_bare_reference() {
        let cfg = parse(r#"{"route": "route:made-up"}"#).unwrap();
        let err = expect_err(&cfg).await;
        assert!(matches!(
            err,
            BuildError::UnresolvedReference { ref name } if name == "route:made-up"
        ));
    }

    #[tokio::test]
    async fn errors_on_prefixed_routes() {
        let json = r#"{"pools": {"A": {"servers": ["x:1"]}}, "routes": [{"aliases": ["/a/"], "route": "PoolRoute|A"}]}"#;
        let cfg = parse(json).unwrap();
        let err = expect_err(&cfg).await;
        assert!(matches!(err, BuildError::PrefixRoutingNotImplemented));
    }

    #[tokio::test]
    async fn errors_on_pool_route_shorthand_with_wrong_arity() {
        let cfg = parse(r#"{"route": "PoolRoute|a|b"}"#).unwrap();
        let err = expect_err(&cfg).await;
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
        let err = expect_err(&cfg).await;
        let BuildError::ConnectFailed { pool, server, .. } = &err else {
            panic!("expected ConnectFailed, got {err:?}");
        };
        assert_eq!(pool, "P");
        assert_eq!(server, "127.0.0.1:1");
    }

    #[tokio::test]
    async fn pool_referenced_twice_shares_destinations() {
        let addr = mock_backend(b"END\r\n").await;
        let json =
            format!(r#"{{"pools": {{"P": {{"servers": ["{addr}"]}}}}, "route": "PoolRoute|P"}}"#);
        let cfg = parse(&json).unwrap();
        let mut builder = RouteBuilder::new(&cfg);
        let d1 = builder.get_or_build_destinations("P").await.unwrap();
        let d2 = builder.get_or_build_destinations("P").await.unwrap();
        assert!(
            Rc::ptr_eq(&d1[0], &d2[0]),
            "destinations should be shared across references"
        );
    }
}
