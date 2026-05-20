use std::{collections::BTreeMap, sync::Arc};

use rusty_mcrouter_config::{ConfigDocument, RouteEntry, RouteHandleConfig};
use rusty_mcrouter_net::Client;
use thiserror::Error;

use crate::{
    destination_route::DestinationRoute,
    error_route::ErrorRoute,
    null_route::NullRoute,
    pool_route::PoolRoute,
    route::{DynRoute, Route},
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
        source: std::io::Error,
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
}

pub async fn build_route(config: &ConfigDocument) -> Result<Arc<dyn DynRoute>, BuildError> {
    let entry = match &config.route {
        RouteEntry::Single(handle) => handle,
        RouteEntry::Prefixed(_) => return Err(BuildError::PrefixRoutingNotImplemented),
    };

    let mut route_builder = RouteBuilder::new(config);
    route_builder.build_handle(entry).await
}

struct RouteBuilder<'a> {
    config: &'a ConfigDocument,
    pool_cache: BTreeMap<String, Arc<PoolRoute>>,
}

impl<'a> RouteBuilder<'a> {
    fn new(config: &'a ConfigDocument) -> Self {
        Self {
            config,
            pool_cache: BTreeMap::new(),
        }
    }

    async fn build_handle(
        &mut self,
        handle: &RouteHandleConfig,
    ) -> Result<Arc<dyn DynRoute>, BuildError> {
        match handle {
            RouteHandleConfig::NullRoute => Ok(NullRoute.into_dyn()),

            RouteHandleConfig::ErrorRoute { message } => {
                Ok(ErrorRoute::new(message.clone()).into_dyn())
            }

            RouteHandleConfig::PoolRoute { pool } => {
                Ok(self.get_or_build_pool(pool).await?.arc_into_dyn())
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
                    Ok(self.get_or_build_pool(&args[0]).await?.arc_into_dyn())
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

    async fn get_or_build_pool(&mut self, pool_name: &str) -> Result<Arc<PoolRoute>, BuildError> {
        if let Some(cached) = self.pool_cache.get(pool_name) {
            return Ok(Arc::clone(cached));
        }

        let pool_config =
            self.config
                .pools
                .get(pool_name)
                .ok_or_else(|| BuildError::PoolNotFound {
                    name: pool_name.to_string(),
                })?;

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
            destinations.push(Arc::new(DestinationRoute::new(client)));
        }

        let pool = PoolRoute::new(destinations).ok_or_else(|| BuildError::EmptyPool {
            name: pool_name.to_string(),
        })?;

        let pool = Arc::new(pool);
        self.pool_cache
            .insert(pool_name.to_string(), Arc::clone(&pool));

        Ok(pool)
    }
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
    async fn pool_referenced_twice_is_built_once_and_shared() {
        let addr = mock_backend(b"END\r\n").await;
        let json =
            format!(r#"{{"pools": {{"P": {{"servers": ["{addr}"]}}}}, "route": "PoolRoute|P"}}"#);
        let cfg = parse(&json).unwrap();
        let mut builder = RouteBuilder::new(&cfg);
        let p1 = builder.get_or_build_pool("P").await.unwrap();
        let p2 = builder.get_or_build_pool("P").await.unwrap();
        assert!(
            Arc::ptr_eq(&p1, &p2),
            "second call should return the cached Arc"
        );
    }
}
