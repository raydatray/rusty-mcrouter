use std::{
    collections::BTreeMap,
    fmt,
    path::{Path, PathBuf},
};

use json_comments::StripComments;
use serde::de::{self, Deserializer, MapAccess, SeqAccess, Visitor};
use serde::Deserialize;
use serde_json::Value;
use thiserror::Error;

use crate::{
    pool::RawPoolConfig,
    route::{RawNamedHandle, RawRouteConfig},
    HashConfig, HashFunc, PoolConfig, RouteConfig,
};

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read config `{path}`: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("json parse error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("config schema error at `{path}`: {source}")]
    Schema {
        path: String,
        #[source]
        source: serde_json::Error,
    },

    #[error("config must define exactly one of `route` or `routes`; both were provided")]
    BothRouteAndRoutes,

    #[error("config must define exactly one of `route` or `routes`; neither was provided")]
    MissingRoute,

    #[error("invalid tko_tracker for pool `{pool}`: {reason}")]
    InvalidPoolTkoTracker { pool: String, reason: &'static str },

    #[error("{field} for pool `{pool}` must be in 1..=1000000 ms, got {value}")]
    InvalidPoolTimeout {
        pool: String,
        field: &'static str,
        value: u64,
    },

    #[error("pool `{pool}` uses unsupported protocol `{protocol}`")]
    UnsupportedPoolProtocol { pool: String, protocol: String },

    #[error("pool `{pool}` option `{option}` is invalid")]
    InvalidPoolOption { pool: String, option: &'static str },

    #[error("server object at `pools.{pool}.servers[{index}]` is not implemented")]
    UnsupportedServerObject { pool: String, index: usize },

    #[error("invalid server `{address}` at `pools.{pool}.servers[{index}]`")]
    InvalidServerAddress {
        pool: String,
        index: usize,
        address: String,
    },

    #[error("`routes` (with prefix aliases) is not implemented")]
    PrefixRoutingNotImplemented,

    #[error("route type `{kind}` is not implemented")]
    UnsupportedRouteType { kind: String },

    #[error("unresolved route reference `{name}`")]
    UnresolvedReference { name: String },

    #[error("named route handle cycle: {chain}")]
    NamedHandleCycle { chain: String },

    #[error("route nesting exceeds the maximum depth of {limit}")]
    RouteDepthExceeded { limit: usize },

    #[error("pool `{name}` is referenced by a route but not defined in `pools`")]
    PoolNotFound { name: String },

    #[error("pool `{name}` has zero servers; refusing to construct empty PoolRoute")]
    EmptyPool { name: String },

    #[error("FailoverRoute has zero children; refusing to construct an empty failover")]
    EmptyFailover,

    #[error("`{kind}` shorthand expects {expected} arguments, got {actual}")]
    InvalidShorthandArity {
        kind: String,
        expected: &'static str,
        actual: usize,
    },

    #[error("Ch3 pool `{pool}` has {size} servers; expected 1..=8388608")]
    InvalidCh3PoolSize { pool: String, size: usize },
}

type ConfigResult<T> = std::result::Result<T, ConfigError>;
const MAX_ROUTE_DEPTH: usize = 128;

#[derive(Clone, Debug, PartialEq)]
pub struct ConfigDocument {
    pools: Vec<PoolConfig>,
    pool_ids: BTreeMap<String, PoolId>,
    route: RouteConfig,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PoolId(usize);

impl PoolId {
    pub fn index(self) -> usize {
        self.0
    }
}

#[derive(Clone, Debug, PartialEq)]
struct RawPrefixedRoute {
    aliases: Vec<String>,
    route: RawRouteConfig,
}

impl ConfigDocument {
    pub fn pools(&self) -> impl ExactSizeIterator<Item = (PoolId, &PoolConfig)> {
        self.pools
            .iter()
            .enumerate()
            .map(|(index, pool)| (PoolId(index), pool))
    }

    pub fn pool(&self, id: PoolId) -> &PoolConfig {
        &self.pools[id.0]
    }

    pub fn pool_by_name(&self, name: &str) -> Option<&PoolConfig> {
        self.pool_ids.get(name).map(|id| self.pool(*id))
    }

    pub fn pool_id(&self, name: &str) -> Option<PoolId> {
        self.pool_ids.get(name).copied()
    }

    pub fn pool_names(&self) -> impl ExactSizeIterator<Item = &str> {
        self.pools.iter().map(PoolConfig::name)
    }

    pub fn route(&self) -> &RouteConfig {
        &self.route
    }
}

#[derive(Deserialize)]
struct RawConfigDocument {
    #[serde(default)]
    pools: BTreeMap<String, RawPoolConfig>,
    #[serde(default, deserialize_with = "deserialize_named_handles")]
    named_handles: BTreeMap<String, RawRouteConfig>,
    #[serde(default, deserialize_with = "deserialize_route_field")]
    route: Option<RawRouteConfig>,
    #[serde(default, deserialize_with = "deserialize_routes_field")]
    routes: Option<Vec<RawPrefixedRoute>>,
}

impl TryFrom<RawConfigDocument> for ConfigDocument {
    type Error = ConfigError;

    fn try_from(raw: RawConfigDocument) -> ConfigResult<Self> {
        let RawConfigDocument {
            pools: raw_pools,
            named_handles,
            route,
            routes,
        } = raw;

        let root = match (route, routes) {
            (Some(_), Some(_)) => return Err(ConfigError::BothRouteAndRoutes),
            (None, None) => return Err(ConfigError::MissingRoute),
            (Some(single), None) => single,
            (None, Some(_)) => return Err(ConfigError::PrefixRoutingNotImplemented),
        };

        let mut pools = Vec::with_capacity(raw_pools.len());
        let mut pool_ids = BTreeMap::new();
        for (name, raw_pool) in raw_pools {
            let id = PoolId(pools.len());
            pools.push(raw_pool.validate(&name)?);
            pool_ids.insert(name, id);
        }

        let mut validator = RouteValidator::new(&pools, &pool_ids, named_handles);
        let route = validator.validate(root, 0)?;
        validator.validate_all_named_handles()?;

        Ok(ConfigDocument {
            pools,
            pool_ids,
            route,
        })
    }
}

struct RouteValidator<'a> {
    pools: &'a [PoolConfig],
    pool_ids: &'a BTreeMap<String, PoolId>,
    definitions: BTreeMap<String, RawRouteConfig>,
    resolved: BTreeMap<String, RouteConfig>,
    resolving: Vec<String>,
}

impl<'a> RouteValidator<'a> {
    fn new(
        pools: &'a [PoolConfig],
        pool_ids: &'a BTreeMap<String, PoolId>,
        definitions: BTreeMap<String, RawRouteConfig>,
    ) -> Self {
        Self {
            pools,
            pool_ids,
            definitions,
            resolved: BTreeMap::new(),
            resolving: Vec::new(),
        }
    }

    fn validate_all_named_handles(&mut self) -> ConfigResult<()> {
        let names = self.definitions.keys().cloned().collect::<Vec<_>>();

        for name in names {
            self.resolve_named(&name, 0)?;
        }

        Ok(())
    }

    fn resolve_named(&mut self, name: &str, depth: usize) -> ConfigResult<RouteConfig> {
        check_route_depth(depth)?;

        if let Some(route) = self.resolved.get(name) {
            check_validated_route_depth(route, depth)?;
            return Ok(route.clone());
        }

        if let Some(start) = self.resolving.iter().position(|current| current == name) {
            let mut cycle = self.resolving[start..].to_vec();
            cycle.push(name.to_string());
            return Err(ConfigError::NamedHandleCycle {
                chain: cycle.join(" -> "),
            });
        }

        let route = self.definitions.get(name).cloned().ok_or_else(|| {
            ConfigError::UnresolvedReference {
                name: name.to_string(),
            }
        })?;

        self.resolving.push(name.to_string());

        let route = self.validate(route, depth)?;

        self.resolving.pop();
        self.resolved.insert(name.to_string(), route.clone());

        Ok(route)
    }

    fn validate(&mut self, route: RawRouteConfig, depth: usize) -> ConfigResult<RouteConfig> {
        check_route_depth(depth)?;

        match route {
            RawRouteConfig::Reference(name) => match name.as_str() {
                "NullRoute" => Ok(RouteConfig::NullRoute),
                "ErrorRoute" => Ok(RouteConfig::ErrorRoute { message: None }),
                _ => self.resolve_named(&name, depth + 1),
            },
            RawRouteConfig::Shorthand { kind, args } => self.validate_shorthand(kind, args, depth),
            RawRouteConfig::PoolRoute { pool, hash } => {
                let id = self
                    .pool_ids
                    .get(&pool)
                    .copied()
                    .ok_or_else(|| ConfigError::PoolNotFound { name: pool.clone() })?;

                let config = &self.pools[id.0];
                if config.servers().is_empty() {
                    return Err(ConfigError::EmptyPool { name: pool });
                }

                if hash.func == HashFunc::Ch3 && config.servers().len() > 1 << 23 {
                    return Err(ConfigError::InvalidCh3PoolSize {
                        pool,
                        size: config.servers().len(),
                    });
                }

                Ok(RouteConfig::PoolRoute { pool: id, hash })
            }
            RawRouteConfig::FailoverRoute {
                children,
                failover_errors,
                failover_policy,
            } => {
                if children.is_empty() {
                    return Err(ConfigError::EmptyFailover);
                }

                let children = children
                    .into_iter()
                    .map(|child| self.validate(child, depth + 1))
                    .collect::<ConfigResult<_>>()?;

                Ok(RouteConfig::FailoverRoute {
                    children,
                    failover_errors,
                    failover_policy,
                })
            }
            RawRouteConfig::NullRoute => Ok(RouteConfig::NullRoute),
            RawRouteConfig::ErrorRoute { message } => Ok(RouteConfig::ErrorRoute { message }),
            RawRouteConfig::Unknown { kind, .. } => Err(ConfigError::UnsupportedRouteType { kind }),
        }
    }

    fn validate_shorthand(
        &mut self,
        kind: String,
        args: Vec<String>,
        depth: usize,
    ) -> ConfigResult<RouteConfig> {
        match kind.as_str() {
            "NullRoute" if args.is_empty() => Ok(RouteConfig::NullRoute),
            "NullRoute" => Err(ConfigError::InvalidShorthandArity {
                kind,
                expected: "no",
                actual: args.len(),
            }),
            "ErrorRoute" if args.len() <= 1 => Ok(RouteConfig::ErrorRoute {
                message: args.into_iter().next(),
            }),
            "ErrorRoute" => Err(ConfigError::InvalidShorthandArity {
                kind,
                expected: "at most 1",
                actual: args.len(),
            }),
            "PoolRoute" if args.len() == 1 => self.validate(
                RawRouteConfig::PoolRoute {
                    pool: args.into_iter().next().expect("one argument"),
                    hash: HashConfig::default(),
                },
                depth,
            ),
            "PoolRoute" => Err(ConfigError::InvalidShorthandArity {
                kind,
                expected: "exactly 1",
                actual: args.len(),
            }),
            _ => Err(ConfigError::UnsupportedRouteType { kind }),
        }
    }
}

fn check_route_depth(depth: usize) -> ConfigResult<()> {
    if depth >= MAX_ROUTE_DEPTH {
        return Err(ConfigError::RouteDepthExceeded {
            limit: MAX_ROUTE_DEPTH,
        });
    }
    Ok(())
}

fn check_validated_route_depth(route: &RouteConfig, depth: usize) -> ConfigResult<()> {
    check_route_depth(depth)?;
    if let RouteConfig::FailoverRoute { children, .. } = route {
        for child in children {
            check_validated_route_depth(child, depth + 1)?;
        }
    }
    Ok(())
}

pub fn parse(input: &str) -> ConfigResult<ConfigDocument> {
    let stripped = StripComments::new(input.as_bytes());
    let mut deserializer = serde_json::Deserializer::from_reader(stripped);
    let raw: RawConfigDocument =
        serde_path_to_error::deserialize(&mut deserializer).map_err(|error| {
            let path = error.path().to_string();
            let source = error.into_inner();
            match source.classify() {
                serde_json::error::Category::Data => ConfigError::Schema { path, source },
                _ => ConfigError::Json(source),
            }
        })?;
    deserializer.end().map_err(ConfigError::Json)?;

    ConfigDocument::try_from(raw)
}

pub fn parse_file(path: impl AsRef<Path>) -> ConfigResult<ConfigDocument> {
    let path = path.as_ref();
    let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
        path: path.to_owned(),
        source,
    })?;
    parse(&text)
}

fn deserialize_route_field<'de, D>(deserializer: D) -> Result<Option<RawRouteConfig>, D::Error>
where
    D: Deserializer<'de>,
{
    RawRouteConfig::deserialize(deserializer).map(Some)
}

fn deserialize_named_handles<'de, D>(
    deserializer: D,
) -> Result<BTreeMap<String, RawRouteConfig>, D::Error>
where
    D: Deserializer<'de>,
{
    struct NamedHandlesVisitor;

    impl<'de> Visitor<'de> for NamedHandlesVisitor {
        type Value = BTreeMap<String, RawRouteConfig>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a named_handles object or array")
        }

        fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
        where
            M: MapAccess<'de>,
        {
            let mut handles = BTreeMap::new();
            while let Some(name) = map.next_key::<String>()? {
                if handles.contains_key(&name) {
                    return Err(de::Error::custom(format!(
                        "duplicate named handle `{name}`"
                    )));
                }
                handles.insert(name, map.next_value()?);
            }
            Ok(handles)
        }

        fn visit_seq<S>(self, mut sequence: S) -> Result<Self::Value, S::Error>
        where
            S: SeqAccess<'de>,
        {
            let mut handles = BTreeMap::new();
            while let Some(RawNamedHandle { name, route }) = sequence.next_element()? {
                if handles.insert(name.clone(), route).is_some() {
                    return Err(de::Error::custom(format!(
                        "duplicate named handle `{name}`"
                    )));
                }
            }
            Ok(handles)
        }
    }

    deserializer.deserialize_any(NamedHandlesVisitor)
}

#[derive(Deserialize)]
struct RawPrefixedRouteEntry {
    aliases: Vec<String>,
    route: RawRouteConfig,
}

fn deserialize_routes_field<'de, D>(
    deserializer: D,
) -> Result<Option<Vec<RawPrefixedRoute>>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    let entries: Result<Vec<RawPrefixedRoute>, D::Error> = match value {
        Value::Object(map) => map
            .into_iter()
            .map(|(prefix, val)| {
                let route = serde_json::from_value(val).map_err(de::Error::custom)?;
                Ok(RawPrefixedRoute {
                    aliases: vec![prefix],
                    route,
                })
            })
            .collect(),
        Value::Array(items) => items
            .into_iter()
            .map(|item| {
                let entry: RawPrefixedRouteEntry =
                    serde_json::from_value(item).map_err(de::Error::custom)?;
                Ok(RawPrefixedRoute {
                    aliases: entry.aliases,
                    route: entry.route,
                })
            })
            .collect(),
        other => {
            return Err(de::Error::custom(format!(
                "routes must be an object or array, got {other}"
            )))
        }
    };

    entries.map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_ok(json: &str) -> ConfigDocument {
        crate::parse(json).unwrap_or_else(|e| panic!("expected ok parse, got error: {e}"))
    }

    fn parse_err(json: &str) -> ConfigError {
        crate::parse(json).expect_err("expected parse to fail")
    }

    #[test]
    fn smallest_valid_config_only_has_route() {
        let doc = parse_ok(r#"{ "route": "NullRoute" }"#);
        assert_eq!(doc.pools().len(), 0);
        assert_eq!(doc.route(), &RouteConfig::NullRoute);
    }

    #[test]
    fn pools_and_route_round_trip() {
        let doc =
            parse_ok(r#"{ "pools": { "foo": { "servers": ["a:1"] } }, "route": "PoolRoute|foo" }"#);
        assert_eq!(doc.pools().len(), 1);
        assert_eq!(
            doc.pool_by_name("foo").unwrap().servers()[0].access_point(),
            "a:1"
        );
        assert!(matches!(
            doc.route(),
            RouteConfig::PoolRoute { pool, .. }
                if doc.pool(*pool).name() == "foo"
        ));
    }

    #[test]
    fn named_handles_object_form_resolves_root() {
        let doc = parse_ok(
            r#"{
                "pools": {
                    "A": { "servers": ["a:1"] },
                    "B": { "servers": ["b:1"] }
                },
                "named_handles": {
                    "route:A": { "type": "PoolRoute", "pool": "A" },
                    "route:B": "PoolRoute|B"
                },
                "route": "route:A"
            }"#,
        );
        assert!(matches!(
            doc.route(),
            RouteConfig::PoolRoute { pool, .. } if doc.pool(*pool).name() == "A"
        ));
    }

    #[test]
    fn named_handles_list_form_resolves_root() {
        let doc = parse_ok(
            r#"{
                "pools": { "A": { "servers": ["a:1"] } },
                "named_handles": [
                    { "type": "PoolRoute", "name": "route:A", "pool": "A" },
                    { "type": "NullRoute", "name": "n" }
                ],
                "route": "route:A"
            }"#,
        );
        assert!(matches!(
            doc.route(),
            RouteConfig::PoolRoute { pool, .. } if doc.pool(*pool).name() == "A"
        ));
    }

    #[test]
    fn routes_plural_array_form_is_rejected_until_supported() {
        let error = parse_err(
            r#"{
                "pools": { "A": { "servers": ["x:1"] }, "B": { "servers": ["y:1"] } },
                "routes": [
                    { "aliases": ["/a/a/"], "route": "PoolRoute|A" },
                    { "aliases": ["/b/b/"], "route": "PoolRoute|B" }
                ]
            }"#,
        );
        assert!(matches!(error, ConfigError::PrefixRoutingNotImplemented));
    }

    #[test]
    fn routes_plural_object_form_is_rejected_until_supported() {
        let error = parse_err(
            r#"{
                "pools": { "A": { "servers": ["x:1"] } },
                "routes": { "/foo/bar/": "PoolRoute|A" }
            }"#,
        );
        assert!(matches!(error, ConfigError::PrefixRoutingNotImplemented));
    }

    #[test]
    fn rejects_when_neither_route_nor_routes_present() {
        let err = parse_err(r#"{ "pools": { "A": { "servers": [] } } }"#);
        assert!(matches!(err, ConfigError::MissingRoute));
    }

    #[test]
    fn rejects_when_both_route_and_routes_present() {
        let err = parse_err(
            r#"{ "route": "NullRoute", "routes": [ { "aliases": ["/x/"], "route": "NullRoute" } ] }"#,
        );
        assert!(matches!(err, ConfigError::BothRouteAndRoutes));
    }

    #[test]
    fn validates_executable_route_invariants() {
        assert!(matches!(
            parse_err(r#"{ "route": "PoolRoute|missing" }"#),
            ConfigError::PoolNotFound { ref name } if name == "missing"
        ));
        assert!(matches!(
            parse_err(
                r#"{ "pools": { "empty": { "servers": [] } }, "route": "PoolRoute|empty" }"#
            ),
            ConfigError::EmptyPool { ref name } if name == "empty"
        ));
        assert!(matches!(
            parse_err(r#"{ "route": { "type": "FailoverRoute", "children": [] } }"#),
            ConfigError::EmptyFailover
        ));
        assert!(matches!(
            parse_err(r#"{ "route": "PoolRoute|a|b" }"#),
            ConfigError::InvalidShorthandArity { ref kind, actual: 2, .. }
                if kind == "PoolRoute"
        ));
        assert!(matches!(
            parse_err(r#"{ "route": { "type": "AllSyncRoute" } }"#),
            ConfigError::UnsupportedRouteType { ref kind } if kind == "AllSyncRoute"
        ));
    }

    #[test]
    fn resolves_named_handles_and_rejects_cycles() {
        let document = parse_ok(
            r#"{
                "pools": { "A": { "servers": ["a:1"] } },
                "named_handles": {
                    "first": "second",
                    "second": "PoolRoute|A"
                },
                "route": "first"
            }"#,
        );
        assert!(matches!(
            document.route(),
            RouteConfig::PoolRoute { pool, .. } if document.pool(*pool).name() == "A"
        ));

        assert!(matches!(
            parse_err(
                r#"{
                    "named_handles": {
                        "first": "second",
                        "second": "first"
                    },
                    "route": "first"
                }"#
            ),
            ConfigError::NamedHandleCycle { .. }
        ));

        assert!(matches!(
            parse_err(
                r#"{
                    "named_handles": { "self": "self" },
                    "route": "self"
                }"#
            ),
            ConfigError::NamedHandleCycle { .. }
        ));
    }

    #[test]
    fn rejects_excessive_named_handle_depth() {
        let mut handles = serde_json::Map::new();
        for index in 0..=MAX_ROUTE_DEPTH {
            let target = if index == MAX_ROUTE_DEPTH {
                "NullRoute".to_string()
            } else {
                format!("route:{}", index + 1)
            };
            handles.insert(format!("route:{index}"), Value::String(target));
        }
        let json = serde_json::json!({
            "named_handles": handles,
            "route": "route:0",
        })
        .to_string();

        assert!(matches!(
            parse_err(&json),
            ConfigError::RouteDepthExceeded {
                limit: MAX_ROUTE_DEPTH
            }
        ));
    }

    #[test]
    fn rejects_excessive_nested_route_depth() {
        let mut route = RawRouteConfig::NullRoute;
        for _ in 0..=MAX_ROUTE_DEPTH {
            route = RawRouteConfig::FailoverRoute {
                children: vec![route],
                failover_errors: crate::FailoverErrorsConfig::Default,
                failover_policy: crate::FailoverPolicyConfig::InOrder,
            };
        }

        let pools = Vec::new();
        let pool_ids = BTreeMap::new();
        let mut validator = RouteValidator::new(&pools, &pool_ids, BTreeMap::new());
        assert!(matches!(
            validator.validate(route, 0),
            Err(ConfigError::RouteDepthExceeded {
                limit: MAX_ROUTE_DEPTH
            })
        ));
    }

    #[test]
    fn permits_unreferenced_empty_pools() {
        let document =
            parse_ok(r#"{ "pools": { "empty": { "servers": [] } }, "route": "NullRoute" }"#);
        assert!(document.pool_by_name("empty").unwrap().servers().is_empty());
    }

    #[test]
    fn rejects_null_and_duplicate_document_fields() {
        assert!(matches!(
            parse_err(r#"{ "route": null }"#),
            ConfigError::Schema { ref path, .. } if path == "route"
        ));
        assert!(matches!(
            parse_err(r#"{ "route": "NullRoute", "route": "ErrorRoute" }"#),
            ConfigError::Schema { .. }
        ));
    }

    #[test]
    fn rejects_trailing_input() {
        for json in [
            r#"{ "route": "NullRoute" } {}"#,
            r#"{ "route": "NullRoute" } garbage"#,
        ] {
            assert!(matches!(parse_err(json), ConfigError::Json(_)));
        }
    }

    #[test]
    fn rejects_duplicate_list_form_named_handles() {
        assert!(matches!(
            parse_err(
                r#"{
                    "named_handles": [
                        { "name": "same", "type": "NullRoute" },
                        { "name": "same", "type": "ErrorRoute" }
                    ],
                    "route": "same"
                }"#
            ),
            ConfigError::Schema { ref path, .. } if path.starts_with("named_handles")
        ));
    }

    #[test]
    fn rejects_duplicate_route_fields_in_list_form_named_handles() {
        assert!(matches!(
            parse_err(
                r#"{
                    "pools": {
                        "A": { "servers": ["a:1"] },
                        "B": { "servers": ["b:1"] }
                    },
                    "named_handles": [{
                        "name": "same",
                        "type": "PoolRoute",
                        "pool": "A",
                        "pool": "B"
                    }],
                    "route": "same"
                }"#
            ),
            ConfigError::Schema { ref path, .. } if path.starts_with("named_handles")
        ));
    }

    #[test]
    fn rejects_duplicate_object_form_named_handles() {
        assert!(matches!(
            parse_err(
                r#"{
                    "named_handles": {
                        "same": { "type": "NullRoute" },
                        "same": { "type": "ErrorRoute" }
                    },
                    "route": "same"
                }"#
            ),
            ConfigError::Schema { ref path, .. } if path == "named_handles"
        ));
    }

    #[test]
    fn parse_file_reports_the_path_for_read_errors() {
        let path = Path::new("/definitely/missing/rusty-mcrouter-config.json");
        assert!(matches!(
            parse_file(path),
            Err(ConfigError::Read { path: ref actual, .. }) if actual == path
        ));
    }
}
