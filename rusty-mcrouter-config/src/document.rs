use std::{collections::BTreeMap, path::Path};

use json_comments::StripComments;
use serde::de::{self, Deserializer};
use serde::Deserialize;
use serde_json::Value;
use thiserror::Error;

use crate::{pool::RawPoolConfig, HashConfig, HashFunc, PoolConfig, RouteHandleConfig};

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("json parse error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("config must define exactly one of `route` or `routes`; both were provided")]
    BothRouteAndRoutes,

    #[error("config must define exactly one of `route` or `routes`; neither was provided")]
    MissingRoute,

    #[error("invalid tko_tracker for pool `{pool}`: {reason}")]
    InvalidPoolTkoTracker { pool: String, reason: &'static str },

    #[error("server object at `pools.{pool}.servers[{index}]` is not implemented")]
    UnsupportedServerObject { pool: String, index: usize },

    #[error("`routes` (with prefix aliases) is not implemented")]
    PrefixRoutingNotImplemented,

    #[error("route type `{kind}` is not implemented")]
    UnsupportedRouteType { kind: String },

    #[error("unresolved route reference `{name}`")]
    UnresolvedReference { name: String },

    #[error("named route handle cycle: {chain}")]
    NamedHandleCycle { chain: String },

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

#[derive(Clone, Debug, PartialEq)]
pub struct ConfigDocument {
    pub pools: BTreeMap<String, PoolConfig>,
    pub named_handles: BTreeMap<String, RouteHandleConfig>,
    pub route: RouteEntry,
}

#[derive(Clone, Debug, PartialEq)]
pub enum RouteEntry {
    Single(RouteHandleConfig),
    Prefixed(Vec<PrefixedRoute>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct PrefixedRoute {
    pub aliases: Vec<String>,
    pub route: RouteHandleConfig,
}

#[derive(Deserialize)]
struct RawConfigDocument {
    #[serde(default)]
    pools: BTreeMap<String, RawPoolConfig>,

    #[serde(default, deserialize_with = "deserialize_named_handles")]
    named_handles: BTreeMap<String, RouteHandleConfig>,

    #[serde(default)]
    route: Option<RouteHandleConfig>,

    #[serde(default, deserialize_with = "deserialize_routes_field")]
    routes: Option<Vec<PrefixedRoute>>,
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

        let pools = raw_pools
            .into_iter()
            .map(|(name, pool)| pool.validate(&name).map(|pool| (name, pool)))
            .collect::<ConfigResult<_>>()?;

        let mut validator = RouteValidator::new(&pools, named_handles);
        let route = RouteEntry::Single(validator.validate(root)?);
        let named_handles = validator.validate_all_named_handles()?;

        Ok(ConfigDocument {
            pools,
            named_handles,
            route,
        })
    }
}

struct RouteValidator<'a> {
    pools: &'a BTreeMap<String, PoolConfig>,
    definitions: BTreeMap<String, RawRouteConfig>,
    resolved: BTreeMap<String, RouteHandleConfig>,
    resolving: Vec<String>,
}

impl<'a> RouteValidator<'a> {
    fn new(
        pools: &'a BTreeMap<String, PoolConfig>,
        definitions: BTreeMap<String, RawRouteConfig>,
    ) -> Self {
        Self {
            pools,
            definitions,
            resolved: BTreeMap::new(),
            resolving: Vec::new(),
        }
    }

    fn validate_all_named_handles(&mut self) -> ConfigResult<BTreeMap<String, RouteHandleConfig>> {
        let names = self.definitions.keys().cloned().collect::<Vec<_>>();

        for name in names {
            self.resolve_named(&name)?;
        }

        Ok(self.resolved.clone())
    }

    fn resolve_named(&mut self, name: &str) -> ConfigResult<RouteHandleConfig> {
        if let Some(route) = self.resolved.get(name) {
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

        let route = self.validate(route)?;

        self.resolving.pop();
        self.resolved.insert(name.to_string(), route.clone());

        Ok(route)
    }

    fn validate(&mut self, route: RouteHandleConfig) -> ConfigResult<RouteHandleConfig> {
        match route {
            RawRouteConfig::Reference(name) => match name.as_str() {
                "NullRoute" => Ok(RouteHandleConfig::NullRoute),
                "ErrorRoute" => Ok(RouteHandleConfig::ErrorRoute { message: None }),
                _ => self.resolve_named(&name),
            },
            RawRouteConfig::Shorthand { kind, args } => self.validate_shorthand(kind, args),
            RawRouteConfig::PoolRoute { pool, hash } => {
                let config = self
                    .pools
                    .get(&pool)
                    .ok_or_else(|| ConfigError::PoolNotFound { name: pool.clone() })?;

                if config.servers.is_empty() {
                    return Err(ConfigError::EmptyPool { name: pool });
                }

                if hash.func == HashFunc::Ch3 && config.servers.len() > 1 << 23 {
                    return Err(ConfigError::InvalidCh3PoolSize {
                        pool,
                        size: config.servers.len(),
                    });
                }

                Ok(RouteHandleConfig::PoolRoute { pool, hash })
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
                    .map(|child| self.validate(child))
                    .collect::<ConfigResult<_>>()?;

                Ok(RouteHandleConfig::FailoverRoute {
                    children,
                    failover_errors,
                    failover_policy,
                })
            }
            RouteHandleConfig::NullRoute | RouteHandleConfig::ErrorRoute { .. } => Ok(route),
            RouteHandleConfig::Unknown { kind, .. } => {
                Err(ConfigError::UnsupportedRouteType { kind })
            }
        }
    }

    fn validate_shorthand(
        &mut self,
        kind: String,
        args: Vec<String>,
    ) -> ConfigResult<RouteHandleConfig> {
        match kind.as_str() {
            "NullRoute" if args.is_empty() => Ok(RouteHandleConfig::NullRoute),
            "NullRoute" => Err(ConfigError::InvalidShorthandArity {
                kind,
                expected: "no",
                actual: args.len(),
            }),
            "ErrorRoute" if args.len() <= 1 => Ok(RouteHandleConfig::ErrorRoute {
                message: args.into_iter().next(),
            }),
            "ErrorRoute" => Err(ConfigError::InvalidShorthandArity {
                kind,
                expected: "at most 1",
                actual: args.len(),
            }),
            "PoolRoute" if args.len() == 1 => self.validate(RawRouteConfig::PoolRoute {
                pool: args.into_iter().next().expect("one argument"),
                hash: HashConfig::default(),
            }),
            "PoolRoute" => Err(ConfigError::InvalidShorthandArity {
                kind,
                expected: "exactly 1",
                actual: args.len(),
            }),
            _ => Err(ConfigError::UnsupportedRouteType { kind }),
        }
    }
}

pub fn parse(input: &str) -> ConfigResult<ConfigDocument> {
    let stripped = StripComments::new(input.as_bytes());
    let raw: RawConfigDocument = serde_json::from_reader(stripped)?;

    ConfigDocument::try_from(raw)
}

pub fn parse_file(path: impl AsRef<Path>) -> ConfigResult<ConfigDocument> {
    let text = std::fs::read_to_string(path)?;
    parse(&text)
}

fn deserialize_named_handles<'de, D>(
    deserializer: D,
) -> Result<BTreeMap<String, RouteHandleConfig>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    match value {
        Value::Object(map) => map
            .into_iter()
            .map(|(name, val)| {
                let route = serde_json::from_value(val).map_err(de::Error::custom)?;

                Ok((name, route))
            })
            .collect(),
        Value::Array(items) => items
            .into_iter()
            .map(|item| {
                let mut obj = match item {
                    Value::Object(o) => o,
                    other => {
                        return Err(de::Error::custom(format!(
                            "named_handles list item must be an object, got {other}"
                        )))
                    }
                };

                let name = match obj.remove("name") {
                    Some(Value::String(s)) => s,
                    Some(_) => return Err(de::Error::custom("`name` must be a string")),
                    None => {
                        return Err(de::Error::custom(
                            "named_handles list item missing `name` field",
                        ))
                    }
                };

                let route: RouteHandleConfig =
                    serde_json::from_value(Value::Object(obj)).map_err(de::Error::custom)?;

                Ok((name, route))
            })
            .collect(),
        other => Err(de::Error::custom(format!(
            "named_handles must be an object or array, got {other}"
        ))),
    }
}

#[derive(Deserialize)]
struct PrefixedRouteRaw {
    aliases: Vec<String>,
    route: RouteHandleConfig,
}

fn deserialize_routes_field<'de, D>(deserializer: D) -> Result<Option<Vec<PrefixedRoute>>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    let entries: Result<Vec<PrefixedRoute>, D::Error> = match value {
        Value::Object(map) => map
            .into_iter()
            .map(|(prefix, val)| {
                let route = serde_json::from_value(val).map_err(de::Error::custom)?;
                Ok(PrefixedRoute {
                    aliases: vec![prefix],
                    route,
                })
            })
            .collect(),
        Value::Array(items) => items
            .into_iter()
            .map(|item| {
                let entry: PrefixedRouteRaw =
                    serde_json::from_value(item).map_err(de::Error::custom)?;
                Ok(PrefixedRoute {
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
    use crate::HashConfig;

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
        assert!(doc.pools.is_empty());
        assert!(doc.named_handles.is_empty());
        assert_eq!(doc.route, RouteEntry::Single(RouteHandleConfig::NullRoute));
    }

    #[test]
    fn pools_and_route_round_trip() {
        let doc =
            parse_ok(r#"{ "pools": { "foo": { "servers": ["a:1"] } }, "route": "PoolRoute|foo" }"#);
        assert_eq!(doc.pools.len(), 1);
        assert_eq!(doc.pools["foo"].servers[0].access_point(), "a:1");
        assert!(matches!(
            doc.route,
            RouteEntry::Single(RouteHandleConfig::PoolRoute { ref pool, .. })
                if pool == "foo"
        ));
    }

    #[test]
    fn named_handles_object_form_indexes_by_key() {
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
        assert_eq!(doc.named_handles.len(), 2);
        assert_eq!(
            doc.named_handles["route:A"],
            RouteHandleConfig::PoolRoute {
                pool: "A".into(),
                hash: HashConfig::default()
            }
        );
        assert!(matches!(
            doc.named_handles["route:B"],
            RouteHandleConfig::PoolRoute { ref pool, .. } if pool == "B"
        ));
    }

    #[test]
    fn named_handles_list_form_uses_name_field() {
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
        assert_eq!(doc.named_handles.len(), 2);
        assert_eq!(
            doc.named_handles["route:A"],
            RouteHandleConfig::PoolRoute {
                pool: "A".into(),
                hash: HashConfig::default()
            }
        );
        assert_eq!(doc.named_handles["n"], RouteHandleConfig::NullRoute);
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
            document.route,
            RouteEntry::Single(RouteHandleConfig::PoolRoute { ref pool, .. }) if pool == "A"
        ));
        assert!(matches!(
            document.named_handles["first"],
            RouteHandleConfig::PoolRoute { ref pool, .. } if pool == "A"
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
    }

    #[test]
    fn permits_unreferenced_empty_pools() {
        let document =
            parse_ok(r#"{ "pools": { "empty": { "servers": [] } }, "route": "NullRoute" }"#);
        assert!(document.pools["empty"].servers.is_empty());
    }
}
