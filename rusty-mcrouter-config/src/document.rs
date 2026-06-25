use std::collections::BTreeMap;

use serde::de::{self, Deserializer};
use serde::Deserialize;
use serde_json::Value;

use crate::ConfigError;
use crate::{pool::PoolConfig, route::RouteHandleConfig};

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
struct ConfigDocumentRaw {
    #[serde(default)]
    pools: BTreeMap<String, PoolConfig>,

    #[serde(default, deserialize_with = "deserialize_named_handles")]
    named_handles: BTreeMap<String, RouteHandleConfig>,

    #[serde(default)]
    route: Option<RouteHandleConfig>,

    #[serde(default, deserialize_with = "deserialize_routes_field")]
    routes: Option<Vec<PrefixedRoute>>,
}

impl ConfigDocument {
    pub(crate) fn from_value(value: Value) -> crate::Result<Self> {
        let raw = serde_json::from_value::<ConfigDocumentRaw>(value)?;

        let route = match (raw.route, raw.routes) {
            (Some(_), Some(_)) => return Err(ConfigError::BothRouteAndRoutes),
            (None, None) => return Err(ConfigError::MissingRoute),
            (Some(single), None) => RouteEntry::Single(single),
            (None, Some(prefixed)) => RouteEntry::Prefixed(prefixed),
        };

        Ok(ConfigDocument {
            pools: raw.pools,
            named_handles: raw.named_handles,
            route,
        })
    }
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
    use crate::route::HashConfig;

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
        assert_eq!(
            doc.route,
            RouteEntry::Single(RouteHandleConfig::Reference("NullRoute".into()))
        );
    }

    #[test]
    fn pools_and_route_round_trip() {
        let doc = parse_ok(
            r#"{ "pools": { "foo": { "servers": ["a:1"] } }, "route": "PoolRoute|foo" }"#,
        );
        assert_eq!(doc.pools.len(), 1);
        assert_eq!(doc.pools["foo"].servers, vec!["a:1"]);
        assert!(matches!(
            doc.route,
            RouteEntry::Single(RouteHandleConfig::Shorthand { ref kind, ref args })
                if kind == "PoolRoute" && args == &["foo".to_string()]
        ));
    }

    #[test]
    fn named_handles_object_form_indexes_by_key() {
        let doc = parse_ok(
            r#"{
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
            RouteHandleConfig::Shorthand { .. }
        ));
    }

    #[test]
    fn named_handles_list_form_uses_name_field() {
        let doc = parse_ok(
            r#"{
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
    fn routes_plural_array_form_preserves_aliases() {
        let doc = parse_ok(
            r#"{
                "pools": { "A": { "servers": ["x:1"] }, "B": { "servers": ["y:1"] } },
                "routes": [
                    { "aliases": ["/a/a/"], "route": "PoolRoute|A" },
                    { "aliases": ["/b/b/"], "route": "PoolRoute|B" }
                ]
            }"#,
        );
        let RouteEntry::Prefixed(entries) = doc.route else {
            panic!("expected Prefixed");
        };
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].aliases, vec!["/a/a/".to_string()]);
        assert_eq!(entries[1].aliases, vec!["/b/b/".to_string()]);
    }

    #[test]
    fn routes_plural_object_form_lifts_keys_to_aliases() {
        let doc = parse_ok(
            r#"{
                "pools": { "A": { "servers": ["x:1"] } },
                "routes": { "/foo/bar/": "PoolRoute|A" }
            }"#,
        );
        let RouteEntry::Prefixed(entries) = doc.route else {
            panic!("expected Prefixed");
        };
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].aliases, vec!["/foo/bar/".to_string()]);
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
}
