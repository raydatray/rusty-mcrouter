use std::{collections::BTreeMap, fmt, str::FromStr};

use serde::de::{self, Deserialize, Deserializer, MapAccess, Visitor};
use serde_json::{value::RawValue, Map, Value};

use crate::PoolId;

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum RawRouteConfig {
    Reference(String),
    Shorthand {
        kind: String,
        args: Vec<String>,
    },
    PoolRoute {
        pool: String,
        hash: HashConfig,
    },
    FailoverRoute {
        children: Vec<RawRouteConfig>,
        failover_errors: FailoverErrorsConfig,
        failover_policy: FailoverPolicyConfig,
    },
    PrefixSelectorRoute {
        policies: BTreeMap<String, RawRouteConfig>,
        wildcard: Option<Box<RawRouteConfig>>,
    },
    NullRoute,
    ErrorRoute {
        message: Option<String>,
    },

    // extra routes we currently dont handle yet
    Unknown {
        kind: String,
        fields: Map<String, Value>,
    },
}

pub(crate) struct RawNamedHandle {
    pub(crate) name: String,
    pub(crate) route: RawRouteConfig,
}

type RawRouteFields = BTreeMap<String, Box<RawValue>>;

#[derive(Clone, Debug, PartialEq)]
pub enum RouteConfig {
    PoolRoute {
        pool: PoolId,
        hash: HashConfig,
    },
    FailoverRoute {
        children: Vec<RouteConfig>,
        failover_errors: FailoverErrorsConfig,
        failover_policy: FailoverPolicyConfig,
    },
    NullRoute,
    ErrorRoute {
        message: Option<String>,
    },
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum FailoverErrorsConfig {
    #[default]
    Default,
    All(Vec<FailoverErrorKind>),
    PerOp {
        gets: Option<Vec<FailoverErrorKind>>,
        updates: Option<Vec<FailoverErrorKind>>,
        deletes: Option<Vec<FailoverErrorKind>>,
    },
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum FailoverPolicyConfig {
    #[default]
    InOrder,
    LeastFailures {
        max_tries: usize,
    },
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum HashFunc {
    #[default]
    Ch3,
    Crc32,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HashConfig {
    pub func: HashFunc,
    pub salt: Option<String>,
}

/// Failover-eligible conditions and the `failover_errors` config vocabulary
/// (parsed alias-aware via [`FromStr`]). Mirrors the router's canonical
/// ResultCode names one-to-one — config stays net-independent, and core maps
/// this enum onto ResultCode at route-build time. mcrouter codes with no
/// rusty analogue (`busy`/`shutdown`) are rejected rather than silently
/// ignored.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FailoverErrorKind {
    Timeout,
    ConnectTimeout,
    ConnectError,
    RemoteError,
    ConnectionDropped,
    LocalError,
    Tko,
}

impl FromStr for FailoverErrorKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "timeout" => Ok(FailoverErrorKind::Timeout),
            "connect_timeout" => Ok(FailoverErrorKind::ConnectTimeout),
            "connect_error" => Ok(FailoverErrorKind::ConnectError),
            "remote_error" | "server_error" => Ok(FailoverErrorKind::RemoteError),
            "connection_dropped" => Ok(FailoverErrorKind::ConnectionDropped),
            "local_error" => Ok(FailoverErrorKind::LocalError),
            "tko" => Ok(FailoverErrorKind::Tko),
            other => Err(format!("unknown failover error `{other}`")),
        }
    }
}

impl<'de> Deserialize<'de> for RawRouteConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(RawRouteConfigVisitor)
    }
}

struct RawRouteConfigVisitor;

impl<'de> Visitor<'de> for RawRouteConfigVisitor {
    type Value = RawRouteConfig;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a route string or object")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(parse_string_form(value))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(parse_string_form(&value))
    }

    fn visit_map<M>(self, map: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let fields = collect_route_fields(map)?;
        parse_object_form(fields).map_err(de::Error::custom)
    }
}

impl<'de> Deserialize<'de> for RawNamedHandle {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct RawNamedHandleVisitor;

        impl<'de> Visitor<'de> for RawNamedHandleVisitor {
            type Value = RawNamedHandle;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a named handle object")
            }

            fn visit_map<M>(self, map: M) -> Result<Self::Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                let mut fields = collect_route_fields(map)?;
                let name = match take_json_field(&mut fields, "name").map_err(de::Error::custom)? {
                    Some(Value::String(name)) => name,
                    Some(_) => return Err(de::Error::custom("`name` must be a string")),
                    None => {
                        return Err(de::Error::custom(
                            "named_handles list item missing `name` field",
                        ))
                    }
                };
                let route = parse_object_form(fields).map_err(de::Error::custom)?;
                Ok(RawNamedHandle { name, route })
            }
        }

        deserializer.deserialize_map(RawNamedHandleVisitor)
    }
}

fn collect_route_fields<'de, M>(mut map: M) -> Result<RawRouteFields, M::Error>
where
    M: MapAccess<'de>,
{
    let mut fields = RawRouteFields::new();
    while let Some(key) = map.next_key::<String>()? {
        if fields.contains_key(&key) {
            return Err(de::Error::custom(format!("duplicate route field `{key}`")));
        }
        fields.insert(key, map.next_value()?);
    }
    Ok(fields)
}

fn parse_string_form(s: &str) -> RawRouteConfig {
    match s.split_once('|') {
        None => RawRouteConfig::Reference(s.to_string()),
        Some((kind, rest)) => RawRouteConfig::Shorthand {
            kind: kind.to_string(),
            args: rest.split('|').map(String::from).collect(),
        },
    }
}

fn parse_object_form(mut map: RawRouteFields) -> Result<RawRouteConfig, String> {
    let kind = match take_json_field(&mut map, "type")? {
        Some(Value::String(s)) => s,
        Some(other) => return Err(format!("`type` must be a string, got {}", other)),
        None => return Err("route object missing required field `type`".to_string()),
    };

    match kind.as_str() {
        "NullRoute" => Ok(RawRouteConfig::NullRoute),
        "ErrorRoute" => {
            let message = match take_json_field(&mut map, "message")? {
                Some(Value::String(s)) => Some(s),
                Some(other) => {
                    return Err(format!(
                        "ErrorRoute `message` must be a string, got {}",
                        other
                    ));
                }
                None => None,
            };
            Ok(RawRouteConfig::ErrorRoute { message })
        }
        "PoolRoute" => {
            let pool = match take_json_field(&mut map, "pool")? {
                Some(Value::String(s)) => s,
                Some(Value::Object(mut obj)) => match obj.remove("name") {
                    Some(Value::String(s)) => s,
                    _ => {
                        return Err(
                            "PoolRoute `pool` object form requires a string `name`".to_string()
                        );
                    }
                },
                Some(other) => {
                    return Err(format!(
                        "PoolRoute `pool` must be a string or object, got {}",
                        other
                    ));
                }
                None => return Err("PoolRoute missing required field `pool`".to_string()),
            };
            let hash = parse_hash(&mut map)?;
            Ok(RawRouteConfig::PoolRoute { pool, hash })
        }
        "FailoverRoute" => {
            if map.contains_key("failover_limit") {
                return Err("FailoverRoute `failover_limit` is not supported".to_string());
            }
            let children = parse_failover_children(&mut map)?;
            let failover_errors = parse_failover_errors(&mut map)?;
            let failover_policy = parse_failover_policy(&mut map)?;
            Ok(RawRouteConfig::FailoverRoute {
                children,
                failover_errors,
                failover_policy,
            })
        }
        "PrefixSelectorRoute" => parse_prefix_selector(&mut map),
        _ => Ok(RawRouteConfig::Unknown {
            kind,
            fields: into_json_fields(map)?,
        }),
    }
}

fn take_json_field(map: &mut RawRouteFields, name: &str) -> Result<Option<Value>, String> {
    map.remove(name)
        .map(|value| serde_json::from_str(value.get()).map_err(|error| error.to_string()))
        .transpose()
}

fn into_json_fields(map: RawRouteFields) -> Result<Map<String, Value>, String> {
    map.into_iter()
        .map(|(key, value)| {
            serde_json::from_str(value.get())
                .map(|value| (key, value))
                .map_err(|error| error.to_string())
        })
        .collect()
}

fn parse_hash(map: &mut RawRouteFields) -> Result<HashConfig, String> {
    match take_json_field(map, "hash")? {
        Some(Value::String(name)) => Ok(HashConfig {
            func: parse_hash_func(&name)?,
            salt: None,
        }),
        Some(Value::Object(mut obj)) => {
            let func = match obj.remove("hash_func") {
                None => HashFunc::default(),
                Some(Value::String(name)) => parse_hash_func(&name)?,
                Some(other) => return Err(format!("`hash_func` must be a string, got {}", other)),
            };
            let salt = match obj.remove("salt") {
                None => None,
                Some(Value::String(s)) => Some(s),
                Some(other) => return Err(format!("`salt` must be a string, got {}", other)),
            };
            Ok(HashConfig { func, salt })
        }
        Some(other) => Err(format!("`hash` must be a string or object, got {}", other)),
        None => Ok(HashConfig::default()),
    }
}

fn parse_hash_func(name: &str) -> Result<HashFunc, String> {
    match name {
        "Ch3" => Ok(HashFunc::Ch3),
        "Crc32" => Ok(HashFunc::Crc32),
        other => Err(format!("unknown hash_func `{}`", other)),
    }
}

fn parse_prefix_selector(map: &mut RawRouteFields) -> Result<RawRouteConfig, String> {
    let policies = map
        .remove("policies")
        .map(|raw| serde_json::from_str(raw.get()))
        .transpose()
        .map_err(|error| format!("invalid PrefixSelectorRoute policies: {error}"))?;

    let wildcard = map
        .remove("wildcard")
        .map(|raw| serde_json::from_str(raw.get()).map(Box::new))
        .transpose()
        .map_err(|error| format!("invalid PrefixSelectorRoute wildcard: {error}"))?;

    if policies.is_none() && wildcard.is_none() {
        return Err("PrefixSelectorRoute requires policies or wildcard".into());
    }

    Ok(RawRouteConfig::PrefixSelectorRoute {
        policies: policies.unwrap_or_default(),
        wildcard,
    })
}

fn parse_failover_children(map: &mut RawRouteFields) -> Result<Vec<RawRouteConfig>, String> {
    match map.remove("children") {
        Some(children) => serde_json::from_str(children.get())
            .map_err(|error| format!("invalid FailoverRoute `children`: {error}")),
        None => Err("FailoverRoute missing required field `children`".to_string()),
    }
}

fn parse_failover_errors(map: &mut RawRouteFields) -> Result<FailoverErrorsConfig, String> {
    match take_json_field(map, "failover_errors")? {
        None => Ok(FailoverErrorsConfig::Default),
        Some(Value::Array(names)) => Ok(FailoverErrorsConfig::All(parse_error_names(names)?)),
        Some(Value::Object(mut obj)) => Ok(FailoverErrorsConfig::PerOp {
            gets: parse_optional_error_names(&mut obj, "gets")?,
            updates: parse_optional_error_names(&mut obj, "updates")?,
            deletes: parse_optional_error_names(&mut obj, "deletes")?,
        }),
        Some(other) => Err(format!(
            "`failover_errors` must be an array or object, got {}",
            other
        )),
    }
}

fn parse_optional_error_names(
    obj: &mut Map<String, Value>,
    key: &str,
) -> Result<Option<Vec<FailoverErrorKind>>, String> {
    match obj.remove(key) {
        None => Ok(None),
        Some(Value::Array(names)) => Ok(Some(parse_error_names(names)?)),
        Some(other) => Err(format!(
            "`failover_errors.{}` must be an array, got {}",
            key, other
        )),
    }
}

fn parse_error_names(names: Vec<Value>) -> Result<Vec<FailoverErrorKind>, String> {
    names
        .into_iter()
        .map(|value| match value {
            Value::String(name) => name.parse::<FailoverErrorKind>(),
            other => Err(format!(
                "failover error name must be a string, got {}",
                other
            )),
        })
        .collect()
}

fn parse_failover_policy(map: &mut RawRouteFields) -> Result<FailoverPolicyConfig, String> {
    let mut obj = match take_json_field(map, "failover_policy")? {
        None => return Ok(FailoverPolicyConfig::InOrder),
        Some(Value::Object(obj)) => obj,
        Some(other) => {
            return Err(format!(
                "`failover_policy` must be an object, got {}",
                other
            ))
        }
    };
    let policy_type = match obj.remove("type") {
        Some(Value::String(s)) => s,
        Some(other) => {
            return Err(format!(
                "`failover_policy.type` must be a string, got {}",
                other
            ))
        }
        None => return Err("`failover_policy` object missing `type`".to_string()),
    };
    match policy_type.as_str() {
        "InOrderPolicy" => Ok(FailoverPolicyConfig::InOrder),
        "LeastFailuresPolicy" => {
            let max_tries = match obj.remove("max_tries") {
                Some(Value::Number(n)) => n
                    .as_u64()
                    .and_then(|v| usize::try_from(v).ok())
                    .ok_or_else(|| "`max_tries` must be a positive integer".to_string())?,
                Some(other) => {
                    return Err(format!("`max_tries` must be an integer, got {}", other))
                }
                None => return Err("LeastFailuresPolicy requires `max_tries`".to_string()),
            };
            if max_tries == 0 {
                return Err("`max_tries` must be a positive integer".to_string());
            }
            Ok(FailoverPolicyConfig::LeastFailures { max_tries })
        }
        other => Err(format!("unknown failover_policy type `{}`", other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn route_handle(json: &str) -> RawRouteConfig {
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn bare_string_with_no_pipe_is_a_reference() {
        assert_eq!(
            route_handle(r#""NullRoute""#),
            RawRouteConfig::Reference("NullRoute".into())
        );
        assert_eq!(
            route_handle(r#""route:A""#),
            RawRouteConfig::Reference("route:A".into())
        );
    }

    #[test]
    fn pipe_form_becomes_shorthand_with_args() {
        assert_eq!(
            route_handle(r#""PoolRoute|foo""#),
            RawRouteConfig::Shorthand {
                kind: "PoolRoute".into(),
                args: vec!["foo".into()]
            }
        );
    }

    #[test]
    fn multi_pipe_form_keeps_all_args() {
        assert_eq!(
            route_handle(r#""AllSyncRoute|Pool|A-foo""#),
            RawRouteConfig::Shorthand {
                kind: "AllSyncRoute".into(),
                args: vec!["Pool".into(), "A-foo".into()],
            }
        );
    }

    #[test]
    fn object_form_pool_route_defaults_hash_to_ch3() {
        let r = route_handle(r#"{ "type": "PoolRoute", "pool": "foo" }"#);
        assert_eq!(
            r,
            RawRouteConfig::PoolRoute {
                pool: "foo".into(),
                hash: HashConfig::default()
            }
        );
    }

    #[test]
    fn object_form_pool_route_with_object_pool_name() {
        let r =
            route_handle(r#"{ "type": "PoolRoute", "pool": { "name": "foo", "servers": [] } }"#);
        assert_eq!(
            r,
            RawRouteConfig::PoolRoute {
                pool: "foo".into(),
                hash: HashConfig::default()
            }
        );
    }

    #[test]
    fn object_form_pool_route_drops_extras_but_keeps_pool_and_hash() {
        let r = route_handle(r#"{ "type": "PoolRoute", "pool": "foo", "asynclog": "log_a" }"#);
        assert_eq!(
            r,
            RawRouteConfig::PoolRoute {
                pool: "foo".into(),
                hash: HashConfig::default()
            }
        );
    }

    #[test]
    fn pool_route_hash_string_form() {
        let r = route_handle(r#"{ "type": "PoolRoute", "pool": "A", "hash": "Crc32" }"#);
        assert_eq!(
            r,
            RawRouteConfig::PoolRoute {
                pool: "A".into(),
                hash: HashConfig {
                    func: HashFunc::Crc32,
                    salt: None
                }
            }
        );
    }

    #[test]
    fn pool_route_hash_object_form_with_salt() {
        let r = route_handle(
            r#"{ "type": "PoolRoute", "pool": "A", "hash": { "hash_func": "Crc32", "salt": "x" } }"#,
        );
        assert_eq!(
            r,
            RawRouteConfig::PoolRoute {
                pool: "A".into(),
                hash: HashConfig {
                    func: HashFunc::Crc32,
                    salt: Some("x".into())
                }
            }
        );
    }

    #[test]
    fn pool_route_hash_object_omitted_func_defaults_to_ch3() {
        let r = route_handle(r#"{ "type": "PoolRoute", "pool": "A", "hash": { "salt": "x" } }"#);
        assert_eq!(
            r,
            RawRouteConfig::PoolRoute {
                pool: "A".into(),
                hash: HashConfig {
                    func: HashFunc::Ch3,
                    salt: Some("x".into())
                }
            }
        );
    }

    #[test]
    fn pool_route_unknown_hash_func_is_error() {
        let err = serde_json::from_str::<RawRouteConfig>(
            r#"{ "type": "PoolRoute", "pool": "A", "hash": "Nope" }"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("Nope"), "got: {err}");
    }

    #[test]
    fn pool_route_non_string_hash_func_is_error() {
        let err = serde_json::from_str::<RawRouteConfig>(
            r#"{ "type": "PoolRoute", "pool": "A", "hash": { "hash_func": 123 } }"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("hash_func"), "got: {err}");
    }

    #[test]
    fn object_form_null_route() {
        assert_eq!(
            route_handle(r#"{ "type": "NullRoute" }"#),
            RawRouteConfig::NullRoute
        );
    }

    #[test]
    fn object_form_error_route_with_message() {
        let r = route_handle(r#"{ "type": "ErrorRoute", "message": "boom" }"#);
        assert_eq!(
            r,
            RawRouteConfig::ErrorRoute {
                message: Some("boom".into())
            }
        );
    }

    #[test]
    fn object_form_error_route_without_message() {
        let r = route_handle(r#"{ "type": "ErrorRoute" }"#);
        assert_eq!(r, RawRouteConfig::ErrorRoute { message: None });
    }

    #[test]
    fn object_form_prefix_selector_route() {
        let r = route_handle(
            r#"{ "type": "PrefixSelectorRoute", "policies": { "good": "PoolRoute|A" }, "wildcard": "PoolRoute|B" }"#,
        );
        match r {
            RawRouteConfig::PrefixSelectorRoute { policies, wildcard } => {
                assert!(matches!(
                    policies.get("good"),
                    Some(RawRouteConfig::Shorthand { kind, args })
                        if kind == "PoolRoute" && args == &["A"]
                ));
                assert!(matches!(
                    wildcard.as_deref(),
                    Some(RawRouteConfig::Shorthand { kind, args })
                        if kind == "PoolRoute" && args == &["B"]
                ));
            }
            other => panic!("expected PrefixSelectorRoute, got {other:?}"),
        }
    }

    #[test]
    fn prefix_selector_accepts_explicit_empty_policies() {
        assert_eq!(
            route_handle(r#"{ "type": "PrefixSelectorRoute", "policies": {} }"#),
            RawRouteConfig::PrefixSelectorRoute {
                policies: BTreeMap::new(),
                wildcard: None,
            }
        );
    }

    #[test]
    fn unknown_object_type_preserves_kind_and_all_fields() {
        let r = route_handle(r#"{ "type": "StillUnknownRoute", "field": 1 }"#);
        match r {
            RawRouteConfig::Unknown { kind, fields } => {
                assert_eq!(kind, "StillUnknownRoute");
                assert_eq!(fields.get("field"), Some(&Value::from(1)));
                assert!(!fields.contains_key("type"), "type should be consumed");
            }
            other => panic!("expected Unknown, got {other:?}"),
        }
    }

    #[test]
    fn rejects_object_without_type() {
        let err = serde_json::from_str::<RawRouteConfig>(r#"{ "pool": "A" }"#).unwrap_err();
        assert!(err.to_string().contains("type"), "got: {err}");
    }

    #[test]
    fn rejects_duplicate_route_fields() {
        for json in [
            r#"{ "type": "PoolRoute", "type": "NullRoute", "pool": "A" }"#,
            r#"{ "type": "PoolRoute", "pool": "A", "pool": "B" }"#,
            r#"{ "type": "PoolRoute", "pool": "A", "hash": "Ch3", "hash": "Crc32" }"#,
            r#"{
                "type": "FailoverRoute",
                "children": [{
                    "type": "PoolRoute",
                    "pool": "A",
                    "pool": "B"
                }]
            }"#,
        ] {
            let error = serde_json::from_str::<RawRouteConfig>(json).unwrap_err();
            assert!(error.to_string().contains("duplicate route field"));
        }
    }

    #[test]
    fn rejects_pool_route_without_pool() {
        let err = serde_json::from_str::<RawRouteConfig>(r#"{ "type": "PoolRoute" }"#).unwrap_err();
        assert!(err.to_string().contains("pool"), "got: {err}");
    }

    #[test]
    fn rejects_non_string_non_object_root() {
        assert!(serde_json::from_str::<RawRouteConfig>("42").is_err());
        assert!(serde_json::from_str::<RawRouteConfig>("[]").is_err());
        assert!(serde_json::from_str::<RawRouteConfig>("true").is_err());
    }

    #[test]
    fn failover_error_kind_parses_canonical_names() {
        let table = [
            ("timeout", FailoverErrorKind::Timeout),
            ("connect_timeout", FailoverErrorKind::ConnectTimeout),
            ("connect_error", FailoverErrorKind::ConnectError),
            ("remote_error", FailoverErrorKind::RemoteError),
            ("connection_dropped", FailoverErrorKind::ConnectionDropped),
            ("local_error", FailoverErrorKind::LocalError),
            ("tko", FailoverErrorKind::Tko),
        ];
        for (name, expected) in table {
            assert_eq!(name.parse::<FailoverErrorKind>(), Ok(expected), "{name}");
        }
    }

    #[test]
    fn failover_error_kind_accepts_aliases() {
        assert_eq!(
            "server_error".parse::<FailoverErrorKind>(),
            Ok(FailoverErrorKind::RemoteError)
        );
    }

    #[test]
    fn failover_error_kind_rejects_unknown_names() {
        assert!("busy".parse::<FailoverErrorKind>().is_err());
        assert!("shutdown".parse::<FailoverErrorKind>().is_err());
        assert!("io_error".parse::<FailoverErrorKind>().is_err());
        assert!("".parse::<FailoverErrorKind>().is_err());
    }

    #[test]
    fn failover_route_parses_children_and_defaults() {
        let r = route_handle(
            r#"{ "type": "FailoverRoute", "children": ["PoolRoute|A", "PoolRoute|B"] }"#,
        );
        let RawRouteConfig::FailoverRoute {
            children,
            failover_errors,
            failover_policy,
        } = r
        else {
            panic!("expected FailoverRoute, got {r:?}");
        };
        assert_eq!(children.len(), 2);
        assert_eq!(failover_errors, FailoverErrorsConfig::Default);
        assert_eq!(failover_policy, FailoverPolicyConfig::InOrder);
    }

    #[test]
    fn failover_route_rejects_unsupported_rate_limiter() {
        let error = serde_json::from_str::<RawRouteConfig>(
            r#"{
                "type": "FailoverRoute",
                "children": ["PoolRoute|A"],
                "failover_limit": { "rate": 0.2, "burst": 9.8 }
            }"#,
        )
        .unwrap_err();

        assert!(error.to_string().contains("failover_limit"));
    }

    #[test]
    fn failover_route_missing_children_is_error() {
        let err =
            serde_json::from_str::<RawRouteConfig>(r#"{ "type": "FailoverRoute" }"#).unwrap_err();
        assert!(err.to_string().contains("children"), "got: {err}");
    }

    #[test]
    fn failover_route_non_array_children_is_error() {
        let err = serde_json::from_str::<RawRouteConfig>(
            r#"{ "type": "FailoverRoute", "children": "nope" }"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("children"), "got: {err}");
    }

    #[test]
    fn failover_route_nests() {
        let r = route_handle(
            r#"{ "type": "FailoverRoute", "children": [ { "type": "FailoverRoute", "children": ["PoolRoute|A"] }, "PoolRoute|B" ] }"#,
        );
        let RawRouteConfig::FailoverRoute { children, .. } = r else {
            panic!("expected FailoverRoute");
        };
        assert!(matches!(
            children.first(),
            Some(RawRouteConfig::FailoverRoute { .. })
        ));
    }

    #[test]
    fn failover_errors_array_form() {
        let r = route_handle(
            r#"{ "type": "FailoverRoute", "children": ["PoolRoute|A"], "failover_errors": ["timeout", "server_error"] }"#,
        );
        let RawRouteConfig::FailoverRoute {
            failover_errors, ..
        } = r
        else {
            panic!("expected FailoverRoute");
        };
        assert_eq!(
            failover_errors,
            FailoverErrorsConfig::All(vec![
                FailoverErrorKind::Timeout,
                FailoverErrorKind::RemoteError
            ])
        );
    }

    #[test]
    fn failover_errors_object_form_with_missing_keys() {
        let r = route_handle(
            r#"{ "type": "FailoverRoute", "children": ["PoolRoute|A"], "failover_errors": { "updates": [] } }"#,
        );
        let RawRouteConfig::FailoverRoute {
            failover_errors, ..
        } = r
        else {
            panic!("expected FailoverRoute");
        };
        assert_eq!(
            failover_errors,
            FailoverErrorsConfig::PerOp {
                gets: None,
                updates: Some(vec![]),
                deletes: None,
            }
        );
    }

    #[test]
    fn failover_errors_unknown_name_is_error() {
        let err = serde_json::from_str::<RawRouteConfig>(
            r#"{ "type": "FailoverRoute", "children": ["PoolRoute|A"], "failover_errors": ["busy"] }"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("busy"), "got: {err}");
    }

    #[test]
    fn failover_policy_least_failures() {
        let r = route_handle(
            r#"{ "type": "FailoverRoute", "children": ["PoolRoute|A"], "failover_policy": { "type": "LeastFailuresPolicy", "max_tries": 3 } }"#,
        );
        let RawRouteConfig::FailoverRoute {
            failover_policy, ..
        } = r
        else {
            panic!("expected FailoverRoute");
        };
        assert_eq!(
            failover_policy,
            FailoverPolicyConfig::LeastFailures { max_tries: 3 }
        );
    }

    #[test]
    fn failover_policy_least_failures_requires_max_tries() {
        let err = serde_json::from_str::<RawRouteConfig>(
            r#"{ "type": "FailoverRoute", "children": ["PoolRoute|A"], "failover_policy": { "type": "LeastFailuresPolicy" } }"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("max_tries"), "got: {err}");
    }

    #[test]
    fn failover_policy_least_failures_rejects_zero_max_tries() {
        let err = serde_json::from_str::<RawRouteConfig>(
            r#"{ "type": "FailoverRoute", "children": ["PoolRoute|A"], "failover_policy": { "type": "LeastFailuresPolicy", "max_tries": 0 } }"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("positive"), "got: {err}");
    }

    #[test]
    fn failover_policy_unknown_type_is_error() {
        let err = serde_json::from_str::<RawRouteConfig>(
            r#"{ "type": "FailoverRoute", "children": ["PoolRoute|A"], "failover_policy": { "type": "Nope" } }"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("Nope"), "got: {err}");
    }
}
