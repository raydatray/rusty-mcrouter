use std::str::FromStr;

use serde::de::{self, Deserialize, Deserializer};
use serde_json::{Map, Value};

#[derive(Clone, Debug, PartialEq)]
pub enum RouteHandleConfig {
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
        children: Vec<RouteHandleConfig>,
        failover_errors: FailoverErrorsConfig,
        failover_policy: FailoverPolicyConfig,
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

/// Failover-eligible conditions and the `failover_errors` config vocabulary (parsed
/// alias-aware via [`FromStr`]). Covers only what rusty can observe today (transport
/// errors + a backend `SERVER_ERROR`); mcrouter codes with no rusty analogue
/// (`busy`/`tko`/`shutdown`) are rejected rather than silently ignored.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FailoverErrorKind {
    Timeout,
    Io,
    Protocol,
    ClientClosed,
    ServerError,
}

impl FromStr for FailoverErrorKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "timeout" => Ok(FailoverErrorKind::Timeout),
            "connect_error" | "io_error" => Ok(FailoverErrorKind::Io),
            "protocol_error" => Ok(FailoverErrorKind::Protocol),
            "client_closed" => Ok(FailoverErrorKind::ClientClosed),
            "server_error" | "remote_error" => Ok(FailoverErrorKind::ServerError),
            other => Err(format!("unknown failover error `{other}`")),
        }
    }
}

impl<'de> Deserialize<'de> for RouteHandleConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        match value {
            Value::String(s) => Ok(parse_string_form(&s)),
            Value::Object(map) => parse_object_form(map).map_err(de::Error::custom),
            other => Err(de::Error::custom(format!(
                "route handle must be a string or obejct, got {}",
                other
            ))),
        }
    }
}

fn parse_string_form(s: &str) -> RouteHandleConfig {
    match s.split_once('|') {
        None => RouteHandleConfig::Reference(s.to_string()),
        Some((kind, rest)) => RouteHandleConfig::Shorthand {
            kind: kind.to_string(),
            args: rest.split('|').map(String::from).collect(),
        },
    }
}

fn parse_object_form(mut map: Map<String, Value>) -> Result<RouteHandleConfig, String> {
    let kind = match map.remove("type") {
        Some(Value::String(s)) => s,
        Some(other) => return Err(format!("`type` must be a string, got {}", other)),
        None => return Err("route object missing required field `type`".to_string()),
    };

    match kind.as_str() {
        "NullRoute" => Ok(RouteHandleConfig::NullRoute),
        "ErrorRoute" => {
            let message = match map.remove("message") {
                Some(Value::String(s)) => Some(s),
                Some(other) => {
                    return Err(format!(
                        "ErrorRoute `message` must be a string, got {}",
                        other
                    ));
                }
                None => None,
            };
            Ok(RouteHandleConfig::ErrorRoute { message })
        }
        "PoolRoute" => {
            let pool = match map.remove("pool") {
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
            Ok(RouteHandleConfig::PoolRoute { pool, hash })
        }
        "FailoverRoute" => {
            let children = parse_failover_children(&mut map)?;
            let failover_errors = parse_failover_errors(&mut map)?;
            let failover_policy = parse_failover_policy(&mut map)?;
            Ok(RouteHandleConfig::FailoverRoute {
                children,
                failover_errors,
                failover_policy,
            })
        }
        _ => Ok(RouteHandleConfig::Unknown { kind, fields: map }),
    }
}

fn parse_hash(map: &mut Map<String, Value>) -> Result<HashConfig, String> {
    match map.remove("hash") {
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

fn parse_failover_children(map: &mut Map<String, Value>) -> Result<Vec<RouteHandleConfig>, String> {
    match map.remove("children") {
        Some(Value::Array(items)) => items
            .into_iter()
            .map(|item| serde_json::from_value(item).map_err(|e| e.to_string()))
            .collect(),
        Some(other) => Err(format!(
            "FailoverRoute `children` must be an array, got {}",
            other
        )),
        None => Err("FailoverRoute missing required field `children`".to_string()),
    }
}

fn parse_failover_errors(map: &mut Map<String, Value>) -> Result<FailoverErrorsConfig, String> {
    match map.remove("failover_errors") {
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
            other => Err(format!("failover error name must be a string, got {}", other)),
        })
        .collect()
}

fn parse_failover_policy(map: &mut Map<String, Value>) -> Result<FailoverPolicyConfig, String> {
    let mut obj = match map.remove("failover_policy") {
        None => return Ok(FailoverPolicyConfig::InOrder),
        Some(Value::Object(obj)) => obj,
        Some(other) => return Err(format!("`failover_policy` must be an object, got {}", other)),
    };
    let policy_type = match obj.remove("type") {
        Some(Value::String(s)) => s,
        Some(other) => {
            return Err(format!("`failover_policy.type` must be a string, got {}", other))
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
                    .ok_or_else(|| "`max_tries` must be a non-negative integer".to_string())?,
                Some(other) => {
                    return Err(format!("`max_tries` must be an integer, got {}", other))
                }
                None => return Err("LeastFailuresPolicy requires `max_tries`".to_string()),
            };
            Ok(FailoverPolicyConfig::LeastFailures { max_tries })
        }
        other => Err(format!("unknown failover_policy type `{}`", other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn route_handle(json: &str) -> RouteHandleConfig {
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn bare_string_with_no_pipe_is_a_reference() {
        assert_eq!(
            route_handle(r#""NullRoute""#),
            RouteHandleConfig::Reference("NullRoute".into())
        );
        assert_eq!(
            route_handle(r#""route:A""#),
            RouteHandleConfig::Reference("route:A".into())
        );
    }

    #[test]
    fn pipe_form_becomes_shorthand_with_args() {
        assert_eq!(
            route_handle(r#""PoolRoute|foo""#),
            RouteHandleConfig::Shorthand {
                kind: "PoolRoute".into(),
                args: vec!["foo".into()]
            }
        );
    }

    #[test]
    fn multi_pipe_form_keeps_all_args() {
        assert_eq!(
            route_handle(r#""AllSyncRoute|Pool|A-foo""#),
            RouteHandleConfig::Shorthand {
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
            RouteHandleConfig::PoolRoute {
                pool: "foo".into(),
                hash: HashConfig::default()
            }
        );
    }

    #[test]
    fn object_form_pool_route_with_object_pool_name() {
        let r = route_handle(r#"{ "type": "PoolRoute", "pool": { "name": "foo", "servers": [] } }"#);
        assert_eq!(
            r,
            RouteHandleConfig::PoolRoute {
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
            RouteHandleConfig::PoolRoute {
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
            RouteHandleConfig::PoolRoute {
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
            RouteHandleConfig::PoolRoute {
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
            RouteHandleConfig::PoolRoute {
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
        let err = serde_json::from_str::<RouteHandleConfig>(
            r#"{ "type": "PoolRoute", "pool": "A", "hash": "Nope" }"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("Nope"), "got: {err}");
    }

    #[test]
    fn pool_route_non_string_hash_func_is_error() {
        let err = serde_json::from_str::<RouteHandleConfig>(
            r#"{ "type": "PoolRoute", "pool": "A", "hash": { "hash_func": 123 } }"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("hash_func"), "got: {err}");
    }

    #[test]
    fn object_form_null_route() {
        assert_eq!(
            route_handle(r#"{ "type": "NullRoute" }"#),
            RouteHandleConfig::NullRoute
        );
    }

    #[test]
    fn object_form_error_route_with_message() {
        let r = route_handle(r#"{ "type": "ErrorRoute", "message": "boom" }"#);
        assert_eq!(
            r,
            RouteHandleConfig::ErrorRoute {
                message: Some("boom".into())
            }
        );
    }

    #[test]
    fn object_form_error_route_without_message() {
        let r = route_handle(r#"{ "type": "ErrorRoute" }"#);
        assert_eq!(r, RouteHandleConfig::ErrorRoute { message: None });
    }

    #[test]
    fn unknown_object_type_preserves_kind_and_all_fields() {
        let r = route_handle(
            r#"{ "type": "PrefixSelectorRoute", "policies": { "good": "PoolRoute|A" }, "wildcard": "PoolRoute|B" }"#,
        );
        match r {
            RouteHandleConfig::Unknown { kind, fields } => {
                assert_eq!(kind, "PrefixSelectorRoute");
                assert!(fields.contains_key("policies"));
                assert!(fields.contains_key("wildcard"));
                assert!(!fields.contains_key("type"), "type should be consumed");
            }
            other => panic!("expected Unknown, got {other:?}"),
        }
    }

    #[test]
    fn rejects_object_without_type() {
        let err = serde_json::from_str::<RouteHandleConfig>(r#"{ "pool": "A" }"#).unwrap_err();
        assert!(err.to_string().contains("type"), "got: {err}");
    }

    #[test]
    fn rejects_pool_route_without_pool() {
        let err =
            serde_json::from_str::<RouteHandleConfig>(r#"{ "type": "PoolRoute" }"#).unwrap_err();
        assert!(err.to_string().contains("pool"), "got: {err}");
    }

    #[test]
    fn rejects_non_string_non_object_root() {
        assert!(serde_json::from_str::<RouteHandleConfig>("42").is_err());
        assert!(serde_json::from_str::<RouteHandleConfig>("[]").is_err());
        assert!(serde_json::from_str::<RouteHandleConfig>("true").is_err());
    }

    #[test]
    fn failover_error_kind_parses_canonical_names() {
        assert_eq!("timeout".parse::<FailoverErrorKind>(), Ok(FailoverErrorKind::Timeout));
        assert_eq!(
            "protocol_error".parse::<FailoverErrorKind>(),
            Ok(FailoverErrorKind::Protocol)
        );
        assert_eq!(
            "client_closed".parse::<FailoverErrorKind>(),
            Ok(FailoverErrorKind::ClientClosed)
        );
    }

    #[test]
    fn failover_error_kind_accepts_aliases() {
        assert_eq!("connect_error".parse::<FailoverErrorKind>(), Ok(FailoverErrorKind::Io));
        assert_eq!("io_error".parse::<FailoverErrorKind>(), Ok(FailoverErrorKind::Io));
        assert_eq!(
            "server_error".parse::<FailoverErrorKind>(),
            Ok(FailoverErrorKind::ServerError)
        );
        assert_eq!(
            "remote_error".parse::<FailoverErrorKind>(),
            Ok(FailoverErrorKind::ServerError)
        );
    }

    #[test]
    fn failover_error_kind_rejects_unknown_names() {
        assert!("tko".parse::<FailoverErrorKind>().is_err());
        assert!("busy".parse::<FailoverErrorKind>().is_err());
        assert!("".parse::<FailoverErrorKind>().is_err());
    }

    #[test]
    fn failover_route_parses_children_and_defaults() {
        let r = route_handle(r#"{ "type": "FailoverRoute", "children": ["PoolRoute|A", "PoolRoute|B"] }"#);
        let RouteHandleConfig::FailoverRoute {
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
    fn failover_route_missing_children_is_error() {
        let err = serde_json::from_str::<RouteHandleConfig>(r#"{ "type": "FailoverRoute" }"#)
            .unwrap_err();
        assert!(err.to_string().contains("children"), "got: {err}");
    }

    #[test]
    fn failover_route_non_array_children_is_error() {
        let err = serde_json::from_str::<RouteHandleConfig>(
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
        let RouteHandleConfig::FailoverRoute { children, .. } = r else {
            panic!("expected FailoverRoute");
        };
        assert!(matches!(
            children.first(),
            Some(RouteHandleConfig::FailoverRoute { .. })
        ));
    }

    #[test]
    fn failover_errors_array_form() {
        let r = route_handle(
            r#"{ "type": "FailoverRoute", "children": ["PoolRoute|A"], "failover_errors": ["timeout", "server_error"] }"#,
        );
        let RouteHandleConfig::FailoverRoute { failover_errors, .. } = r else {
            panic!("expected FailoverRoute");
        };
        assert_eq!(
            failover_errors,
            FailoverErrorsConfig::All(vec![
                FailoverErrorKind::Timeout,
                FailoverErrorKind::ServerError
            ])
        );
    }

    #[test]
    fn failover_errors_object_form_with_missing_keys() {
        let r = route_handle(
            r#"{ "type": "FailoverRoute", "children": ["PoolRoute|A"], "failover_errors": { "updates": [] } }"#,
        );
        let RouteHandleConfig::FailoverRoute { failover_errors, .. } = r else {
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
        let err = serde_json::from_str::<RouteHandleConfig>(
            r#"{ "type": "FailoverRoute", "children": ["PoolRoute|A"], "failover_errors": ["tko"] }"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("tko"), "got: {err}");
    }

    #[test]
    fn failover_policy_least_failures() {
        let r = route_handle(
            r#"{ "type": "FailoverRoute", "children": ["PoolRoute|A"], "failover_policy": { "type": "LeastFailuresPolicy", "max_tries": 3 } }"#,
        );
        let RouteHandleConfig::FailoverRoute { failover_policy, .. } = r else {
            panic!("expected FailoverRoute");
        };
        assert_eq!(
            failover_policy,
            FailoverPolicyConfig::LeastFailures { max_tries: 3 }
        );
    }

    #[test]
    fn failover_policy_least_failures_requires_max_tries() {
        let err = serde_json::from_str::<RouteHandleConfig>(
            r#"{ "type": "FailoverRoute", "children": ["PoolRoute|A"], "failover_policy": { "type": "LeastFailuresPolicy" } }"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("max_tries"), "got: {err}");
    }

    #[test]
    fn failover_policy_unknown_type_is_error() {
        let err = serde_json::from_str::<RouteHandleConfig>(
            r#"{ "type": "FailoverRoute", "children": ["PoolRoute|A"], "failover_policy": { "type": "Nope" } }"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("Nope"), "got: {err}");
    }
}
