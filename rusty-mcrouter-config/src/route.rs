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
            Ok(RouteHandleConfig::PoolRoute { pool })
        }
        _ => Ok(RouteHandleConfig::Unknown { kind, fields: map }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(json: &str) -> RouteHandleConfig {
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn bare_string_with_no_pipe_is_a_reference() {
        assert_eq!(
            parse(r#""NullRoute""#),
            RouteHandleConfig::Reference("NullRoute".into())
        );
        assert_eq!(
            parse(r#""route:A""#),
            RouteHandleConfig::Reference("route:A".into())
        );
    }

    #[test]
    fn pipe_form_becomes_shorthand_with_args() {
        assert_eq!(
            parse(r#""PoolRoute|foo""#),
            RouteHandleConfig::Shorthand {
                kind: "PoolRoute".into(),
                args: vec!["foo".into()]
            }
        );
    }

    #[test]
    fn multi_pipe_form_keeps_all_args() {
        assert_eq!(
            parse(r#""AllSyncRoute|Pool|A-foo""#),
            RouteHandleConfig::Shorthand {
                kind: "AllSyncRoute".into(),
                args: vec!["Pool".into(), "A-foo".into()],
            }
        );
    }

    #[test]
    fn object_form_pool_route() {
        let r = parse(r#"{ "type": "PoolRoute", "pool": "foo" }"#);
        assert_eq!(r, RouteHandleConfig::PoolRoute { pool: "foo".into() });
    }

    #[test]
    fn object_form_pool_route_with_object_pool_name() {
        let r = parse(r#"{ "type": "PoolRoute", "pool": { "name": "foo", "servers": [] } }"#);
        assert_eq!(r, RouteHandleConfig::PoolRoute { pool: "foo".into() });
    }

    #[test]
    fn object_form_pool_route_silently_drops_extras() {
        let r = parse(r#"{ "type": "PoolRoute", "pool": "foo", "asynclog": "log_a" }"#);
        assert_eq!(r, RouteHandleConfig::PoolRoute { pool: "foo".into() });
    }

    #[test]
    fn object_form_null_route() {
        assert_eq!(
            parse(r#"{ "type": "NullRoute" }"#),
            RouteHandleConfig::NullRoute
        );
    }

    #[test]
    fn object_form_error_route_with_message() {
        let r = parse(r#"{ "type": "ErrorRoute", "message": "boom" }"#);
        assert_eq!(
            r,
            RouteHandleConfig::ErrorRoute {
                message: Some("boom".into())
            }
        );
    }

    #[test]
    fn object_form_error_route_without_message() {
        let r = parse(r#"{ "type": "ErrorRoute" }"#);
        assert_eq!(r, RouteHandleConfig::ErrorRoute { message: None });
    }

    #[test]
    fn unknown_object_type_preserves_kind_and_all_fields() {
        let r = parse(
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
}
