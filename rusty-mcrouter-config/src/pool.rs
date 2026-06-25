use serde::Deserialize;
use serde_json::{Map, Value};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct PoolConfig {
    pub servers: Vec<String>,

    // extra fields we dont use yet
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pool(json: &str) -> PoolConfig {
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn parse_pool_with_one_server() {
        let pool = pool(r#"{ "servers": ["localhost:11211"] }"#);

        assert_eq!(pool.servers, vec!["localhost:11211".to_string()]);
        assert!(pool.extra.is_empty())
    }

    #[test]
    fn parse_pool_with_no_servers() {
        let pool = pool(r#"{ "servers": []}"#);

        assert!(pool.servers.is_empty());
        assert!(pool.extra.is_empty())
    }

    #[test]
    fn parse_unknown_fields_into_extra() {
        let pool = pool(
            r#"{
                       "servers": ["a:1", "b:2"],
                       "protocol": "ascii",
                       "region": "us-west",
                       "enable_compression": true
                   }"#,
        );

        assert_eq!(pool.servers.len(), 2);
        assert_eq!(pool.extra.get("protocol").unwrap(), "ascii");
        assert_eq!(pool.extra.get("region").unwrap(), "us-west");
        assert_eq!(
            pool.extra.get("enable_compression").unwrap(),
            &Value::Bool(true)
        );
    }

    #[test]
    fn rejects_missing_servers_field() {
        let json = r#"{ "protocol": "ascii" }"#;
        let err = serde_json::from_str::<PoolConfig>(json).unwrap_err();

        assert!(err.to_string().contains("servers"), "got: {err}");
    }

    #[test]
    fn rejects_servers_as_non_array() {
        let json = r#"{ "servers": "localhost:1" }"#;

        assert!(serde_json::from_str::<PoolConfig>(json).is_err());
    }
}
