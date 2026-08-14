use serde::Deserialize;
use serde_json::{Map, Value};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct PoolConfig {
    pub servers: Vec<String>,

    /// Per-pool reply timeout, ms (mcrouter pool key `server_timeout`).
    #[serde(default, rename = "server_timeout")]
    pub server_timeout_ms: Option<u64>,

    /// Per-pool connect timeout, ms. When absent it follows the (possibly
    /// pool-overridden) server timeout — the derivation lives in core.
    #[serde(default, rename = "connect_timeout")]
    pub connect_timeout_ms: Option<u64>,

    /// Optional per-pool fail-open gate on TKO marking.
    #[serde(default)]
    pub tko_tracker: Option<PoolTkoTrackerConfig>,

    // extra fields we dont use yet
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// mcrouter's pool `tko_tracker` block: enter/exit thresholds for the
/// fail-open gate, each expressible as an absolute count or a percentage of
/// the pool's servers. Resolution (num takes precedence over percent) and
/// validation live in core's route builder.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct PoolTkoTrackerConfig {
    #[serde(default)]
    pub num_tko_threshold_upper: Option<u64>,
    #[serde(default)]
    pub percent_tko_threshold_upper: Option<u64>,
    #[serde(default)]
    pub num_tko_threshold_lower: Option<u64>,
    #[serde(default)]
    pub percent_tko_threshold_lower: Option<u64>,
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
    fn parse_pool_timeouts_with_mcrouter_key_names() {
        let pool = pool(
            r#"{
                       "servers": ["a:1"],
                       "server_timeout": 200,
                       "connect_timeout": 150
                   }"#,
        );

        assert_eq!(pool.server_timeout_ms, Some(200));
        assert_eq!(pool.connect_timeout_ms, Some(150));
    }

    #[test]
    fn timeouts_default_to_absent() {
        let pool = pool(r#"{ "servers": ["a:1"] }"#);
        assert_eq!(pool.server_timeout_ms, None);
        assert_eq!(pool.connect_timeout_ms, None);
        assert_eq!(pool.tko_tracker, None);
        // and they no longer leak into extra
        assert!(pool.extra.is_empty());
    }

    #[test]
    fn parse_tko_tracker_block() {
        let pool = pool(
            r#"{
                       "servers": ["a:1", "b:2", "c:3"],
                       "tko_tracker": {
                           "num_tko_threshold_upper": 2,
                           "percent_tko_threshold_lower": 33
                       }
                   }"#,
        );

        let tko = pool.tko_tracker.unwrap();
        assert_eq!(tko.num_tko_threshold_upper, Some(2));
        assert_eq!(tko.percent_tko_threshold_upper, None);
        assert_eq!(tko.num_tko_threshold_lower, None);
        assert_eq!(tko.percent_tko_threshold_lower, Some(33));
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
