use serde::Deserialize;
use serde_json::{Map, Value};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PoolConfig {
    pub servers: Vec<String>,
    pub server_timeout_ms: Option<u64>,
    pub connect_timeout_ms: Option<u64>,
    pub tko_tracker: Option<PoolTkoTrackerConfig>,
    pub extra: Map<String, Value>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct RawPoolConfig {
    pub servers: Vec<String>,
    #[serde(default, rename = "server_timeout")]
    server_timeout_ms: Option<u64>,
    #[serde(default, rename = "connect_timeout")]
    connect_timeout_ms: Option<u64>,
    #[serde(default)]
    tko_tracker: Option<RawPoolTkoTrackerConfig>,
    #[serde(flatten)]
    extra: Map<String, Value>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PoolTkoTrackerConfig {
    enter: u64,
    exit: u64,
}

impl PoolTkoTrackerConfig {
    pub fn enter(&self) -> u64 {
        self.enter
    }

    pub fn exit(&self) -> u64 {
        self.exit
    }
}

#[derive(Debug, Deserialize)]
struct RawPoolTkoTrackerConfig {
    #[serde(default)]
    num_tko_threshold_upper: Option<u64>,
    #[serde(default)]
    percent_tko_threshold_upper: Option<u64>,
    #[serde(default)]
    num_tko_threshold_lower: Option<u64>,
    #[serde(default)]
    percent_tko_threshold_lower: Option<u64>,
}

impl RawPoolConfig {
    pub(crate) fn validate(self, name: &str) -> Result<PoolConfig, ConfigError> {
        let tko_tracker = self
            .tko_tracker
            .map(|config| config.validate(name, self.servers.len()))
            .transpose()?;

        Ok(PoolConfig {
            servers: self.servers,
            server_timeout_ms: self.server_timeout_ms,
            connect_timeout_ms: self.connect_timeout_ms,
            tko_tracker,
            extra: self.extra,
        })
    }
}

impl RawPoolTkoTrackerConfig {
    fn validate(
        self,
        pool: &str,
        server_count: usize,
    ) -> Result<PoolTkoTrackerConfig, ConfigError> {
        let enter = resolve_tko_threshold(
            self.num_tko_threshold_upper,
            self.percent_tko_threshold_upper,
            server_count,
            pool,
        )?;
        let exit = resolve_tko_threshold(
            self.num_tko_threshold_lower,
            self.percent_tko_threshold_lower,
            server_count,
            pool,
        )?;

        if enter == 0 || exit == 0 {
            return Err(ConfigError::InvalidPoolTkoTracker {
                pool: pool.to_string(),
                reason: "both tko threshold upper and lower must be configured",
            });
        }
        if exit > enter {
            return Err(ConfigError::InvalidPoolTkoTracker {
                pool: pool.to_string(),
                reason: "tko upper threshold must be >= lower threshold",
            });
        }

        Ok(PoolTkoTrackerConfig { enter, exit })
    }
}

fn resolve_tko_threshold(
    number: Option<u64>,
    percent: Option<u64>,
    server_count: usize,
    pool: &str,
) -> Result<u64, ConfigError> {
    // number takes precedence
    match (number, percent) {
        (Some(threshold), _) => Ok(threshold),
        (None, None) => Ok(0),
        (None, Some(percent)) => {
            let threshold = u128::from(percent) * server_count as u128 / 100;
            u64::try_from(threshold).map_err(|_| ConfigError::InvalidPoolTkoTracker {
                pool: pool.to_string(),
                reason: "resolved tko threshold exceeds u64",
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pool(json: &str) -> PoolConfig {
        serde_json::from_str::<RawPoolConfig>(json)
            .unwrap()
            .validate("test")
            .unwrap()
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
                           "num_tko_threshold_lower": 1
                       }
                   }"#,
        );

        let tko = pool.tko_tracker.unwrap();
        assert_eq!(tko.enter(), 2);
        assert_eq!(tko.exit(), 1);
    }

    #[test]
    fn rejects_missing_servers_field() {
        let json = r#"{ "protocol": "ascii" }"#;
        let err = serde_json::from_str::<RawPoolConfig>(json).unwrap_err();

        assert!(err.to_string().contains("servers"), "got: {err}");
    }

    #[test]
    fn rejects_servers_as_non_array() {
        let json = r#"{ "servers": "localhost:1" }"#;

        assert!(serde_json::from_str::<RawPoolConfig>(json).is_err());
    }

    #[test]
    fn rejects_invalid_tko_thresholds() {
        for json in [
            r#"{ "servers": ["a:1"], "tko_tracker": { "num_tko_threshold_upper": 3 } }"#,
            r#"{ "servers": ["a:1"], "tko_tracker": { "num_tko_threshold_upper": 1, "num_tko_threshold_lower": 3 } }"#,
        ] {
            let raw = serde_json::from_str::<RawPoolConfig>(json).unwrap();
            assert!(matches!(
                raw.validate("test"),
                Err(ConfigError::InvalidPoolTkoTracker { .. })
            ));
        }
    }
}
