use serde::Deserialize;
use serde_json::{Map, Value};

use crate::{server::RawServerConfig, ConfigError, ServerConfig};

const MAX_TIMEOUT_MS: u64 = 1_000_000;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PoolConfig {
    name: String,
    servers: Vec<ServerConfig>,
    server_timeout_ms: Option<u64>,
    connect_timeout_ms: Option<u64>,
    tko_tracker: Option<PoolTkoTrackerConfig>,
    extra: Map<String, Value>,
}

impl PoolConfig {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn servers(&self) -> &[ServerConfig] {
        &self.servers
    }

    pub fn server_timeout_ms(&self) -> Option<u64> {
        self.server_timeout_ms
    }

    pub fn connect_timeout_ms(&self) -> Option<u64> {
        self.connect_timeout_ms
    }

    pub fn tko_tracker(&self) -> Option<PoolTkoTrackerConfig> {
        self.tko_tracker
    }

    pub fn extra(&self) -> &Map<String, Value> {
        &self.extra
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct RawPoolConfig {
    servers: Vec<RawServerConfig>,
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
        let servers = self
            .servers
            .into_iter()
            .enumerate()
            .map(|(index, server)| server.validate(name, index))
            .collect::<Result<Vec<_>, _>>()?;

        let tko_tracker = self
            .tko_tracker
            .map(|config| config.validate(name, servers.len()))
            .transpose()?;
        let server_timeout_ms = validate_timeout(name, "server_timeout", self.server_timeout_ms)?;
        let connect_timeout_ms =
            validate_timeout(name, "connect_timeout", self.connect_timeout_ms)?;

        Ok(PoolConfig {
            name: name.to_string(),
            servers,
            server_timeout_ms,
            connect_timeout_ms,
            tko_tracker,
            extra: self.extra,
        })
    }
}

fn validate_timeout(
    pool: &str,
    field: &'static str,
    value: Option<u64>,
) -> Result<Option<u64>, ConfigError> {
    match value {
        None | Some(1..=MAX_TIMEOUT_MS) => Ok(value),
        Some(value) => Err(ConfigError::InvalidPoolTimeout {
            pool: pool.to_string(),
            field,
            value,
        }),
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

        assert_eq!(pool.servers[0].access_point(), "localhost:11211");
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
    fn validates_timeout_bounds() {
        for value in [1, MAX_TIMEOUT_MS] {
            let raw = serde_json::from_value::<RawPoolConfig>(serde_json::json!({
                "servers": ["a:1"],
                "server_timeout": value,
                "connect_timeout": value,
            }))
            .unwrap();
            assert!(raw.validate("test").is_ok());
        }

        for value in [0, MAX_TIMEOUT_MS + 1] {
            let raw = serde_json::from_value::<RawPoolConfig>(serde_json::json!({
                "servers": ["a:1"],
                "server_timeout": value,
            }))
            .unwrap();
            assert!(matches!(
                raw.validate("test"),
                Err(ConfigError::InvalidPoolTimeout { .. })
            ));
        }
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
    fn resolves_percent_tko_thresholds() {
        let pool = pool(
            r#"{
                "servers": ["a:1", "b:2", "c:3", "d:4", "e:5", "f:6", "g:7", "h:8", "i:9", "j:10"],
                "tko_tracker": {
                    "percent_tko_threshold_upper": 50,
                    "percent_tko_threshold_lower": 20
                }
            }"#,
        );

        let tko = pool.tko_tracker.unwrap();
        assert_eq!(tko.enter(), 5);
        assert_eq!(tko.exit(), 2);
    }

    #[test]
    fn numeric_tko_threshold_takes_precedence() {
        assert_eq!(
            resolve_tko_threshold(Some(2), Some(u64::MAX), usize::MAX, "test").unwrap(),
            2
        );
    }

    #[test]
    fn rejects_tko_thresholds_that_resolve_to_zero_or_overflow() {
        let raw = serde_json::from_str::<RawPoolConfig>(
            r#"{
                "servers": ["a:1", "b:2", "c:3"],
                "tko_tracker": {
                    "percent_tko_threshold_upper": 1,
                    "percent_tko_threshold_lower": 1
                }
            }"#,
        )
        .unwrap();
        assert!(matches!(
            raw.validate("test"),
            Err(ConfigError::InvalidPoolTkoTracker { .. })
        ));

        assert!(matches!(
            resolve_tko_threshold(None, Some(u64::MAX), usize::MAX, "test"),
            Err(ConfigError::InvalidPoolTkoTracker { .. })
        ));
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
    fn rejects_server_objects_until_route_servers_are_supported() {
        let raw =
            serde_json::from_str::<RawPoolConfig>(r#"{ "servers": [{ "type": "ErrorRoute" }] }"#)
                .unwrap();

        assert!(matches!(
            raw.validate("test"),
            Err(ConfigError::UnsupportedServerObject { .. })
        ));
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
