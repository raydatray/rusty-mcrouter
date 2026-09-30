use serde::Deserialize;
use serde_json::{Map, Value};

use crate::ConfigError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServerConfig {
    access_point: String,
}

impl ServerConfig {
    pub fn access_point(&self) -> &str {
        &self.access_point
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub(crate) enum RawServerConfig {
    AccessPoint(String),
    Route(Map<String, Value>),
}

impl RawServerConfig {
    pub(crate) fn validate(self, pool: &str, index: usize) -> Result<ServerConfig, ConfigError> {
        match self {
            RawServerConfig::AccessPoint(access_point) => {
                if !validate_access_point(&access_point) {
                    return Err(ConfigError::InvalidServerAddress {
                        pool: pool.to_string(),
                        index,
                        address: access_point,
                    });
                }
                Ok(ServerConfig { access_point })
            }
            RawServerConfig::Route(fields) => {
                drop(fields);
                Err(ConfigError::UnsupportedServerObject {
                    pool: pool.to_string(),
                    index,
                })
            }
        }
    }
}

fn validate_access_point(access_point: &str) -> bool {
    access_point
        .rsplit_once(':')
        .is_some_and(|(host, port)| !host.is_empty() && port.parse::<u16>().is_ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn validate(access_point: &str) -> Result<ServerConfig, ConfigError> {
        RawServerConfig::AccessPoint(access_point.to_string()).validate("test", 0)
    }

    #[test]
    fn validates_supported_access_points() {
        for access_point in ["localhost:11211", "127.0.0.1:11211", "[::1]:11211"] {
            assert!(validate(access_point).is_ok(), "got: {access_point}");
        }
    }

    #[test]
    fn rejects_invalid_access_points() {
        for access_point in ["", "localhost", ":11211", "host:notaport", "host:65536"] {
            assert!(matches!(
                validate(access_point),
                Err(ConfigError::InvalidServerAddress { .. })
            ));
        }
    }
}
