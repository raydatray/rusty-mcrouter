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
            RawServerConfig::AccessPoint(access_point) => Ok(ServerConfig { access_point }),
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
