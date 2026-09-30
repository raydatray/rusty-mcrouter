use std::future::{pending, Future};

use rusty_mcrouter_backend::destination::DestinationConfig;
use rusty_mcrouter_backend::error::SendError;
use rusty_mcrouter_backend::{Backend, BackendFactory, PoolHealth, PreparedSend, TkoRejection};
use rusty_mcrouter_config::{ConfigDocument, ServerConfig};
use rusty_mcrouter_protocol::{Reply, Request};

use crate::{build_route_with_options, BuildError, RootRouteOptions};

/// Checks that `config` builds by running the real builder over inert
/// backends: nothing is connected, spawned or registered.
pub fn validate(
    config: &ConfigDocument,
    defaults: &DestinationConfig,
    root_options: &RootRouteOptions,
) -> Result<(), BuildError> {
    build_route_with_options(config, &InertFactory, defaults, root_options).map(drop)
}

struct InertFactory;

struct InertBackend;

impl BackendFactory for InertFactory {
    type Backend = InertBackend;

    fn make(&self, _: &ServerConfig, _: &DestinationConfig, _: &PoolHealth<'_>) -> InertBackend {
        InertBackend
    }
}

impl Backend for InertBackend {
    fn prepare_send(
        &self,
        _request: Request,
    ) -> Result<PreparedSend<impl Future<Output = Result<Reply, SendError>> + '_>, TkoRejection>
    {
        Ok(PreparedSend::new(pending::<Result<Reply, SendError>>()))
    }
}

#[cfg(test)]
mod tests {
    use rusty_mcrouter_config::parse;

    use super::*;

    fn check(json: &str, root_options: &RootRouteOptions) -> Result<(), BuildError> {
        validate(
            &parse(json).unwrap(),
            &DestinationConfig::default(),
            root_options,
        )
    }

    #[test]
    fn rejects_a_config_without_the_default_route() {
        let error = check(
            r#"{ "routes": { "/other/cluster/": "NullRoute" } }"#,
            &RootRouteOptions::default(),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            BuildError::DefaultRouteMissing { ref prefix } if prefix == "/././"
        ));
    }

    #[test]
    fn checks_the_default_route_the_process_was_started_with() {
        let json = r#"{ "routes": { "/a/a/": "NullRoute", "/b/b/": "NullRoute" } }"#;
        let started_with = |prefix: &str| RootRouteOptions {
            default_route: prefix.parse().unwrap(),
            send_invalid_to_default: false,
        };

        assert!(check(json, &started_with("/b/b/")).is_ok());
        assert!(check(json, &started_with("/c/c/")).is_err());
    }

    /// A plain `#[test]` on purpose: a build that spawned tasks would panic.
    #[test]
    fn accepts_a_full_config_without_a_runtime() {
        let json = r#"{
            "pools": {
                "primary": {
                    "servers": ["a:1", "b:1"],
                    "tko_tracker": {
                        "num_tko_threshold_upper": 2,
                        "num_tko_threshold_lower": 1
                    }
                },
                "backup": { "servers": ["c:1"], "server_timeout": 50 }
            },
            "route": {
                "type": "PrefixSelectorRoute",
                "policies": {
                    "user:": {
                        "type": "FailoverRoute",
                        "children": ["PoolRoute|primary", "PoolRoute|backup"]
                    }
                },
                "wildcard": "PoolRoute|backup"
            }
        }"#;

        assert!(check(json, &RootRouteOptions::default()).is_ok());
    }
}
