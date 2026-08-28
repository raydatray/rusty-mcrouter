use rusty_mcrouter_config::{
    parse_file, ConfigDocument, ConfigError, FailoverErrorKind, FailoverErrorsConfig,
    FailoverPolicyConfig, PrefixSelectorConfig, RootRouteConfig, RouteConfig,
};
use std::path::PathBuf;

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

fn parse_ok(name: &str) -> ConfigDocument {
    parse_file(fixture(name)).unwrap_or_else(|e| panic!("fixture {name} failed to parse: {e}"))
}

fn parse_err(name: &str) -> ConfigError {
    parse_file(fixture(name))
        .err()
        .unwrap_or_else(|| panic!("fixture {name} unexpectedly parsed successfully"))
}

fn single_selector(document: &ConfigDocument) -> &PrefixSelectorConfig {
    let RootRouteConfig::Single(selector) = document.root();
    selector
}

fn single_wildcard(document: &ConfigDocument) -> &RouteConfig {
    single_selector(document)
        .wildcard()
        .expect("ordinary root route should normalize to wildcard")
        .route()
}

#[test]
fn nullroute_minimal_config() {
    let doc = parse_ok("nullroute.json");
    assert_eq!(doc.pools().len(), 0);
    assert_eq!(single_wildcard(&doc), &RouteConfig::NullRoute);
}

#[test]
fn basic_1_1_1_canonical_pool_and_route() {
    let doc = parse_ok("basic_1_1_1.json");
    assert_eq!(doc.pools().len(), 1);
    assert_eq!(
        doc.pool_by_name("foo").unwrap().servers()[0].access_point(),
        "localhost:12345"
    );

    assert!(matches!(
        single_wildcard(&doc),
        RouteConfig::PoolRoute { pool, .. } if doc.pool(*pool).name() == "foo"
    ));
}

#[test]
fn memcache_local_config_parses() {
    let doc = parse_ok("memcache_local_config.json");
    assert_eq!(doc.pools().len(), 1);
    assert!(doc.pool_by_name("A").is_some());
}

#[test]
fn upstream_pool_timeout_parses() {
    let doc = parse_ok("tko_reconfigure1.json");
    assert_eq!(
        doc.pool_by_name("A").unwrap().server_timeout_ms(),
        Some(12_345)
    );
}

#[test]
fn unsupported_caret_protocol_is_rejected() {
    assert!(matches!(
        parse_err("basic_caret.json"),
        ConfigError::UnsupportedPoolProtocol { ref protocol, .. } if protocol == "caret"
    ));
}

#[test]
fn prefix_selector_route_validates_its_children() {
    assert!(matches!(
        parse_err("dev_null.json"),
        ConfigError::UnresolvedReference { ref name } if name == "DevNullRoute"
    ));
}

#[test]
fn named_handles_object_form_rejects_unsupported_definitions() {
    assert!(matches!(
        parse_err("named_handles_obj.json"),
        ConfigError::UnsupportedRouteType { ref kind } if kind == "AllSyncRoute"
    ));
}

#[test]
fn named_handles_list_form_rejects_unsupported_definitions() {
    assert!(matches!(
        parse_err("named_handles_list.json"),
        ConfigError::UnsupportedRouteType { ref kind } if kind == "AllSyncRoute"
    ));
}

#[test]
fn unsupported_all_sync_route_is_rejected() {
    assert!(matches!(
        parse_err("empty_pool.json"),
        ConfigError::UnsupportedRouteType { ref kind } if kind == "AllSyncRoute"
    ));
}

#[test]
fn routes_plural_is_rejected_until_supported() {
    assert!(matches!(
        parse_err("duplicate_servers.json"),
        ConfigError::PrefixRoutingNotImplemented
    ));
}

#[test]
fn comments_in_jsonc_are_stripped() {
    let doc = parse_ok("with_comments.json");
    assert_eq!(
        doc.pool_by_name("foo").unwrap().servers()[0].access_point(),
        "localhost:11211"
    );
    assert!(matches!(
        single_wildcard(&doc),
        RouteConfig::PoolRoute { .. }
    ));
}

#[test]
fn missing_route_field_yields_structural_error() {
    let err = parse_err("missing_route.json");
    assert!(matches!(err, ConfigError::MissingRoute), "got {err:?}");
}

#[test]
fn both_route_and_routes_present_yields_structural_error() {
    let err = parse_err("both_route_and_routes.json");
    assert!(
        matches!(err, ConfigError::BothRouteAndRoutes),
        "got {err:?}"
    );
}

#[test]
fn bad_pool_servers_yields_json_error() {
    let err = parse_err("bad_pool_servers_not_array.json");
    assert!(matches!(err, ConfigError::Schema { .. }), "got {err:?}");
}

#[test]
fn malformed_json_yields_json_error() {
    let err = parse_err("malformed_json.json");
    assert!(matches!(err, ConfigError::Json(_)), "got {err:?}");
}

#[test]
fn pool_missing_servers_yields_json_error() {
    let err = parse_err("pool_missing_servers.json");
    assert!(matches!(err, ConfigError::Schema { .. }), "got {err:?}");
}

#[test]
fn route_invalid_type_yields_json_error() {
    let err = parse_err("route_invalid_type.json");
    assert!(matches!(err, ConfigError::Schema { .. }), "got {err:?}");
}

#[test]
fn failover_least_failures_parses_children_and_policy() {
    let doc = parse_ok("failover_least_failures.json");
    assert_eq!(doc.pools().len(), 4);
    let RouteConfig::FailoverRoute {
        children,
        failover_errors,
        failover_policy,
    } = single_wildcard(&doc)
    else {
        panic!("expected FailoverRoute, got {:?}", single_wildcard(&doc));
    };
    assert_eq!(children.len(), 4);
    assert_eq!(*failover_errors, FailoverErrorsConfig::Default);
    assert_eq!(
        *failover_policy,
        FailoverPolicyConfig::LeastFailures { max_tries: 3 }
    );
}

#[test]
fn failover_custom_errors_parses_per_op_lists() {
    let doc = parse_ok("failover_custom_errors.json");
    let RouteConfig::FailoverRoute {
        failover_errors, ..
    } = single_wildcard(&doc)
    else {
        panic!("expected FailoverRoute, got {:?}", single_wildcard(&doc));
    };
    assert_eq!(
        *failover_errors,
        FailoverErrorsConfig::PerOp {
            gets: Some(vec![FailoverErrorKind::RemoteError]),
            updates: Some(vec![]),
            deletes: None,
        }
    );
}

#[test]
fn unsupported_failover_limit_is_rejected() {
    assert!(matches!(
        parse_err("failover_limit.json"),
        ConfigError::Schema { .. }
    ));
}

#[test]
fn failover_with_exptime_route_is_rejected_pending_support() {
    assert!(matches!(
        parse_err("failover_with_exptime.json"),
        ConfigError::UnsupportedRouteType { ref kind } if kind == "FailoverWithExptimeRoute"
    ));
}
