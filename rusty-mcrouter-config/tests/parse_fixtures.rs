use rusty_mcrouter_config::{
    parse_file, ConfigDocument, ConfigError, FailoverErrorKind, FailoverErrorsConfig,
    FailoverPolicyConfig, HashConfig, RouteEntry, RouteHandleConfig,
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

#[test]
fn nullroute_minimal_config() {
    let doc = parse_ok("nullroute.json");
    assert!(doc.pools.is_empty());
    assert!(doc.named_handles.is_empty());

    let RouteEntry::Single(RouteHandleConfig::Reference(name)) = &doc.route else {
        panic!("expected Single Reference, got {:?}", doc.route);
    };
    assert_eq!(name, "NullRoute");
}

#[test]
fn basic_1_1_1_canonical_pool_and_route() {
    let doc = parse_ok("basic_1_1_1.json");
    assert_eq!(doc.pools.len(), 1);
    assert_eq!(
        doc.pools["foo"].servers[0].access_point(),
        "localhost:12345"
    );

    let RouteEntry::Single(RouteHandleConfig::Shorthand { kind, args }) = &doc.route else {
        panic!("expected Shorthand, got {:?}", doc.route);
    };
    assert_eq!(kind, "PoolRoute");
    assert_eq!(args, &vec!["foo".to_string()]);
}

#[test]
fn memcache_local_config_parses() {
    let doc = parse_ok("memcache_local_config.json");
    assert_eq!(doc.pools.len(), 1);
    assert!(doc.pools.contains_key("A"));
}

#[test]
fn dev_null_object_form_route_with_nested_children() {
    let doc = parse_ok("dev_null.json");
    assert_eq!(doc.pools.len(), 2);

    let RouteEntry::Single(RouteHandleConfig::Unknown { kind, fields }) = &doc.route else {
        panic!("expected Unknown PrefixSelectorRoute, got {:?}", doc.route);
    };
    assert_eq!(kind, "PrefixSelectorRoute");
    assert!(fields.contains_key("policies"));
    assert!(fields.contains_key("wildcard"));
}

#[test]
fn named_handles_object_form_resolves_keys() {
    let doc = parse_ok("named_handles_obj.json");
    assert!(doc.named_handles.contains_key("route:A"));
    assert!(doc.named_handles.contains_key("route:B"));
    assert!(doc.named_handles.contains_key("route:C"));
    assert!(doc.named_handles.contains_key("route:D"));
    assert!(doc.named_handles.contains_key("route:all"));
    assert!(doc.named_handles.contains_key("null"));
    assert_eq!(
        doc.named_handles["route:A"],
        RouteHandleConfig::PoolRoute {
            pool: "A".into(),
            hash: HashConfig::default()
        }
    );
}

#[test]
fn named_handles_list_form_lifts_name_field() {
    let doc = parse_ok("named_handles_list.json");
    assert_eq!(doc.named_handles.len(), 4);
    assert_eq!(
        doc.named_handles["route:A"],
        RouteHandleConfig::PoolRoute {
            pool: "A".into(),
            hash: HashConfig::default()
        }
    );

    let RouteHandleConfig::Unknown { kind, .. } = &doc.named_handles["route:all"] else {
        panic!("expected route:all to be Unknown AllSyncRoute");
    };
    assert_eq!(kind, "AllSyncRoute");
}

#[test]
fn empty_pool_with_multi_pipe_shorthand_children() {
    let doc = parse_ok("empty_pool.json");
    assert!(doc.pools["A-foo"].servers.is_empty());

    let RouteEntry::Single(RouteHandleConfig::Unknown { kind, fields }) = &doc.route else {
        panic!("expected Unknown AllSyncRoute, got {:?}", doc.route);
    };
    assert_eq!(kind, "AllSyncRoute");

    let children = fields
        .get("children")
        .and_then(|v| v.as_array())
        .expect("children array");
    assert!(children.iter().all(|c| c.is_string()));
    assert!(children
        .iter()
        .any(|c| c.as_str() == Some("AllSyncRoute|Pool|A-foo")));
}

#[test]
fn duplicate_servers_uses_routes_plural_with_aliases() {
    let doc = parse_ok("duplicate_servers.json");
    let RouteEntry::Prefixed(entries) = &doc.route else {
        panic!("expected Prefixed, got {:?}", doc.route);
    };
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].aliases, vec!["/a/a/".to_string()]);
    assert_eq!(entries[1].aliases, vec!["/b/b/".to_string()]);
    assert_eq!(doc.pools["A.wildcard"].servers.len(), 2);
}

#[test]
fn comments_in_jsonc_are_stripped() {
    let doc = parse_ok("with_comments.json");
    assert_eq!(
        doc.pools["foo"].servers[0].access_point(),
        "localhost:11211"
    );
    assert!(matches!(
        doc.route,
        RouteEntry::Single(RouteHandleConfig::Shorthand { ref kind, .. }) if kind == "PoolRoute"
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
    assert!(matches!(err, ConfigError::Json(_)), "got {err:?}");
}

#[test]
fn malformed_json_yields_json_error() {
    let err = parse_err("malformed_json.json");
    assert!(matches!(err, ConfigError::Json(_)), "got {err:?}");
}

#[test]
fn pool_missing_servers_yields_json_error() {
    let err = parse_err("pool_missing_servers.json");
    assert!(matches!(err, ConfigError::Json(_)), "got {err:?}");
}

#[test]
fn route_invalid_type_yields_json_error() {
    let err = parse_err("route_invalid_type.json");
    assert!(matches!(err, ConfigError::Json(_)), "got {err:?}");
}

#[test]
fn failover_least_failures_parses_children_and_policy() {
    let doc = parse_ok("failover_least_failures.json");
    assert_eq!(doc.pools.len(), 4);
    let RouteEntry::Single(RouteHandleConfig::FailoverRoute {
        children,
        failover_errors,
        failover_policy,
    }) = &doc.route
    else {
        panic!("expected FailoverRoute, got {:?}", doc.route);
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
    let RouteEntry::Single(RouteHandleConfig::FailoverRoute {
        failover_errors, ..
    }) = &doc.route
    else {
        panic!("expected FailoverRoute, got {:?}", doc.route);
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
fn failover_limit_parses_and_tolerates_unsupported_rate_limiter() {
    let doc = parse_ok("failover_limit.json");
    let RouteEntry::Single(RouteHandleConfig::FailoverRoute {
        children,
        failover_errors,
        failover_policy,
    }) = &doc.route
    else {
        panic!("expected FailoverRoute, got {:?}", doc.route);
    };
    assert_eq!(children.len(), 2);
    assert_eq!(*failover_errors, FailoverErrorsConfig::Default);
    assert_eq!(*failover_policy, FailoverPolicyConfig::InOrder);
    assert!(matches!(
        children.first(),
        Some(RouteHandleConfig::ErrorRoute { .. })
    ));
}

#[test]
fn failover_with_exptime_route_is_unknown_pending_support() {
    let doc = parse_ok("failover_with_exptime.json");
    assert_eq!(doc.pools.len(), 2);
    let RouteEntry::Single(RouteHandleConfig::Unknown { kind, .. }) = &doc.route else {
        panic!(
            "expected Unknown FailoverWithExptimeRoute, got {:?}",
            doc.route
        );
    };
    assert_eq!(kind, "FailoverWithExptimeRoute");
}
