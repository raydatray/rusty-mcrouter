use std::fmt::Write as _;
use std::net::SocketAddr;
use std::time::Duration;

use rusty_mcrouter_backend::mock_memcached::{spawn_failing_mock_memcached, spawn_mock_memcached};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::process::Command;

mod support;

use support::{assert_stays_missing, assert_stays_value, eventually_gets, exchange, RouterProcess};

type Stack = RouterProcess;

impl RouterProcess {
    fn metrics_addr(&self) -> SocketAddr {
        self._metrics_addr
    }

    fn pid(&self) -> u32 {
        self._child.id().expect("router process is running")
    }

    async fn wait(&mut self) -> std::process::ExitStatus {
        self._child.wait().await.unwrap()
    }
}

async fn scrape(addr: SocketAddr) -> String {
    let mut connection = TcpStream::connect(addr).await.unwrap();
    connection
        .write_all(b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n")
        .await
        .unwrap();
    let mut response = Vec::new();
    connection.read_to_end(&mut response).await.unwrap();
    let response = String::from_utf8(response).unwrap();
    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"), "{response}");
    response.split_once("\r\n\r\n").unwrap().1.to_string()
}

fn assert_series(body: &str, name: &str, labels: &[(&str, &str)], expected: u64) {
    let mut rendered = String::from(name);
    if !labels.is_empty() {
        rendered.push('{');
        for (index, (key, value)) in labels.iter().enumerate() {
            if index != 0 {
                rendered.push(',');
            }
            write!(rendered, "{key}=\"{value}\"").unwrap();
        }
        rendered.push('}');
    }
    writeln!(rendered, " {expected}").unwrap();
    assert!(body.contains(&rendered), "missing {rendered:?} in:\n{body}");
}

async fn start_router(config_body: &str, tag: u16) -> Stack {
    start_router_with_args(config_body, tag, 1, &[]).await
}

async fn start_router_with_args(
    config_body: &str,
    tag: u16,
    num_proxies: usize,
    extra_args: &[&str],
) -> Stack {
    RouterProcess::spawn(config_body, tag, num_proxies, extra_args).await
}

async fn start_stack() -> Stack {
    let backend_addr = spawn_mock_memcached().await;

    exchange(backend_addr, b"ms seeded_foo 3\r\nbar\r\n", b"HD\r\n").await;

    let config_body = format!(
        r#"{{ "pools": {{ "memcached": {{ "servers": ["{}"] }} }}, "route": "PoolRoute|memcached" }}"#,
        backend_addr
    );
    start_router(&config_body, backend_addr.port()).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn basic_command_flow_crosses_the_full_stack() {
    let fx = start_stack().await;

    exchange(fx.router_addr, b"mg seeded_foo v\r\n", b"VA 3\r\nbar\r\n").await;
    exchange(fx.router_addr, b"mg system_missing v\r\n", b"EN\r\n").await;
    exchange(
        fx.router_addr,
        b"ms system_store 5 F9\r\nworld\r\n",
        b"HD\r\n",
    )
    .await;
    exchange(
        fx.router_addr,
        b"mg system_store v f s\r\n",
        b"VA 5 f9 s5\r\nworld\r\n",
    )
    .await;
    exchange(
        fx.router_addr,
        b"ma system_counter N60 J41 v\r\n",
        b"VA 2\r\n41\r\n",
    )
    .await;
    exchange(fx.router_addr, b"md system_store\r\n", b"HD\r\n").await;
    exchange(fx.router_addr, b"mg system_store v\r\n", b"EN\r\n").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pipelined_quiet_gets_with_noop_fence_replace_multiget() {
    let fx = start_stack().await;
    // Meta multiget: quiet gets suppress the miss, opaque correlates the
    // hit, and `mn` fences the batch. The miss slot must produce no bytes
    // while preserving order.
    exchange(
        fx.router_addr,
        b"mg seeded_foo v q Ofirst\r\nmg mock_e2e_multi_miss v q Osecond\r\nmn\r\n",
        b"VA 3 Ofirst\r\nbar\r\nMN\r\n",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recoverable_parse_error_keeps_pipeline_order_and_connection() {
    let fx = start_stack().await;
    // middle command is malformed; its error must arrive in order and the
    // connection must keep serving.
    exchange(
        fx.router_addr,
        b"mg seeded_foo v\r\nmg seeded_foo zz\r\nmg seeded_foo v\r\n",
        b"VA 3\r\nbar\r\nCLIENT_ERROR invalid flag\r\nVA 3\r\nbar\r\n",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn opaque_and_key_echo_survive_the_hop() {
    let fx = start_stack().await;
    exchange(
        fx.router_addr,
        b"ms me2e_echo 2 c s k Otag\r\nhi\r\n",
        b"HD c2 s2 kme2e_echo Otag\r\n",
    )
    .await;
}

/// the observability finale: traffic shows up on /metrics with the
/// right families, labels and values.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_endpoint_reports_traffic() {
    let fx = start_stack().await;
    exchange(fx.router_addr, b"mg seeded_foo v\r\n", b"VA 3\r\nbar\r\n").await;
    exchange(fx.router_addr, b"mg mock_e2e_missing v\r\n", b"EN\r\n").await;

    let body = scrape(fx.metrics_addr()).await;
    assert!(
        body.contains("rusty_mcrouter_requests_total{command=\"mg\"} 2\n"),
        "{body}"
    );
    assert!(
        body.contains(
            "rusty_mcrouter_backend_requests_total{command=\"mg\",result=\"success\"} 2\n"
        ),
        "{body}"
    );
    assert!(
        body.contains("rusty_mcrouter_destination_up{destination=\"") && body.contains("\"} 1\n"),
        "{body}"
    );
    assert!(body.contains("rusty_mcrouter_proxies 1\n"), "{body}");
    assert!(
        body.contains("rusty_mcrouter_build_info{version="),
        "{body}"
    );
    // gauges settled after the exchanges closed their connections
    assert!(
        body.contains("rusty_mcrouter_backend_pending_reqs 0\n"),
        "{body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_endpoint_reports_null_route_requests() {
    let fx = start_router(r#"{ "route": "NullRoute" }"#, 60_001).await;

    exchange(fx.router_addr, b"mg discarded v\r\n", b"EN\r\n").await;

    let body = scrape(fx.metrics_addr()).await;
    assert!(
        body.contains("rusty_mcrouter_dev_null_requests_total 1\n"),
        "{body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn null_route_sums_two_proxy_shards() {
    let fx = start_router_with_args(r#"{ "route": "NullRoute" }"#, 60_002, 2, &[]).await;

    exchange(fx.router_addr, b"mg first v\r\n", b"EN\r\n").await;
    exchange(fx.router_addr, b"mg second v\r\n", b"EN\r\n").await;

    let body = scrape(fx.metrics_addr()).await;
    assert_series(&body, "rusty_mcrouter_dev_null_requests_total", &[], 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ctrl_c_stops_proxy_and_control_threads_cleanly() {
    let mut stack = start_router(r#"{ "route": "NullRoute" }"#, 60_003).await;
    let pid = stack.pid();
    let status = Command::new("kill")
        .arg("-INT")
        .arg(pid.to_string())
        .status()
        .await
        .unwrap();
    assert!(status.success());

    let status = tokio::time::timeout(Duration::from_secs(5), stack.wait())
        .await
        .expect("router did not stop after Ctrl-C");
    assert!(status.success(), "router exited with {status}");
}

/// a dead backend marks hard on first contact (connect refused) and the
/// scrape shows it: tko gauge up, destination down, tko-result counted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_endpoint_reports_tko() {
    // bind-then-drop: the port is (almost certainly) unbound
    let dead_addr = {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        listener.local_addr().unwrap()
    };
    let config_body = format!(
        r#"{{ "pools": {{ "memcached": {{ "servers": ["{dead_addr}"] }} }}, "route": "PoolRoute|memcached" }}"#
    );
    let fx = start_router_with_args(&config_body, dead_addr.port(), 1, &[]).await;

    // first send fails and marks hard; retry until the mark lands
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let mut conn = TcpStream::connect(fx.router_addr).await.unwrap();
        conn.write_all(b"mg tko_probe v\r\n").await.unwrap();
        // the reply is an error line; the router keeps the connection
        // open, so read one bounded chunk instead of to-close
        let mut chunk = [0u8; 1024];
        let _ = tokio::time::timeout(Duration::from_secs(2), conn.read(&mut chunk)).await;
        drop(conn);

        let body = scrape(fx.metrics_addr()).await;
        if body.contains("rusty_mcrouter_tko{kind=\"hard\"} 1\n") {
            assert!(
                body.contains(&format!(
                    "rusty_mcrouter_destination_up{{destination=\"{dead_addr}\"}} 0\n"
                )),
                "{body}"
            );
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "hard tko never appeared on /metrics: {body}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failover_from_failing_primary_serves_from_secondary() {
    let primary = spawn_failing_mock_memcached().await;
    let secondary = spawn_mock_memcached().await;

    exchange(secondary, b"ms failover_k 6\r\nbackup\r\n", b"HD\r\n").await;

    let config_body = format!(
        r#"{{ "pools": {{ "primary": {{ "servers": ["{primary}"] }}, "secondary": {{ "servers": ["{secondary}"] }} }}, "route": {{ "type": "FailoverRoute", "children": ["PoolRoute|primary", "PoolRoute|secondary"] }} }}"#
    );
    let fx = start_router(&config_body, primary.port()).await;

    exchange(
        fx.router_addr,
        b"mg failover_k v\r\n",
        b"VA 6\r\nbackup\r\n",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failover_metrics_count_one_entry_and_three_pool_attempts() {
    let primary = spawn_failing_mock_memcached().await;
    let backup_1 = spawn_failing_mock_memcached().await;
    let backup_2 = spawn_mock_memcached().await;

    exchange(backup_2, b"ms route_obs 5\r\nvalue\r\n", b"HD\r\n").await;

    let config = format!(
        r#"{{
            "pools": {{
                "primary": {{"servers": ["{primary}"]}},
                "backup_1": {{"servers": ["{backup_1}"]}},
                "backup_2": {{"servers": ["{backup_2}"]}}
            }},
            "route": {{
                "type": "FailoverRoute",
                "children": [
                    "PoolRoute|primary",
                    "PoolRoute|backup_1",
                    "PoolRoute|backup_2"
                ]
            }}
        }}"#
    );
    let stack = start_router(&config, primary.port()).await;

    exchange(
        stack.router_addr,
        b"mg route_obs v\r\n",
        b"VA 5\r\nvalue\r\n",
    )
    .await;

    let body = scrape(stack.metrics_addr()).await;
    assert_series(
        &body,
        "rusty_mcrouter_failover_total",
        &[("policy", "inorder")],
        1,
    );
    for pool in ["primary", "backup_1", "backup_2"] {
        assert_series(
            &body,
            "rusty_mcrouter_pool_requests_total",
            &[("pool", pool)],
            1,
        );
    }
    assert_series(
        &body,
        "rusty_mcrouter_pool_completed_requests_total",
        &[("pool", "primary")],
        1,
    );
    assert_series(
        &body,
        "rusty_mcrouter_pool_completed_requests_total",
        &[("pool", "backup_2")],
        0,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_proxy_shards_sum_into_one_pool_series() {
    let backend = spawn_mock_memcached().await;
    let config = format!(
        r#"{{"pools": {{"pool": {{"servers": ["{backend}"]}}}}, "route": "PoolRoute|pool"}}"#
    );
    let stack = start_router_with_args(&config, backend.port(), 2, &[]).await;

    exchange(stack.router_addr, b"mg first v\r\n", b"EN\r\n").await;
    exchange(stack.router_addr, b"mg second v\r\n", b"EN\r\n").await;

    let body = scrape(stack.metrics_addr()).await;
    assert_series(
        &body,
        "rusty_mcrouter_pool_requests_total",
        &[("pool", "pool")],
        2,
    );
    assert_series(
        &body,
        "rusty_mcrouter_pool_completed_requests_total",
        &[("pool", "pool")],
        2,
    );
    assert_series(&body, "rusty_mcrouter_proxies", &[], 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prefix_routes_select_default_exact_and_invalid_fallback() {
    let backend_a = spawn_mock_memcached().await;
    let backend_b = spawn_mock_memcached().await;
    exchange(backend_a, b"ms choice 1\r\na\r\n", b"HD\r\n").await;
    exchange(backend_b, b"ms choice 1\r\nb\r\n", b"HD\r\n").await;
    let config = format!(
        r#"{{
            "pools": {{
                "a": {{"servers": ["{backend_a}"]}},
                "b": {{"servers": ["{backend_b}"]}}
            }},
            "routes": {{
                "/a/a/": "PoolRoute|a",
                "/b/b/": "PoolRoute|b"
            }}
        }}"#
    );
    let stack = start_router_with_args(
        &config,
        backend_a.port(),
        1,
        &["-R", "/b/b/", "--send-invalid-route-to-default"],
    )
    .await;

    exchange(stack.router_addr, b"mg choice v\r\n", b"VA 1\r\nb\r\n").await;
    exchange(stack.router_addr, b"mg /a/a/choice v\r\n", b"VA 1\r\na\r\n").await;
    exchange(
        stack.router_addr,
        b"mg /missing/route/choice v\r\n",
        b"VA 1\r\nb\r\n",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prefix_selector_uses_longest_policy_and_wildcard() {
    let users = spawn_mock_memcached().await;
    let vip = spawn_mock_memcached().await;
    let wildcard = spawn_mock_memcached().await;
    exchange(users, b"ms user:1 1\r\nu\r\n", b"HD\r\n").await;
    exchange(vip, b"ms user:vip:1 1\r\nv\r\n", b"HD\r\n").await;
    exchange(wildcard, b"ms other:1 1\r\nw\r\n", b"HD\r\n").await;
    let config = format!(
        r#"{{
            "pools": {{
                "users": {{"servers": ["{users}"]}},
                "vip": {{"servers": ["{vip}"]}},
                "wildcard": {{"servers": ["{wildcard}"]}}
            }},
            "route": {{
                "type": "PrefixSelectorRoute",
                "policies": {{
                    "user:": "PoolRoute|users",
                    "user:vip:": "PoolRoute|vip"
                }},
                "wildcard": "PoolRoute|wildcard"
            }}
        }}"#
    );
    let stack = start_router(&config, users.port()).await;

    exchange(stack.router_addr, b"mg user:vip:1 v\r\n", b"VA 1\r\nv\r\n").await;
    exchange(stack.router_addr, b"mg user:1 v\r\n", b"VA 1\r\nu\r\n").await;
    exchange(stack.router_addr, b"mg other:1 v\r\n", b"VA 1\r\nw\r\n").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn regional_and_global_wildcards_fan_out() {
    let us_a = spawn_mock_memcached().await;
    let us_b = spawn_mock_memcached().await;
    let eu_a = spawn_mock_memcached().await;
    let config = format!(
        r#"{{
            "pools": {{
                "us_a": {{"servers": ["{us_a}"]}},
                "us_b": {{"servers": ["{us_b}"]}},
                "eu_a": {{"servers": ["{eu_a}"]}}
            }},
            "routes": {{
                "/us/a/": "PoolRoute|us_a",
                "/us/b/": "PoolRoute|us_b",
                "/eu/a/": "PoolRoute|eu_a"
            }}
        }}"#
    );
    let stack = start_router_with_args(&config, us_a.port(), 1, &["-R", "/us/a/"]).await;

    exchange(
        stack.router_addr,
        b"ms /us/*/regional 1\r\nr\r\n",
        b"HD\r\n",
    )
    .await;
    eventually_gets(us_a, b"regional", b"r").await;
    eventually_gets(us_b, b"regional", b"r").await;
    assert_stays_missing(eu_a, b"regional").await;

    exchange(stack.router_addr, b"ms /*/*/global 1\r\ng\r\n", b"HD\r\n").await;
    eventually_gets(us_a, b"global", b"g").await;
    eventually_gets(us_b, b"global", b"g").await;
    eventually_gets(eu_a, b"global", b"g").await;

    let body = scrape(stack.metrics_addr()).await;
    assert_series(
        &body,
        "rusty_mcrouter_pool_completed_requests_total",
        &[("pool", "us_a")],
        2,
    );
    for pool in ["us_b", "eu_a"] {
        assert_series(
            &body,
            "rusty_mcrouter_pool_completed_requests_total",
            &[("pool", pool)],
            0,
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fallback_and_arbitrary_globs_match_mcrouter() {
    let primary = spawn_mock_memcached().await;
    let secondary = spawn_mock_memcached().await;
    let fallback = spawn_mock_memcached().await;
    let config = format!(
        r#"{{
            "pools": {{
                "primary": {{"servers": ["{primary}"]}},
                "secondary": {{"servers": ["{secondary}"]}},
                "fallback": {{"servers": ["{fallback}"]}}
            }},
            "routes": {{
                "/us/prod/": "PoolRoute|primary",
                "/uk/preprod/": "PoolRoute|secondary",
                "/us/fallback/": "PoolRoute|fallback"
            }}
        }}"#
    );
    let stack = start_router_with_args(&config, primary.port(), 1, &["-R", "/us/prod/"]).await;

    exchange(
        stack.router_addr,
        b"ms /us/missing/fallback-key 1\r\nf\r\n",
        b"HD\r\n",
    )
    .await;
    eventually_gets(fallback, b"fallback-key", b"f").await;
    assert_stays_missing(primary, b"fallback-key").await;

    exchange(
        stack.router_addr,
        b"ms /u*/*prod/glob-key 1\r\nx\r\n",
        b"HD\r\n",
    )
    .await;
    eventually_gets(primary, b"glob-key", b"x").await;
    eventually_gets(secondary, b"glob-key", b"x").await;
    assert_stays_missing(fallback, b"glob-key").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wildcard_fanout_deduplicates_shared_aliases() {
    let backend = spawn_mock_memcached().await;
    exchange(backend, b"ms dedup 1\r\nx\r\n", b"HD\r\n").await;
    let config = format!(
        r#"{{
            "pools": {{ "shared": {{"servers": ["{backend}"]}} }},
            "named_handles": {{
                "shared-route": "PoolRoute|shared"
            }},
            "routes": {{
                "/us/a/": "shared-route",
                "/us/b/": "PoolRoute|shared"
            }}
        }}"#
    );
    let stack = start_router_with_args(&config, backend.port(), 1, &["-R", "/us/a/"]).await;

    exchange(stack.router_addr, b"ms /*/*/dedup 1 MA\r\ny\r\n", b"HD\r\n").await;
    eventually_gets(backend, b"dedup", b"xy").await;
    assert_stays_value(backend, b"dedup", b"xy").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wildcard_fanout_deduplicates_inline_named_routes() {
    let backend = spawn_mock_memcached().await;
    exchange(backend, b"ms named-dedup 1\r\nx\r\n", b"HD\r\n").await;
    let config = format!(
        r#"{{
            "pools": {{ "shared": {{"servers": ["{backend}"]}} }},
            "routes": [
                {{
                    "aliases": ["/us/a/"],
                    "route": {{
                        "name": "inline-shared",
                        "type": "PoolRoute",
                        "pool": "shared"
                    }}
                }},
                {{
                    "aliases": ["/us/b/"],
                    "route": "inline-shared"
                }}
            ]
        }}"#
    );
    let stack = start_router_with_args(&config, backend.port(), 1, &["-R", "/us/a/"]).await;

    exchange(
        stack.router_addr,
        b"ms /*/*/named-dedup 1 MA\r\ny\r\n",
        b"HD\r\n",
    )
    .await;
    eventually_gets(backend, b"named-dedup", b"xy").await;
    assert_stays_value(backend, b"named-dedup", b"xy").await;
}
