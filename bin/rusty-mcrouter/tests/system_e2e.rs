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

fn series(name: &str, labels: &[(&str, &str)], value: u64) -> String {
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
    writeln!(rendered, " {value}").unwrap();
    rendered
}

fn assert_series(body: &str, name: &str, labels: &[(&str, &str)], expected: u64) {
    let rendered = series(name, labels, expected);
    assert!(body.contains(&rendered), "missing {rendered:?} in:\n{body}");
}

async fn eventually_series(addr: SocketAddr, name: &str, labels: &[(&str, &str)], expected: u64) {
    let rendered = series(name, labels, expected);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let body = scrape(addr).await;
        if body.contains(&rendered) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "never saw {rendered:?}; last scrape:\n{body}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn roundtrip(client: &mut TcpStream, request: &[u8], expected: &[u8]) {
    client.write_all(request).await.unwrap();
    let mut reply = vec![0; expected.len()];
    tokio::time::timeout(Duration::from_secs(5), client.read_exact(&mut reply))
        .await
        .expect("timed out waiting for reply")
        .unwrap();
    assert_eq!(reply, expected, "got {:?}", String::from_utf8_lossy(&reply));
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

async fn startup_failure(
    tag: u16,
    listen_addr: SocketAddr,
    metrics_addr: SocketAddr,
    config: &str,
) -> std::process::Output {
    let path = std::env::temp_dir().join(format!(
        "rusty-mcrouter-startup-failure-{}-{tag}.json",
        std::process::id()
    ));
    std::fs::write(&path, config).unwrap();
    let output = tokio::time::timeout(
        Duration::from_secs(5),
        Command::new(env!("CARGO_BIN_EXE_rusty-mcrouter"))
            .arg("--config")
            .arg(&path)
            .arg("--listen")
            .arg(listen_addr.to_string())
            .arg("--metrics-addr")
            .arg(metrics_addr.to_string())
            .kill_on_drop(true)
            .output(),
    )
    .await;
    std::fs::remove_file(path).unwrap();
    let output = output
        .expect("router did not stop after startup failure")
        .unwrap();
    assert!(!output.status.success(), "router unexpectedly started");
    assert!(
        output.stdout.is_empty(),
        "startup failure reported readiness"
    );
    output
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
async fn null_route_sums_two_worker_shards() {
    let fx = start_router_with_args(r#"{ "route": "NullRoute" }"#, 60_002, 2, &[]).await;

    exchange(fx.router_addr, b"mg first v\r\n", b"EN\r\n").await;
    exchange(fx.router_addr, b"mg second v\r\n", b"EN\r\n").await;

    let body = scrape(fx.metrics_addr()).await;
    assert_series(&body, "rusty_mcrouter_dev_null_requests_total", &[], 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ctrl_c_stops_worker_and_control_threads_cleanly() {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_bind_failure_is_reported_before_worker_startup() {
    let worker_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let metrics_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let metrics_addr = metrics_listener.local_addr().unwrap();
    let output = startup_failure(
        63_001,
        worker_listener.local_addr().unwrap(),
        metrics_addr,
        r#"{ "route": "NullRoute" }"#,
    )
    .await;
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains(&format!("bind({metrics_addr}) failed")),
        "{stderr}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_bind_failure_stops_the_already_started_control_thread() {
    let worker_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let listen_addr = worker_listener.local_addr().unwrap();
    let output = startup_failure(
        63_002,
        listen_addr,
        "127.0.0.1:0".parse().unwrap(),
        r#"{ "route": "NullRoute" }"#,
    )
    .await;
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains(&format!("bind({listen_addr}) failed")),
        "{stderr}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn route_build_failure_is_reported_and_control_stops() {
    let output = startup_failure(
        63_003,
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.1:0".parse().unwrap(),
        r#"{ "routes": { "/a/b/": "NullRoute" } }"#,
    )
    .await;
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("build_route failed"), "{stderr}");
    assert!(stderr.contains("invalid default route"), "{stderr}");
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
async fn two_worker_shards_sum_into_one_pool_series() {
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

fn single_pool_config(server: SocketAddr) -> String {
    format!(r#"{{ "pools": {{ "p": {{ "servers": ["{server}"] }} }}, "route": "PoolRoute|p" }}"#)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn config_reload_moves_existing_connections_and_rejects_bad_configs() {
    let first = spawn_mock_memcached().await;
    let second = spawn_mock_memcached().await;
    exchange(first, b"ms k 5\r\nfirst\r\n", b"HD\r\n").await;
    exchange(second, b"ms k 6\r\nsecond\r\n", b"HD\r\n").await;

    let router = start_router_with_args(
        &single_pool_config(first),
        first.port(),
        1,
        &["--reconfiguration-delay-ms", "20"],
    )
    .await;
    let metrics = router.metrics_addr();
    let mut client = TcpStream::connect(router.router_addr).await.unwrap();
    roundtrip(&mut client, b"mg k v\r\n", b"VA 5\r\nfirst\r\n").await;

    router.rewrite_config(&single_pool_config(second));
    eventually_series(metrics, "rusty_mcrouter_config_generation", &[], 2).await;
    roundtrip(&mut client, b"mg k v\r\n", b"VA 6\r\nsecond\r\n").await;

    router.rewrite_config("{ not json");
    eventually_series(
        metrics,
        "rusty_mcrouter_config_reload_failures_total",
        &[("stage", "parse")],
        1,
    )
    .await;
    let body = scrape(metrics).await;
    assert_series(
        &body,
        "rusty_mcrouter_config_last_reload_successful",
        &[],
        0,
    );
    assert_series(&body, "rusty_mcrouter_config_generation", &[], 2);
    roundtrip(&mut client, b"mg k v\r\n", b"VA 6\r\nsecond\r\n").await;

    router.rewrite_config(&single_pool_config(second));
    eventually_series(
        metrics,
        "rusty_mcrouter_config_last_reload_successful",
        &[],
        1,
    )
    .await;
    let body = scrape(metrics).await;
    assert_series(&body, "rusty_mcrouter_config_generation", &[], 2);
    // pool "p" survived every reload, so its series never reset
    assert_series(
        &body,
        "rusty_mcrouter_pool_requests_total",
        &[("pool", "p")],
        3,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disabled_reloads_ignore_config_changes() {
    let first = spawn_mock_memcached().await;
    let second = spawn_mock_memcached().await;
    exchange(first, b"ms k 5\r\nfirst\r\n", b"HD\r\n").await;

    let router = start_router_with_args(
        &single_pool_config(first),
        first.port(),
        1,
        &[
            "--disable-reload-configs",
            "--reconfiguration-delay-ms",
            "20",
        ],
    )
    .await;
    router.rewrite_config(&single_pool_config(second));
    tokio::time::sleep(Duration::from_millis(200)).await;

    let body = scrape(router.metrics_addr()).await;
    assert_series(&body, "rusty_mcrouter_config_generation", &[], 1);
    assert_series(&body, "rusty_mcrouter_config_reload_attempts_total", &[], 0);
    exchange(router.router_addr, b"mg k v\r\n", b"VA 5\r\nfirst\r\n").await;
}
