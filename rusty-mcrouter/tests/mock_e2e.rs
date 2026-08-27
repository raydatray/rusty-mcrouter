use std::time::Duration;

use rusty_mcrouter_backend::mock_memcached::{spawn_failing_mock_memcached, spawn_mock_memcached};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::process::Command;

mod support;

use support::{assert_series, exchange, scrape, RouterProcess};

type Stack = RouterProcess;

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
async fn get_seeded_key_returns_value() {
    let fx = start_stack().await;
    exchange(fx.router_addr, b"mg seeded_foo v\r\n", b"VA 3\r\nbar\r\n").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_missing_key_returns_miss() {
    let fx = start_stack().await;
    exchange(fx.router_addr, b"mg mock_e2e_missing v\r\n", b"EN\r\n").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn store_then_get_round_trip() {
    let fx = start_stack().await;
    exchange(fx.router_addr, b"ms me2e_k 5 F9\r\nworld\r\n", b"HD\r\n").await;
    exchange(
        fx.router_addr,
        b"mg me2e_k v f s\r\n",
        b"VA 5 f9 s5\r\nworld\r\n",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn store_delete_get_round_trip() {
    let fx = start_stack().await;
    exchange(fx.router_addr, b"ms me2e_d 1\r\nx\r\n", b"HD\r\n").await;
    exchange(fx.router_addr, b"md me2e_d\r\n", b"HD\r\n").await;
    exchange(fx.router_addr, b"mg me2e_d v\r\n", b"EN\r\n").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn arithmetic_returns_new_value() {
    let fx = start_stack().await;
    exchange(fx.router_addr, b"ms me2e_n 2\r\n42\r\n", b"HD\r\n").await;
    exchange(fx.router_addr, b"ma me2e_n v\r\n", b"VA 2\r\n43\r\n").await;
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

    let body = scrape(fx.metrics_addr).await;
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

    let body = scrape(fx.metrics_addr).await;
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

    let body = scrape(fx.metrics_addr).await;
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

        let body = scrape(fx.metrics_addr).await;
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

    let body = scrape(stack.metrics_addr).await;
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

    let body = scrape(stack.metrics_addr).await;
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
