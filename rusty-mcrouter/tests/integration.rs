//! End-to-end tests: real memcached (Docker) behind the real binary,
//! speaking the Meta protocol on both hops.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use testcontainers::{core::IntoContainerPort, runners::AsyncRunner, ContainerAsync, GenericImage};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::process::{Child, Command};
use tokio::sync::OnceCell;
use tokio::time::Instant;

struct Fixture {
    router_addr: SocketAddr,
    // Held to keep the container/process alive for the entire test binary's
    // lifetime. Tests share one backend + one router instance via OnceCell.
    _backend: ContainerAsync<GenericImage>,
    _router: Child,
    _config_path: PathBuf,
}

static FIXTURE: OnceCell<Fixture> = OnceCell::const_new();

async fn fixture() -> &'static Fixture {
    FIXTURE
        .get_or_init(|| async {
            let backend = GenericImage::new("memcached", "1.6")
                .with_exposed_port(11211.tcp())
                .start()
                .await
                .expect("docker start failed (is Docker running?)");

            let backend_port = backend
                .get_host_port_ipv4(11211)
                .await
                .expect("get backend port");
            let backend_addr: SocketAddr = format!("127.0.0.1:{}", backend_port).parse().unwrap();

            // Pre-seed a read-only key. Tests that need fresh writes use their
            // own per-test key namespace so they're safe under parallel
            // execution. The seed retries: docker's port proxy accepts TCP
            // before memcached inside the container listens, so early
            // connections can reset.
            let deadline = Instant::now() + Duration::from_secs(10);
            loop {
                let mut conn = wait_for_tcp(backend_addr, Duration::from_secs(5)).await;
                let seeded: std::io::Result<bool> = async {
                    conn.write_all(b"ms seeded_foo 3\r\nbar\r\n").await?;
                    let mut buf = [0u8; 64];
                    let n = conn.read(&mut buf).await?;
                    Ok(&buf[..n] == b"HD\r\n")
                }
                .await;
                match seeded {
                    Ok(true) => break,
                    Ok(_) | Err(_) if Instant::now() < deadline => {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    other => panic!("seeding backend failed: {other:?}"),
                }
            }

            let config_path = std::env::temp_dir().join(format!(
                "rusty-mcrouter-integration-{}.json",
                std::process::id()
            ));
            let config_body = format!(
                r#"{{ "pools": {{ "memcached": {{ "servers": ["{}"] }} }}, "route": "PoolRoute|memcached" }}"#,
                backend_addr
            );
            std::fs::write(&config_path, &config_body).expect("write config file");

            let mut router = Command::new(env!("CARGO_BIN_EXE_rusty-mcrouter"))
                .arg("--config")
                .arg(&config_path)
                .env("RUSTY_MCROUTER_LISTEN", "127.0.0.1:0")
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .kill_on_drop(true)
                .spawn()
                .expect("spawn binary");

            let stdout = router.stdout.take().expect("stdout pipe");
            let mut lines = BufReader::new(stdout).lines();
            let ready = lines
                .next_line()
                .await
                .expect("read line")
                .expect("eof before READY line");
            let router_addr: SocketAddr = ready
                .strip_prefix("READY ")
                .expect("expected READY prefix on stdout")
                .parse()
                .expect("parse router addr");

            Fixture {
                router_addr,
                _backend: backend,
                _router: router,
                _config_path: config_path,
            }
        })
        .await
}

async fn wait_for_tcp(addr: SocketAddr, timeout: Duration) -> TcpStream {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(s) = TcpStream::connect(addr).await {
            return s;
        }
        if Instant::now() > deadline {
            panic!("connect to {} timed out", addr);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Writes `request` on a fresh connection and asserts it yields exactly
/// `expected`, reassembling partial reads until the expected length arrives.
async fn exchange(addr: SocketAddr, request: &[u8], expected: &[u8]) {
    let mut conn = TcpStream::connect(addr).await.unwrap();
    conn.write_all(request).await.unwrap();

    let mut received = Vec::with_capacity(expected.len());
    let read = tokio::time::timeout(Duration::from_secs(5), async {
        let mut chunk = [0u8; 4096];
        while received.len() < expected.len() {
            let n = conn.read(&mut chunk).await.unwrap();
            if n == 0 {
                break;
            }
            received.extend_from_slice(&chunk[..n]);
        }
    });
    read.await.expect("timed out waiting for reply bytes");
    assert_eq!(
        received,
        expected,
        "request {:?}: got {:?}",
        String::from_utf8_lossy(request),
        String::from_utf8_lossy(&received),
    );
}

macro_rules! docker_test {
    ($(#[$meta:meta])* async fn $name:ident() $body:block) => {
        #[tokio::test]
        #[ignore = "requires Docker; run with `cargo test --test integration -- --ignored`"]
        $(#[$meta])*
        async fn $name() $body
    };
}

docker_test! {
    async fn get_seeded_key_returns_value() {
        let fx = fixture().await;
        exchange(fx.router_addr, b"mg seeded_foo v\r\n", b"VA 3\r\nbar\r\n").await;
    }
}

docker_test! {
    async fn get_missing_key_returns_miss() {
        let fx = fixture().await;
        exchange(fx.router_addr, b"mg get_missing_key v\r\n", b"EN\r\n").await;
    }
}

docker_test! {
    async fn pipelined_quiet_gets_with_noop_fence_replace_multiget() {
        let fx = fixture().await;
        exchange(
            fx.router_addr,
            b"mg seeded_foo v q Ofirst\r\nmg get_multi_key_miss v q Osecond\r\nmn\r\n",
            b"VA 3 Ofirst\r\nbar\r\nMN\r\n",
        )
        .await;
    }
}

docker_test! {
    async fn store_returns_success_with_projections() {
        let fx = fixture().await;
        exchange(
            fx.router_addr,
            b"ms set_returns_stored_key 5 F7 s\r\nhello\r\n",
            b"HD s5\r\n",
        )
        .await;
    }
}

docker_test! {
    async fn store_then_get_round_trip() {
        let fx = fixture().await;
        exchange(
            fx.router_addr,
            b"ms set_then_get_key 5 F9\r\nworld\r\n",
            b"HD\r\n",
        )
        .await;
        exchange(
            fx.router_addr,
            b"mg set_then_get_key v f s\r\n",
            b"VA 5 f9 s5\r\nworld\r\n",
        )
        .await;
    }
}

docker_test! {
    async fn store_overwrites_existing_value() {
        let fx = fixture().await;
        exchange(fx.router_addr, b"ms set_overwrites_key 3\r\nbar\r\n", b"HD\r\n").await;
        exchange(
            fx.router_addr,
            b"ms set_overwrites_key 7\r\nupdated\r\n",
            b"HD\r\n",
        )
        .await;
        exchange(
            fx.router_addr,
            b"mg set_overwrites_key v\r\n",
            b"VA 7\r\nupdated\r\n",
        )
        .await;
    }
}

docker_test! {
    async fn delete_existing_key_returns_success() {
        let fx = fixture().await;
        exchange(fx.router_addr, b"ms delete_existing_key 3\r\nbar\r\n", b"HD\r\n").await;
        exchange(fx.router_addr, b"md delete_existing_key\r\n", b"HD\r\n").await;
    }
}

docker_test! {
    async fn delete_missing_key_returns_not_found() {
        let fx = fixture().await;
        exchange(fx.router_addr, b"md delete_missing_key\r\n", b"NF\r\n").await;
    }
}

docker_test! {
    async fn store_delete_get_round_trip() {
        let fx = fixture().await;
        exchange(fx.router_addr, b"ms set_delete_get_key 5\r\nhello\r\n", b"HD\r\n").await;
        exchange(fx.router_addr, b"md set_delete_get_key\r\n", b"HD\r\n").await;
        exchange(fx.router_addr, b"mg set_delete_get_key v\r\n", b"EN\r\n").await;
    }
}

docker_test! {
    async fn add_new_key_returns_success() {
        let fx = fixture().await;
        exchange(fx.router_addr, b"ms add_new_key 5 ME\r\nhello\r\n", b"HD\r\n").await;
    }
}

docker_test! {
    async fn add_existing_key_returns_not_stored() {
        let fx = fixture().await;
        exchange(fx.router_addr, b"ms add_existing_key 5\r\nfirst\r\n", b"HD\r\n").await;
        exchange(
            fx.router_addr,
            b"ms add_existing_key 6 ME\r\nsecond\r\n",
            b"NS\r\n",
        )
        .await;
    }
}

docker_test! {
    async fn add_then_get_round_trip() {
        let fx = fixture().await;
        exchange(
            fx.router_addr,
            b"ms add_then_get_key 5 ME F7\r\nworld\r\n",
            b"HD\r\n",
        )
        .await;
        exchange(
            fx.router_addr,
            b"mg add_then_get_key v f\r\n",
            b"VA 5 f7\r\nworld\r\n",
        )
        .await;
    }
}

docker_test! {
    async fn replace_missing_key_returns_not_stored() {
        let fx = fixture().await;
        exchange(
            fx.router_addr,
            b"ms replace_missing_key 5 MR\r\nhello\r\n",
            b"NS\r\n",
        )
        .await;
    }
}

docker_test! {
    async fn replace_existing_key_returns_success() {
        let fx = fixture().await;
        exchange(
            fx.router_addr,
            b"ms replace_existing_key 5\r\nfirst\r\n",
            b"HD\r\n",
        )
        .await;
        exchange(
            fx.router_addr,
            b"ms replace_existing_key 6 MR\r\nsecond\r\n",
            b"HD\r\n",
        )
        .await;
    }
}

docker_test! {
    async fn replace_changes_value() {
        let fx = fixture().await;
        exchange(fx.router_addr, b"ms replace_changes_key 5\r\nfirst\r\n", b"HD\r\n").await;
        exchange(
            fx.router_addr,
            b"ms replace_changes_key 6 MR\r\nsecond\r\n",
            b"HD\r\n",
        )
        .await;
        exchange(
            fx.router_addr,
            b"mg replace_changes_key v\r\n",
            b"VA 6\r\nsecond\r\n",
        )
        .await;
    }
}

docker_test! {
    async fn append_to_missing_key_returns_not_stored() {
        let fx = fixture().await;
        exchange(
            fx.router_addr,
            b"ms append_missing_key 3 MA\r\nbar\r\n",
            b"NS\r\n",
        )
        .await;
    }
}

docker_test! {
    async fn append_extends_existing_value() {
        let fx = fixture().await;
        exchange(fx.router_addr, b"ms append_extends_key 5\r\nhello\r\n", b"HD\r\n").await;
        exchange(
            fx.router_addr,
            b"ms append_extends_key 6 MA\r\n world\r\n",
            b"HD\r\n",
        )
        .await;
        exchange(
            fx.router_addr,
            b"mg append_extends_key v s\r\n",
            b"VA 11 s11\r\nhello world\r\n",
        )
        .await;
    }
}

docker_test! {
    async fn append_keeps_original_client_flags() {
        let fx = fixture().await;
        exchange(fx.router_addr, b"ms append_ignores_key 5 F7\r\nhello\r\n", b"HD\r\n").await;
        exchange(
            fx.router_addr,
            b"ms append_ignores_key 6 MA F999\r\n world\r\n",
            b"HD\r\n",
        )
        .await;
        exchange(
            fx.router_addr,
            b"mg append_ignores_key v f\r\n",
            b"VA 11 f7\r\nhello world\r\n",
        )
        .await;
    }
}

docker_test! {
    async fn prepend_to_missing_key_returns_not_stored() {
        let fx = fixture().await;
        exchange(
            fx.router_addr,
            b"ms prepend_missing_key 3 MP\r\nbar\r\n",
            b"NS\r\n",
        )
        .await;
    }
}

docker_test! {
    async fn prepend_extends_existing_value() {
        let fx = fixture().await;
        exchange(fx.router_addr, b"ms prepend_extends_key 5\r\nworld\r\n", b"HD\r\n").await;
        exchange(
            fx.router_addr,
            b"ms prepend_extends_key 6 MP\r\nhello \r\n",
            b"HD\r\n",
        )
        .await;
        exchange(
            fx.router_addr,
            b"mg prepend_extends_key v\r\n",
            b"VA 11\r\nhello world\r\n",
        )
        .await;
    }
}

docker_test! {
    async fn append_miss_with_vivify_seeds_item() {
        let fx = fixture().await;
        exchange(
            fx.router_addr,
            b"ms append_vivify_key 3 MA N60\r\nnew\r\n",
            b"HD\r\n",
        )
        .await;
        exchange(
            fx.router_addr,
            b"mg append_vivify_key v\r\n",
            b"VA 3\r\nnew\r\n",
        )
        .await;
    }
}

docker_test! {
    async fn arithmetic_missing_key_returns_not_found() {
        let fx = fixture().await;
        exchange(fx.router_addr, b"ma incr_missing_key\r\n", b"NF\r\n").await;
    }
}

docker_test! {
    async fn arithmetic_incr_returns_new_value() {
        let fx = fixture().await;
        exchange(fx.router_addr, b"ms incr_existing_key 2\r\n42\r\n", b"HD\r\n").await;
        exchange(fx.router_addr, b"ma incr_existing_key v\r\n", b"VA 2\r\n43\r\n").await;
    }
}

docker_test! {
    async fn arithmetic_incr_by_delta() {
        let fx = fixture().await;
        exchange(fx.router_addr, b"ms incr_by_delta_key 1\r\n5\r\n", b"HD\r\n").await;
        exchange(
            fx.router_addr,
            b"ma incr_by_delta_key v D100\r\n",
            b"VA 3\r\n105\r\n",
        )
        .await;
        exchange(
            fx.router_addr,
            b"mg incr_by_delta_key v\r\n",
            b"VA 3\r\n105\r\n",
        )
        .await;
    }
}

docker_test! {
    async fn arithmetic_decr_returns_new_value() {
        let fx = fixture().await;
        exchange(fx.router_addr, b"ms decr_existing_key 2\r\n42\r\n", b"HD\r\n").await;
        exchange(
            fx.router_addr,
            b"ma decr_existing_key v MD D5\r\n",
            b"VA 2\r\n37\r\n",
        )
        .await;
    }
}

docker_test! {
    async fn arithmetic_decr_clamps_at_zero() {
        let fx = fixture().await;
        exchange(fx.router_addr, b"ms decr_underflow_key 1\r\n5\r\n", b"HD\r\n").await;
        exchange(
            fx.router_addr,
            b"ma decr_underflow_key v MD D100\r\n",
            b"VA 1\r\n0\r\n",
        )
        .await;
    }
}

docker_test! {
    async fn arithmetic_vivify_seeds_initial_value() {
        let fx = fixture().await;
        exchange(
            fx.router_addr,
            b"ma vivify_counter_key N60 J5 v\r\n",
            b"VA 1\r\n5\r\n",
        )
        .await;
    }
}

docker_test! {
    async fn touch_missing_key_returns_miss() {
        let fx = fixture().await;
        // "touch" in meta is mg with a TTL update and no value.
        exchange(fx.router_addr, b"mg touch_missing_key T60\r\n", b"EN\r\n").await;
    }
}

docker_test! {
    async fn touch_existing_key_returns_header() {
        let fx = fixture().await;
        exchange(fx.router_addr, b"ms touch_existing_key 5\r\nhello\r\n", b"HD\r\n").await;
        exchange(fx.router_addr, b"mg touch_existing_key T60\r\n", b"HD\r\n").await;
    }
}

docker_test! {
    async fn touch_updates_ttl_and_preserves_value_and_flags() {
        let fx = fixture().await;
        exchange(
            fx.router_addr,
            b"ms touch_preserves_key 5 F42 T500\r\nhello\r\n",
            b"HD\r\n",
        )
        .await;
        // update-then-read: t must observe the new TTL (temporal ordering).
        exchange(
            fx.router_addr,
            b"mg touch_preserves_key T3600 t\r\n",
            b"HD t3600\r\n",
        )
        .await;
        exchange(
            fx.router_addr,
            b"mg touch_preserves_key v f\r\n",
            b"VA 5 f42\r\nhello\r\n",
        )
        .await;
    }
}

docker_test! {
    async fn recoverable_parse_error_keeps_pipeline_order_and_connection() {
        let fx = fixture().await;
        exchange(
            fx.router_addr,
            b"mg seeded_foo v\r\nmg seeded_foo zz\r\nmg seeded_foo v\r\n",
            b"VA 3\r\nbar\r\nCLIENT_ERROR invalid flag\r\nVA 3\r\nbar\r\n",
        )
        .await;
    }
}

docker_test! {
    async fn opaque_and_key_echo_survive_the_hop() {
        let fx = fixture().await;
        exchange(
            fx.router_addr,
            b"ms echo_key 2 s k Otag\r\nhi\r\n",
            b"HD s2 kecho_key Otag\r\n",
        )
        .await;
        exchange(
            fx.router_addr,
            b"mg echo_key v k Oget\r\n",
            b"VA 2 kecho_key Oget\r\nhi\r\n",
        )
        .await;
    }
}

docker_test! {
    async fn quiet_store_suppresses_success_but_not_failure() {
        let fx = fixture().await;
        exchange(
            fx.router_addr,
            b"ms quiet_store_key 2 q\r\nhi\r\nms quiet_store_key 2 q ME\r\nhi\r\nmn\r\n",
            b"NS\r\nMN\r\n",
        )
        .await;
    }
}

docker_test! {
    async fn debug_command_round_trips() {
        let fx = fixture().await;
        exchange(fx.router_addr, b"me debug_missing_key\r\n", b"EN\r\n").await;
    }
}
