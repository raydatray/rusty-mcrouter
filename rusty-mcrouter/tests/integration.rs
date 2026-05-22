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
            // execution.
            let mut conn = wait_for_tcp(backend_addr, Duration::from_secs(5)).await;
            conn.write_all(b"set seeded_foo 0 0 3\r\nbar\r\n")
                .await
                .unwrap();
            let mut buf = vec![0u8; 64];
            let n = conn.read(&mut buf).await.unwrap();
            assert_eq!(&buf[..n], b"STORED\r\n");
            drop(conn);

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
                .env("RUSTY_MCROUTER_BACKEND", backend_addr.to_string())
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

async fn round_trip(addr: SocketAddr, request: &[u8]) -> Vec<u8> {
    let mut conn = TcpStream::connect(addr).await.unwrap();
    conn.write_all(request).await.unwrap();
    let mut buf = vec![0u8; 256];
    let n = conn.read(&mut buf).await.unwrap();
    buf.truncate(n);
    buf
}

#[tokio::test]
#[ignore = "requires Docker; run with `cargo test --test integration -- --ignored`"]
async fn get_seeded_key_returns_value() {
    let fx = fixture().await;
    let resp = round_trip(fx.router_addr, b"get seeded_foo\r\n").await;
    assert_eq!(resp, b"VALUE seeded_foo 0 3\r\nbar\r\nEND\r\n");
}

#[tokio::test]
#[ignore = "requires Docker; run with `cargo test --test integration -- --ignored`"]
async fn get_missing_key_returns_end() {
    let fx = fixture().await;
    let resp = round_trip(fx.router_addr, b"get get_missing_key\r\n").await;
    assert_eq!(resp, b"END\r\n");
}

#[tokio::test]
#[ignore = "requires Docker; run with `cargo test --test integration -- --ignored`"]
async fn get_multi_key_returns_only_hits() {
    let fx = fixture().await;
    let resp = round_trip(fx.router_addr, b"get seeded_foo get_multi_key_miss\r\n").await;
    assert_eq!(resp, b"VALUE seeded_foo 0 3\r\nbar\r\nEND\r\n");
}

#[tokio::test]
#[ignore = "requires Docker; run with `cargo test --test integration -- --ignored`"]
async fn set_returns_stored() {
    let fx = fixture().await;
    let resp = round_trip(
        fx.router_addr,
        b"set set_returns_stored_key 7 0 5\r\nhello\r\n",
    )
    .await;
    assert_eq!(resp, b"STORED\r\n");
}

#[tokio::test]
#[ignore = "requires Docker; run with `cargo test --test integration -- --ignored`"]
async fn set_then_get_round_trip() {
    let fx = fixture().await;
    let stored = round_trip(fx.router_addr, b"set set_then_get_key 9 0 5\r\nworld\r\n").await;
    assert_eq!(stored, b"STORED\r\n");

    let fetched = round_trip(fx.router_addr, b"get set_then_get_key\r\n").await;
    assert_eq!(fetched, b"VALUE set_then_get_key 9 5\r\nworld\r\nEND\r\n");
}

#[tokio::test]
#[ignore = "requires Docker; run with `cargo test --test integration -- --ignored`"]
async fn set_overwrites_existing_value() {
    let fx = fixture().await;
    let initial = round_trip(fx.router_addr, b"set set_overwrites_key 0 0 3\r\nbar\r\n").await;
    assert_eq!(initial, b"STORED\r\n");

    let updated = round_trip(
        fx.router_addr,
        b"set set_overwrites_key 0 0 7\r\nupdated\r\n",
    )
    .await;
    assert_eq!(updated, b"STORED\r\n");

    let fetched = round_trip(fx.router_addr, b"get set_overwrites_key\r\n").await;
    assert_eq!(
        fetched,
        b"VALUE set_overwrites_key 0 7\r\nupdated\r\nEND\r\n"
    );
}

#[tokio::test]
#[ignore = "requires Docker; run with `cargo test --test integration -- --ignored`"]
async fn delete_existing_key_returns_deleted() {
    let fx = fixture().await;
    let stored = round_trip(
        fx.router_addr,
        b"set delete_existing_key 0 0 3\r\nbar\r\n",
    )
    .await;
    assert_eq!(stored, b"STORED\r\n");

    let deleted = round_trip(fx.router_addr, b"delete delete_existing_key\r\n").await;
    assert_eq!(deleted, b"DELETED\r\n");
}

#[tokio::test]
#[ignore = "requires Docker; run with `cargo test --test integration -- --ignored`"]
async fn delete_missing_key_returns_not_found() {
    let fx = fixture().await;
    let resp = round_trip(fx.router_addr, b"delete delete_missing_key\r\n").await;
    assert_eq!(resp, b"NOT_FOUND\r\n");
}

#[tokio::test]
#[ignore = "requires Docker; run with `cargo test --test integration -- --ignored`"]
async fn set_delete_get_round_trip() {
    let fx = fixture().await;
    let stored = round_trip(fx.router_addr, b"set set_delete_get_key 0 0 5\r\nhello\r\n").await;
    assert_eq!(stored, b"STORED\r\n");

    let deleted = round_trip(fx.router_addr, b"delete set_delete_get_key\r\n").await;
    assert_eq!(deleted, b"DELETED\r\n");

    let fetched = round_trip(fx.router_addr, b"get set_delete_get_key\r\n").await;
    assert_eq!(fetched, b"END\r\n");
}

#[tokio::test]
#[ignore = "requires Docker; run with `cargo test --test integration -- --ignored`"]
async fn add_new_key_returns_stored() {
    let fx = fixture().await;
    let resp = round_trip(fx.router_addr, b"add add_new_key 0 0 5\r\nhello\r\n").await;
    assert_eq!(resp, b"STORED\r\n");
}

#[tokio::test]
#[ignore = "requires Docker; run with `cargo test --test integration -- --ignored`"]
async fn add_existing_key_returns_not_stored() {
    let fx = fixture().await;
    let stored = round_trip(fx.router_addr, b"set add_existing_key 0 0 5\r\nfirst\r\n").await;
    assert_eq!(stored, b"STORED\r\n");

    let resp = round_trip(fx.router_addr, b"add add_existing_key 0 0 6\r\nsecond\r\n").await;
    assert_eq!(resp, b"NOT_STORED\r\n");
}

#[tokio::test]
#[ignore = "requires Docker; run with `cargo test --test integration -- --ignored`"]
async fn add_then_get_round_trip() {
    let fx = fixture().await;
    let stored = round_trip(fx.router_addr, b"add add_then_get_key 7 0 5\r\nworld\r\n").await;
    assert_eq!(stored, b"STORED\r\n");

    let fetched = round_trip(fx.router_addr, b"get add_then_get_key\r\n").await;
    assert_eq!(fetched, b"VALUE add_then_get_key 7 5\r\nworld\r\nEND\r\n");
}

#[tokio::test]
#[ignore = "requires Docker; run with `cargo test --test integration -- --ignored`"]
async fn replace_missing_key_returns_not_stored() {
    let fx = fixture().await;
    let resp = round_trip(fx.router_addr, b"replace replace_missing_key 0 0 5\r\nhello\r\n").await;
    assert_eq!(resp, b"NOT_STORED\r\n");
}

#[tokio::test]
#[ignore = "requires Docker; run with `cargo test --test integration -- --ignored`"]
async fn replace_existing_key_returns_stored() {
    let fx = fixture().await;
    let stored = round_trip(
        fx.router_addr,
        b"set replace_existing_key 0 0 5\r\nfirst\r\n",
    )
    .await;
    assert_eq!(stored, b"STORED\r\n");

    let resp = round_trip(
        fx.router_addr,
        b"replace replace_existing_key 0 0 6\r\nsecond\r\n",
    )
    .await;
    assert_eq!(resp, b"STORED\r\n");
}

#[tokio::test]
#[ignore = "requires Docker; run with `cargo test --test integration -- --ignored`"]
async fn replace_changes_value() {
    let fx = fixture().await;
    let stored = round_trip(fx.router_addr, b"set replace_changes_key 0 0 5\r\nfirst\r\n").await;
    assert_eq!(stored, b"STORED\r\n");

    let replaced = round_trip(
        fx.router_addr,
        b"replace replace_changes_key 0 0 6\r\nsecond\r\n",
    )
    .await;
    assert_eq!(replaced, b"STORED\r\n");

    let fetched = round_trip(fx.router_addr, b"get replace_changes_key\r\n").await;
    assert_eq!(
        fetched,
        b"VALUE replace_changes_key 0 6\r\nsecond\r\nEND\r\n"
    );
}

#[tokio::test]
#[ignore = "requires Docker; run with `cargo test --test integration -- --ignored`"]
async fn append_to_missing_key_returns_not_stored() {
    let fx = fixture().await;
    let resp = round_trip(fx.router_addr, b"append append_missing_key 0 0 3\r\nbar\r\n").await;
    assert_eq!(resp, b"NOT_STORED\r\n");
}

#[tokio::test]
#[ignore = "requires Docker; run with `cargo test --test integration -- --ignored`"]
async fn append_extends_existing_value() {
    let fx = fixture().await;
    let stored = round_trip(fx.router_addr, b"set append_extends_key 0 0 5\r\nhello\r\n").await;
    assert_eq!(stored, b"STORED\r\n");

    let appended = round_trip(
        fx.router_addr,
        b"append append_extends_key 0 0 6\r\n world\r\n",
    )
    .await;
    assert_eq!(appended, b"STORED\r\n");

    let fetched = round_trip(fx.router_addr, b"get append_extends_key\r\n").await;
    assert_eq!(
        fetched,
        b"VALUE append_extends_key 0 11\r\nhello world\r\nEND\r\n"
    );
}

#[tokio::test]
#[ignore = "requires Docker; run with `cargo test --test integration -- --ignored`"]
async fn append_keeps_original_flags_when_command_specifies_different_flags() {
    let fx = fixture().await;
    let stored = round_trip(fx.router_addr, b"set append_ignores_key 7 0 5\r\nhello\r\n").await;
    assert_eq!(stored, b"STORED\r\n");

    let appended = round_trip(
        fx.router_addr,
        b"append append_ignores_key 999 999 6\r\n world\r\n",
    )
    .await;
    assert_eq!(appended, b"STORED\r\n");

    let fetched = round_trip(fx.router_addr, b"get append_ignores_key\r\n").await;
    assert_eq!(
        fetched,
        b"VALUE append_ignores_key 7 11\r\nhello world\r\nEND\r\n"
    );
}

#[tokio::test]
#[ignore = "requires Docker; run with `cargo test --test integration -- --ignored`"]
async fn prepend_to_missing_key_returns_not_stored() {
    let fx = fixture().await;
    let resp = round_trip(fx.router_addr, b"prepend prepend_missing_key 0 0 3\r\nbar\r\n").await;
    assert_eq!(resp, b"NOT_STORED\r\n");
}

#[tokio::test]
#[ignore = "requires Docker; run with `cargo test --test integration -- --ignored`"]
async fn prepend_extends_existing_value() {
    let fx = fixture().await;
    let stored = round_trip(fx.router_addr, b"set prepend_extends_key 0 0 5\r\nworld\r\n").await;
    assert_eq!(stored, b"STORED\r\n");

    let prepended = round_trip(
        fx.router_addr,
        b"prepend prepend_extends_key 0 0 6\r\nhello \r\n",
    )
    .await;
    assert_eq!(prepended, b"STORED\r\n");

    let fetched = round_trip(fx.router_addr, b"get prepend_extends_key\r\n").await;
    assert_eq!(
        fetched,
        b"VALUE prepend_extends_key 0 11\r\nhello world\r\nEND\r\n"
    );
}

#[tokio::test]
#[ignore = "requires Docker; run with `cargo test --test integration -- --ignored`"]
async fn prepend_keeps_original_flags_when_command_specifies_different_flags() {
    let fx = fixture().await;
    let stored = round_trip(fx.router_addr, b"set prepend_ignores_key 7 0 5\r\nworld\r\n").await;
    assert_eq!(stored, b"STORED\r\n");

    let prepended = round_trip(
        fx.router_addr,
        b"prepend prepend_ignores_key 999 999 6\r\nhello \r\n",
    )
    .await;
    assert_eq!(prepended, b"STORED\r\n");

    let fetched = round_trip(fx.router_addr, b"get prepend_ignores_key\r\n").await;
    assert_eq!(
        fetched,
        b"VALUE prepend_ignores_key 7 11\r\nhello world\r\nEND\r\n"
    );
}

#[tokio::test]
#[ignore = "requires Docker; run with `cargo test --test integration -- --ignored`"]
async fn incr_missing_key_returns_not_found() {
    let fx = fixture().await;
    let resp = round_trip(fx.router_addr, b"incr incr_missing_key 1\r\n").await;
    assert_eq!(resp, b"NOT_FOUND\r\n");
}

#[tokio::test]
#[ignore = "requires Docker; run with `cargo test --test integration -- --ignored`"]
async fn incr_existing_numeric_returns_new_value() {
    let fx = fixture().await;
    let stored = round_trip(fx.router_addr, b"set incr_existing_key 0 0 2\r\n42\r\n").await;
    assert_eq!(stored, b"STORED\r\n");

    let resp = round_trip(fx.router_addr, b"incr incr_existing_key 1\r\n").await;
    assert_eq!(resp, b"43\r\n");
}

#[tokio::test]
#[ignore = "requires Docker; run with `cargo test --test integration -- --ignored`"]
async fn incr_by_delta_increments_value() {
    let fx = fixture().await;
    let stored = round_trip(fx.router_addr, b"set incr_by_delta_key 0 0 1\r\n5\r\n").await;
    assert_eq!(stored, b"STORED\r\n");

    let resp = round_trip(fx.router_addr, b"incr incr_by_delta_key 100\r\n").await;
    assert_eq!(resp, b"105\r\n");

    let fetched = round_trip(fx.router_addr, b"get incr_by_delta_key\r\n").await;
    assert_eq!(fetched, b"VALUE incr_by_delta_key 0 3\r\n105\r\nEND\r\n");
}
