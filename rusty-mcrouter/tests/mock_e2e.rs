use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::Stdio;

use rusty_mcrouter_net::mock_memcached::spawn_mock_memcached;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::process::{Child, Command};

struct Stack {
    router_addr: SocketAddr,
    _router: Child,
    _config_path: PathBuf,
}

async fn start_stack() -> Stack {
    let backend_addr = spawn_mock_memcached().await;

    let mut seed = TcpStream::connect(backend_addr).await.unwrap();
    seed.write_all(b"set seeded_foo 0 0 3\r\nbar\r\n").await.unwrap();
    let mut buf = [0u8; 32];
    let n = seed.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"STORED\r\n");
    drop(seed);

    let config_path = std::env::temp_dir().join(format!(
        "rusty-mcrouter-mock-e2e-{}.json",
        backend_addr.port()
    ));
    let config_body = format!(
        r#"{{ "pools": {{ "memcached": {{ "servers": ["{}"] }} }}, "route": "PoolRoute|memcached" }}"#,
        backend_addr
    );
    std::fs::write(&config_path, &config_body).unwrap();

    let mut router = Command::new(env!("CARGO_BIN_EXE_rusty-mcrouter"))
        .arg("--config")
        .arg(&config_path)
        .arg("--num-proxies")
        .arg("1")
        .env("RUSTY_MCROUTER_LISTEN", "127.0.0.1:0")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();

    let stdout = router.stdout.take().unwrap();
    let mut lines = BufReader::new(stdout).lines();
    let ready = lines
        .next_line()
        .await
        .unwrap()
        .expect("eof before READY line");
    let router_addr: SocketAddr = ready
        .strip_prefix("READY ")
        .expect("expected READY prefix on stdout")
        .parse()
        .unwrap();

    Stack {
        router_addr,
        _router: router,
        _config_path: config_path,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_seeded_key_returns_value() {
    let fx = start_stack().await;
    let resp = round_trip(fx.router_addr, b"get seeded_foo\r\n").await;
    assert_eq!(resp, b"VALUE seeded_foo 0 3\r\nbar\r\nEND\r\n");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_missing_key_returns_end() {
    let fx = start_stack().await;
    let resp = round_trip(fx.router_addr, b"get mock_e2e_missing\r\n").await;
    assert_eq!(resp, b"END\r\n");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn set_then_get_round_trip() {
    let fx = start_stack().await;
    assert_eq!(
        round_trip(fx.router_addr, b"set me2e_k 9 0 5\r\nworld\r\n").await,
        b"STORED\r\n"
    );
    assert_eq!(
        round_trip(fx.router_addr, b"get me2e_k\r\n").await,
        b"VALUE me2e_k 9 5\r\nworld\r\nEND\r\n"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn set_delete_get_round_trip() {
    let fx = start_stack().await;
    assert_eq!(
        round_trip(fx.router_addr, b"set me2e_d 0 0 1\r\nx\r\n").await,
        b"STORED\r\n"
    );
    assert_eq!(
        round_trip(fx.router_addr, b"delete me2e_d\r\n").await,
        b"DELETED\r\n"
    );
    assert_eq!(round_trip(fx.router_addr, b"get me2e_d\r\n").await, b"END\r\n");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incr_returns_new_value() {
    let fx = start_stack().await;
    assert_eq!(
        round_trip(fx.router_addr, b"set me2e_n 0 0 2\r\n42\r\n").await,
        b"STORED\r\n"
    );
    assert_eq!(round_trip(fx.router_addr, b"incr me2e_n 1\r\n").await, b"43\r\n");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multiget_returns_only_hits() {
    let fx = start_stack().await;
    let resp = round_trip(fx.router_addr, b"get seeded_foo mock_e2e_multi_miss\r\n").await;
    assert_eq!(resp, b"VALUE seeded_foo 0 3\r\nbar\r\nEND\r\n");
}
