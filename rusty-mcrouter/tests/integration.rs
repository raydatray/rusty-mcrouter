use std::net::SocketAddr;
use std::process::Stdio;
use std::time::Duration;
use testcontainers::{core::IntoContainerPort, runners::AsyncRunner, GenericImage};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::process::Command;
use tokio::time::Instant;

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

#[tokio::test]
#[ignore = "requires Docker; run with `cargo test --test integration -- --ignored`"]
async fn end_to_end_get_round_trip_against_real_memcached() {
    let memcached = GenericImage::new("memcached", "1.6")
        .with_exposed_port(11211.tcp())
        .start()
        .await
        .expect("docker start failed (is Docker running?)");

    let backend_port = memcached
        .get_host_port_ipv4(11211)
        .await
        .expect("get backend port");
    let backend_addr: SocketAddr = format!("127.0.0.1:{}", backend_port).parse().unwrap();

    let mut backend = wait_for_tcp(backend_addr, Duration::from_secs(5)).await;
    backend.write_all(b"set foo 0 0 3\r\nbar\r\n").await.unwrap();
    let mut buf = vec![0u8; 256];
    let n = backend.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"STORED\r\n");
    drop(backend);

    let mut child = Command::new(env!("CARGO_BIN_EXE_rusty-mcrouter"))
        .env("RUSTY_MCROUTER_LISTEN", "127.0.0.1:0")
        .env("RUSTY_MCROUTER_BACKEND", backend_addr.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn binary");

    let stdout = child.stdout.take().expect("stdout pipe");
    let mut reader = BufReader::new(stdout).lines();
    let line = reader
        .next_line()
        .await
        .expect("read line")
        .expect("eof before READY line");
    let router_addr: SocketAddr = line
        .strip_prefix("READY ")
        .expect("expected READY prefix on stdout")
        .parse()
        .expect("parse router addr");

    let mut app = TcpStream::connect(router_addr).await.unwrap();
    app.write_all(b"get foo\r\n").await.unwrap();
    let n = app.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"VALUE foo 0 3\r\nbar\r\nEND\r\n");

    app.write_all(b"get nonexistent\r\n").await.unwrap();
    let n = app.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"END\r\n");

    app.write_all(b"get foo nonexistent\r\n").await.unwrap();
    let n = app.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"VALUE foo 0 3\r\nbar\r\nEND\r\n");
}
