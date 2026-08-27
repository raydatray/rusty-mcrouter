//! Compatibility tests against an official memcached container.

use std::net::SocketAddr;
use std::time::Duration;

use testcontainers::{core::IntoContainerPort, runners::AsyncRunner, ContainerAsync, GenericImage};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::OnceCell;
use tokio::time::Instant;

mod support;

use support::{exchange, RouterProcess};

struct Fixture {
    router_addr: SocketAddr,
    _backend: ContainerAsync<GenericImage>,
    _router: RouterProcess,
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
            let backend_addr = format!("127.0.0.1:{backend_port}").parse().unwrap();

            seed_backend(backend_addr).await;

            let config = format!(
                r#"{{"pools":{{"memcached":{{"servers":["{backend_addr}"]}}}},"route":"PoolRoute|memcached"}}"#
            );
            let router = RouterProcess::spawn(&config, backend_port, 1, &[]).await;

            Fixture {
                router_addr: router.router_addr,
                _backend: backend,
                _router: router,
            }
        })
        .await
}

async fn seed_backend(addr: SocketAddr) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let mut connection = wait_for_tcp(addr).await;
        let seeded: std::io::Result<bool> = async {
            connection.write_all(b"ms seeded_foo 3\r\nbar\r\n").await?;
            let mut response = [0u8; 64];
            let read = connection.read(&mut response).await?;
            Ok(&response[..read] == b"HD\r\n")
        }
        .await;

        match seeded {
            Ok(true) => return,
            Ok(false) | Err(_) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            result => panic!("seeding backend failed: {result:?}"),
        }
    }
}

async fn wait_for_tcp(addr: SocketAddr) -> TcpStream {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(connection) = TcpStream::connect(addr).await {
            return connection;
        }
        assert!(Instant::now() <= deadline, "connect to {addr} timed out");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn exchange_many(addr: SocketAddr, exchanges: &[(&'static [u8], &'static [u8])]) {
    for &(request, expected) in exchanges {
        exchange(addr, request, expected).await;
    }
}

#[tokio::test]
async fn get_hit_and_miss_match_memcached() {
    let fixture = fixture().await;
    exchange_many(
        fixture.router_addr,
        &[
            (b"mg seeded_foo v\r\n", b"VA 3\r\nbar\r\n"),
            (b"mg compat_missing v\r\n", b"EN\r\n"),
        ],
    )
    .await;
}

#[tokio::test]
async fn storage_modes_match_memcached() {
    let fixture = fixture().await;
    exchange_many(
        fixture.router_addr,
        &[
            (b"ms compat_modes 1 F7\r\nx\r\n", b"HD\r\n"),
            (b"ms compat_modes 1 ME\r\ny\r\n", b"NS\r\n"),
            (b"ms compat_replace_missing 1 MR\r\nx\r\n", b"NS\r\n"),
            (b"ms compat_modes 1 MR F7\r\ny\r\n", b"HD\r\n"),
            (b"ms compat_modes 1 MA F999\r\nz\r\n", b"HD\r\n"),
            (b"ms compat_modes 1 MP\r\nx\r\n", b"HD\r\n"),
            (b"mg compat_modes v f s\r\n", b"VA 3 f7 s3\r\nxyz\r\n"),
        ],
    )
    .await;
}

#[tokio::test]
async fn delete_and_touch_match_memcached() {
    let fixture = fixture().await;
    exchange_many(
        fixture.router_addr,
        &[
            (b"ms compat_touch 5 F42 T500\r\nhello\r\n", b"HD\r\n"),
            (b"mg compat_touch T3600 t\r\n", b"HD t3600\r\n"),
            (b"mg compat_touch v f\r\n", b"VA 5 f42\r\nhello\r\n"),
            (b"md compat_touch\r\n", b"HD\r\n"),
            (b"mg compat_touch v\r\n", b"EN\r\n"),
        ],
    )
    .await;
}

#[tokio::test]
async fn arithmetic_matches_memcached() {
    let fixture = fixture().await;
    exchange_many(
        fixture.router_addr,
        &[
            (b"ma compat_counter N60 J5 v\r\n", b"VA 1\r\n5\r\n"),
            (b"ma compat_counter D100 v\r\n", b"VA 3\r\n105\r\n"),
            (b"ma compat_counter MD D200 v\r\n", b"VA 1\r\n0\r\n"),
        ],
    )
    .await;
}

#[tokio::test]
async fn quiet_pipeline_and_echo_match_memcached() {
    let fixture = fixture().await;
    exchange(
        fixture.router_addr,
        b"mg seeded_foo v q Ofirst\r\nmg compat_missing v q Osecond\r\nmn\r\n",
        b"VA 3 Ofirst\r\nbar\r\nMN\r\n",
    )
    .await;
    exchange(
        fixture.router_addr,
        b"ms compat_echo 2 s k Otag\r\nhi\r\n",
        b"HD s2 kcompat_echo Otag\r\n",
    )
    .await;
    exchange(
        fixture.router_addr,
        b"ms compat_quiet 2 q\r\nhi\r\nms compat_quiet 2 q ME\r\nhi\r\nmn\r\n",
        b"NS\r\nMN\r\n",
    )
    .await;
}

#[tokio::test]
async fn debug_matches_memcached() {
    let fixture = fixture().await;
    exchange(fixture.router_addr, b"me compat_missing\r\n", b"EN\r\n").await;
}
