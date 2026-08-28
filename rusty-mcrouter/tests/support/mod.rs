use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::net::TcpStream;
use tokio::process::{Child, ChildStdout, Command};

pub struct RouterProcess {
    pub router_addr: SocketAddr,
    pub(crate) _metrics_addr: SocketAddr,
    pub(crate) _child: Child,
    config_path: PathBuf,
}

impl RouterProcess {
    pub async fn spawn(config: &str, tag: u16, num_proxies: usize, extra_args: &[&str]) -> Self {
        let config_path = std::env::temp_dir().join(format!(
            "rusty-mcrouter-e2e-{}-{tag}.json",
            std::process::id()
        ));
        std::fs::write(&config_path, config).unwrap();

        let mut child = Command::new(env!("CARGO_BIN_EXE_rusty-mcrouter"))
            .arg("--config")
            .arg(&config_path)
            .arg("--num-proxies")
            .arg(num_proxies.to_string())
            .arg("--metrics-addr")
            .arg("127.0.0.1:0")
            .args(extra_args)
            .env("RUSTY_MCROUTER_LISTEN", "127.0.0.1:0")
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .unwrap();

        let stdout = child.stdout.take().unwrap();
        let mut lines = BufReader::new(stdout).lines();
        let router_addr = read_address(&mut lines, "READY ").await;
        let metrics_addr = read_address(&mut lines, "METRICS ").await;

        Self {
            router_addr,
            _metrics_addr: metrics_addr,
            _child: child,
            config_path,
        }
    }
}

impl Drop for RouterProcess {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.config_path);
    }
}

async fn read_address(lines: &mut Lines<BufReader<ChildStdout>>, prefix: &str) -> SocketAddr {
    lines
        .next_line()
        .await
        .unwrap()
        .expect("router exited before reporting readiness")
        .strip_prefix(prefix)
        .unwrap_or_else(|| panic!("expected {prefix:?} address line"))
        .parse()
        .unwrap()
}

pub async fn exchange(addr: SocketAddr, request: &[u8], expected: &[u8]) {
    let mut connection = TcpStream::connect(addr).await.unwrap();
    connection.write_all(request).await.unwrap();

    let mut received = Vec::with_capacity(expected.len());
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut chunk = [0u8; 4096];
        while received.len() < expected.len() {
            let read = connection.read(&mut chunk).await.unwrap();
            if read == 0 {
                break;
            }
            received.extend_from_slice(&chunk[..read]);
        }
    })
    .await
    .expect("timed out waiting for reply bytes");

    assert_eq!(
        received,
        expected,
        "request {:?}",
        String::from_utf8_lossy(request)
    );
}

pub async fn eventually_gets(addr: SocketAddr, key: &[u8], value: &[u8]) {
    let expected = fenced_value(value);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);

    loop {
        let response = fenced_get(addr, key).await;
        if response == expected {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "key {:?} never reached expected value {:?}; last response {:?}",
            String::from_utf8_lossy(key),
            String::from_utf8_lossy(value),
            String::from_utf8_lossy(&response),
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

pub async fn assert_stays_missing(addr: SocketAddr, key: &[u8]) {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(100);

    loop {
        let response = fenced_get(addr, key).await;
        assert_eq!(
            response,
            b"MN\r\n",
            "key {:?}",
            String::from_utf8_lossy(key)
        );
        if tokio::time::Instant::now() >= deadline {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

pub async fn assert_stays_value(addr: SocketAddr, key: &[u8], value: &[u8]) {
    let expected = fenced_value(value);
    let deadline = tokio::time::Instant::now() + Duration::from_millis(100);

    loop {
        let response = fenced_get(addr, key).await;
        assert_eq!(response, expected, "key {:?}", String::from_utf8_lossy(key));
        if tokio::time::Instant::now() >= deadline {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn fenced_get(addr: SocketAddr, key: &[u8]) -> Vec<u8> {
    let mut request = Vec::with_capacity(key.len() + 14);
    request.extend_from_slice(b"mg ");
    request.extend_from_slice(key);
    request.extend_from_slice(b" v q\r\nmn\r\n");

    let mut connection = TcpStream::connect(addr).await.unwrap();
    connection.write_all(&request).await.unwrap();

    tokio::time::timeout(Duration::from_secs(5), async {
        let mut response = Vec::new();
        let mut chunk = [0; 4096];
        while !response.ends_with(b"MN\r\n") {
            let read = connection.read(&mut chunk).await.unwrap();
            assert_ne!(read, 0, "connection closed before noop fence");
            response.extend_from_slice(&chunk[..read]);
        }
        response
    })
    .await
    .expect("timed out waiting for fenced get")
}

fn fenced_value(value: &[u8]) -> Vec<u8> {
    let mut expected = format!("VA {}\r\n", value.len()).into_bytes();
    expected.extend_from_slice(value);
    expected.extend_from_slice(b"\r\nMN\r\n");
    expected
}
