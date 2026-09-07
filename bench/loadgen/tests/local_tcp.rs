use std::sync::Arc;
use std::time::Duration;

use rusty_mcrouter_loadgen::runner::{run_closed, RunConfig};
use rusty_mcrouter_loadgen::workload::{OpKind, WorkloadSpec};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const WORKLOAD: &str = r#"
seed = 9
keyspace = 100

[ops]
mg = 1
ms = 1
md = 1
ma = 1

[keys]
distribution = "uniform"
length = { min = 8, max = 12 }

[values]
ttl = { min = 60, max = 60 }
buckets = [{ weight = 1, min = 3, max = 9 }]
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drives_pipelined_meta_requests_against_local_tcp_server() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            tokio::spawn(serve(stream));
        }
    });
    let workload = Arc::new(WorkloadSpec::parse(WORKLOAD).unwrap().validate().unwrap());
    let stats = run_closed(
        RunConfig {
            target: address.to_string(),
            connections: 2,
            depth: 8,
            warmup: Duration::from_millis(20),
            duration: Duration::from_millis(80),
        },
        workload,
    )
    .await
    .unwrap();
    server.abort();

    assert!(stats.measured > 0);
    assert_eq!(stats.sent, stats.completed);
    assert_eq!(stats.errors(), 0);
    assert_eq!(
        stats.measured,
        OpKind::ALL
            .iter()
            .map(|kind| stats.ops[*kind as usize].count)
            .sum()
    );
}

async fn serve(mut stream: TcpStream) {
    let mut buffered = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let read = stream.read(&mut chunk).await.unwrap();
        if read == 0 {
            return;
        }
        buffered.extend_from_slice(&chunk[..read]);
        while let Some((consumed, reply)) = request(&buffered) {
            stream.write_all(reply).await.unwrap();
            buffered.drain(..consumed);
        }
    }
}

fn request(buffer: &[u8]) -> Option<(usize, &'static [u8])> {
    let newline = buffer.windows(2).position(|pair| pair == b"\r\n")?;
    let line = &buffer[..newline];
    let line_frame = newline + 2;
    if line.starts_with(b"ms ") {
        let length = line
            .split(|byte| *byte == b' ')
            .nth(2)
            .and_then(|raw| std::str::from_utf8(raw).ok())
            .and_then(|raw| raw.parse::<usize>().ok())?;
        let frame = line_frame + length + 2;
        if buffer.len() < frame {
            return None;
        }
        assert_eq!(&buffer[frame - 2..frame], b"\r\n");
        Some((frame, b"HD\r\n"))
    } else if line.starts_with(b"mg ") {
        Some((line_frame, b"EN\r\n"))
    } else if line.starts_with(b"md ") {
        Some((line_frame, b"NF\r\n"))
    } else if line.starts_with(b"ma ") {
        Some((line_frame, b"HD\r\n"))
    } else {
        panic!("unexpected request: {}", String::from_utf8_lossy(line));
    }
}
