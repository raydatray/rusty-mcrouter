//! Raw Meta-protocol load generator for benchmarking rusty-mcrouter.
//!
//! Opens `--conns` TCP connections to `--addr`, keeps `--depth` pipelined
//! `mg <key> v\r\n` requests in flight per connection, and counts the
//! `EN\r\n` reply terminators. A non-existent key is used so every reply is
//! exactly `EN\r\n` (a pure miss) from both `NullRoute` and real memcached —
//! that keeps reply framing trivial and exercises the full router path.
//!
//! This is the Meta twin of the classic `get`-miss workload the write-batching
//! baselines in `docs/mvp/design/write-batching.md` were measured with
//! (request 16B vs 15B, reply 4B vs 5B), so runs stay comparable.
//!
//! Reports aggregate throughput (rps) and batch round-trip latency
//! percentiles (one sample per pipelined batch of `depth` requests).
//!
//! Usage:
//!   cargo run --release -p rusty-mcrouter --example load -- \
//!       --addr 127.0.0.1:5000 --conns 8 --depth 64 --secs 5 [--key benchmiss]

use std::{
    env,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

struct Args {
    addr: String,
    conns: usize,
    depth: usize,
    secs: u64,
    key: String,
}

fn parse_args() -> Args {
    let mut addr = "127.0.0.1:5000".to_string();
    let mut conns = 8usize;
    let mut depth = 64usize;
    let mut secs = 5u64;
    let mut key = "benchmiss".to_string();

    let mut it = env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut val = || it.next().expect("missing value for flag");
        match flag.as_str() {
            "--addr" => addr = val(),
            "--conns" => conns = val().parse().expect("conns"),
            "--depth" => depth = val().parse().expect("depth"),
            "--secs" => secs = val().parse().expect("secs"),
            "--key" => key = val(),
            other => panic!("unknown flag: {other}"),
        }
    }

    assert!(conns >= 1 && depth >= 1, "conns and depth must be >= 1");
    Args {
        addr,
        conns,
        depth,
        secs,
        key,
    }
}

/// Count non-overlapping occurrences of `pat` across a streamed chunk.
/// `partial` carries how many leading bytes of `pat` were matched at the tail
/// of the previous chunk. `EN\r\n` has no self-overlapping prefix, so a
/// mismatch resets cleanly.
fn scan(buf: &[u8], pat: &[u8], partial: &mut usize) -> u64 {
    let mut count = 0;
    let mut m = *partial;
    for &b in buf {
        if b == pat[m] {
            m += 1;
            if m == pat.len() {
                count += 1;
                m = 0;
            }
        } else {
            m = usize::from(b == pat[0]);
        }
    }
    *partial = m;
    count
}

#[tokio::main]
async fn main() {
    let args = parse_args();

    // One pipelined batch = `depth` Meta get requests for a missing key.
    let mut batch = Vec::new();
    for _ in 0..args.depth {
        batch.extend_from_slice(format!("mg {} v\r\n", args.key).as_bytes());
    }
    let batch: Arc<[u8]> = Arc::from(batch.into_boxed_slice());
    const TERM: &[u8] = b"EN\r\n";

    let stop = Arc::new(AtomicBool::new(false));
    let total = Arc::new(AtomicU64::new(0));
    let depth = args.depth;

    let start = Instant::now();
    let mut handles = Vec::with_capacity(args.conns);
    for _ in 0..args.conns {
        let addr = args.addr.clone();
        let batch = Arc::clone(&batch);
        let stop = Arc::clone(&stop);
        let total = Arc::clone(&total);
        handles.push(tokio::spawn(async move {
            let mut stream = TcpStream::connect(&addr).await.expect("connect to target");
            stream.set_nodelay(true).expect("nodelay");

            let mut read_buf = vec![0u8; 64 * 1024];
            let mut samples: Vec<u32> = Vec::new();
            let mut local: u64 = 0;

            while !stop.load(Ordering::Relaxed) {
                let t0 = Instant::now();
                if stream.write_all(&batch).await.is_err() {
                    break;
                }
                let mut seen = 0u64;
                let mut partial = 0usize;
                while seen < depth as u64 {
                    match stream.read(&mut read_buf).await {
                        Ok(0) | Err(_) => return (local, samples),
                        Ok(n) => seen += scan(&read_buf[..n], TERM, &mut partial),
                    }
                }
                let us = t0.elapsed().as_micros() as u32;
                if samples.len() < 1_000_000 {
                    samples.push(us);
                }
                local += depth as u64;
            }
            total.fetch_add(local, Ordering::Relaxed);
            (local, samples)
        }));
    }

    tokio::time::sleep(Duration::from_secs(args.secs)).await;
    stop.store(true, Ordering::Relaxed);

    let mut all_samples: Vec<u32> = Vec::new();
    for h in handles {
        if let Ok((_local, samples)) = h.await {
            all_samples.extend_from_slice(&samples);
        }
    }

    let elapsed = start.elapsed().as_secs_f64();
    let requests = total.load(Ordering::Relaxed);
    let rps = requests as f64 / elapsed;

    all_samples.sort_unstable();
    let pct = |p: f64| -> u32 {
        if all_samples.is_empty() {
            return 0;
        }
        let idx = ((all_samples.len() as f64 - 1.0) * p).round() as usize;
        all_samples[idx]
    };
    let (p50, p99, p999) = (pct(0.50), pct(0.99), pct(0.999));
    let per_req_p50 = p50 as f64 / depth as f64;

    println!("----------------------------------------------------------------");
    println!(
        "target={} conns={} depth={} dur={:.2}s",
        args.addr, args.conns, depth, elapsed
    );
    println!("requests={requests} batches={}", all_samples.len());
    println!("throughput: {:.0} rps ({:.2} M/s)", rps, rps / 1.0e6);
    println!("batch latency (depth={depth}): p50={p50}us p99={p99}us p99.9={p999}us");
    println!("per-request p50 (batch/depth): {per_req_p50:.2}us");
    println!("----------------------------------------------------------------");
}
