use clap::Parser;
use rusty_mcrouter_config::parse_file;
use tokio::sync::mpsc;

use std::{
    io::Write,
    net::{SocketAddr, ToSocketAddrs},
    path::PathBuf,
    sync::Arc,
};
mod proxy;

use crate::proxy::{
    ListenerConfig, ProxyHandle, ProxyMessage, ProxySet, ProxyThreadConfig, ThreadMode,
};

const WORK_CHANNEL_CAPACITY: usize = 1024;
const PROXY_CHANNEL_CAPACITY: usize = 1024;

#[derive(Parser)]
struct Args {
    #[arg(
        long,
        value_name = "PATH",
        help = "path to mcrouter-format JSON config file"
    )]
    config: PathBuf,

    #[arg(
        long,
        value_name = "ADDR",
        default_value = "127.0.0.1:5000",
        env = "RUSTY_MCROUTER_LISTEN",
        help = "address to listen on"
    )]
    listen: String,

    #[arg(
        long,
        value_name = "N",
        default_value = "4",
        env = "RUSTY_MCROUTER_NUM_PROXIES",
        help = "num of proxy threads"
    )]
    num_proxies: usize,

    #[arg(
        long,
        value_name = "M",
        env = "RUSTY_MCROUTER_NUM_LISTENING_SOCKETS",
        help = "number of SO_REUSEPORT listening sockets (defaults to num_proxies)"
    )]
    num_listening_sockets: Option<usize>,
}

fn main() -> anyhow::Result<()> {
    std::panic::set_hook(Box::new(|info| {
        eprintln!("FATAL: panic in proxy thread: {info}");
        std::process::exit(1);
    }));

    let args = Args::parse();
    let num_listening_sockets = args.num_listening_sockets.unwrap_or(1);
    if args.num_proxies == 0 {
        anyhow::bail!("num_proxies must be >= 1")
    }
    if num_listening_sockets == 0 {
        anyhow::bail!("num_listening_sockets must be >= 1");
    }
    if num_listening_sockets > args.num_proxies {
        anyhow::bail!(
            "num_listening_sockets ({}) must be <= num_proxies ({})",
            num_listening_sockets,
            args.num_proxies
        );
    }

    let listen_addr = args
        .listen
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| anyhow::anyhow!("could not resolve listen address: {}", args.listen))?;

    let config = Arc::new(parse_file(&args.config)?);

    let (work_txs, work_rxs): (Vec<_>, Vec<_>) = (0..args.num_proxies)
        .map(|_| mpsc::channel::<std::net::TcpStream>(WORK_CHANNEL_CAPACITY))
        .unzip();

    let (proxy_txs, proxy_rxs): (Vec<_>, Vec<_>) = (0..args.num_proxies)
        .map(|_| mpsc::channel::<ProxyMessage>(PROXY_CHANNEL_CAPACITY))
        .unzip();
    let proxies = ProxySet::new(
        proxy_txs
            .iter()
            .enumerate()
            .map(|(id, tx)| ProxyHandle::new(id, tx.clone()))
            .collect(),
    );

    let use_reuseport = num_listening_sockets > 1;
    let mut handles = Vec::with_capacity(args.num_proxies);
    let mut bound_addr: Option<SocketAddr> = None;
    let mut work_rxs_iter = work_rxs.into_iter();
    let mut proxy_rxs_iter = proxy_rxs.into_iter();

    for proxy_id in 0..args.num_proxies {
        let has_listener = proxy_id < num_listening_sockets;
        let work_rx = work_rxs_iter.next().expect("one work_rx per proxy thread");
        let proxy_rx = proxy_rxs_iter.next().expect("one proxy_rx per proxy thread");
        let listener_config = if has_listener {
            Some(ListenerConfig {
                listen_addr,
                use_reuseport,
                listener_txs: work_txs.clone(),
            })
        } else {
            None
        };

        let cfg = ProxyThreadConfig {
            proxy_id,
            config: Arc::clone(&config),
            work_rx,
            proxy_rx,
            proxies: proxies.clone(),
            thread_mode: ThreadMode::SameThread,
            listener_config,
        };

        let (ready_tx, ready_rx) =
            std::sync::mpsc::sync_channel::<anyhow::Result<Option<SocketAddr>>>(1);

        let handle = std::thread::Builder::new()
            .name(format!("proxy-{proxy_id}"))
            .spawn(move || {
                if let Err(e) = proxy::proxy_thread_main(cfg, ready_tx) {
                    eprintln!("proxy-{proxy_id} terminated: {e}");
                    std::process::exit(1);
                }
            })?;

        match ready_rx.recv() {
            Ok(Ok(maybe_addr)) => {
                if let Some(addr) = maybe_addr {
                    bound_addr.get_or_insert(addr);
                }
            }
            Ok(Err(e)) => anyhow::bail!("proxy-{proxy_id} startup failed: {e}"),
            Err(_) => anyhow::bail!("proxy-{proxy_id} died during startup"),
        }

        handles.push(handle);
    }

    // Drop main's sender copies. Each thread keeps its own clones (work_txs inside
    // listener_config, proxy senders inside the ProxySet), so the queues stay open
    // until the threads terminate.
    drop(work_txs);
    drop(proxy_txs);
    drop(proxies);

    let addr =
        bound_addr.ok_or_else(|| anyhow::anyhow!("no proxy thread reported a bound address"))?;
    println!("READY {addr}");
    std::io::stdout().flush().ok();
    eprintln!(
        "rusty-mcrouter listening on {} with {} proxy threads ({} listening sockets) -> {}",
        addr,
        args.num_proxies,
        num_listening_sockets,
        args.config.display()
    );

    for handle in handles {
        let _ = handle.join();
    }

    Ok(())
}
