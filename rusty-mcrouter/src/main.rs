use clap::Parser;
use rusty_mcrouter_config::parse_file;
use tokio::sync::mpsc;

use std::{
    io::Write,
    net::{SocketAddr, ToSocketAddrs},
    path::PathBuf,
    sync::Arc,
};
mod proxy_thread;

const WORK_CHANNEL_CAPACITY: usize = 1024;

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

    // todo - threading, this is only socket handoff; add per-proxy message queues so requests enter a proxy actor like mcrouter
    let (work_txs, work_rxs): (Vec<_>, Vec<_>) = (0..args.num_proxies)
        .map(|_| mpsc::channel::<std::net::TcpStream>(WORK_CHANNEL_CAPACITY))
        .unzip();

    let use_reuseport = num_listening_sockets > 1;
    let mut handles = Vec::with_capacity(args.num_proxies);
    let mut bound_addr: Option<SocketAddr> = None;
    let mut work_rxs_iter = work_rxs.into_iter();

    for proxy_id in 0..args.num_proxies {
        let has_listener = proxy_id < num_listening_sockets;
        let work_rx = work_rxs_iter.next().expect("one work_rx per proxy thread");
        let listener_txs = if has_listener {
            Some(work_txs.clone())
        } else {
            None
        };

        let (ready_tx, ready_rx) =
            std::sync::mpsc::sync_channel::<anyhow::Result<Option<SocketAddr>>>(1);

        let config = Arc::clone(&config);
        let handle = std::thread::Builder::new()
            .name(format!("proxy-{proxy_id}"))
            .spawn(move || {
                if let Err(e) = proxy_thread::proxy_thread_main(
                    listen_addr,
                    use_reuseport,
                    config,
                    listener_txs,
                    work_rx,
                    ready_tx,
                ) {
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

    // Drop our copy of the work_txs Vec. Each listener thread already holds its own
    // clone, so the channels stay open until all listeners terminate.
    drop(work_txs);

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
