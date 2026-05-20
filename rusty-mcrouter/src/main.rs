use bytes::Bytes;
use clap::Parser;
use rusty_mcrouter_config::parse_file;
use rusty_mcrouter_core::route_builder::build_route;
use rusty_mcrouter_net::Server;
use rusty_mcrouter_protocol::reply::Reply;
use std::{path::PathBuf, sync::Arc};

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
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let config = parse_file(&args.config)?;
    let route = build_route(&config).await?;
    let server = Server::bind(&args.listen).await?;
    let bound = server.local_addr()?;

    println!("READY {}", bound);
    eprintln!(
        "rusty-mcrouter listening on {} -> backend {}",
        bound,
        args.config.display()
    );

    server
        .serve(move |req| {
            let route = Arc::clone(&route);
            async move {
                route.route_dyn(req).await.unwrap_or_else(|_| {
                    Reply::ServerError(Bytes::from_static(b"backend unavailable"))
                })
            }
        })
        .await?;

    Ok(())
}
