use rusty_mcrouter_core::{DestinationRoute, Route};
use rusty_mcrouter_net::{Client, Server};
use rusty_mcrouter_protocol::reply::Reply;
use std::sync::Arc;

const DEFAULT_LISTEN_ADDR: &str = "127.0.0.1:5000";
const DEFAULT_BACKEND_ADDR: &str = "127.0.0.1:11211";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listen = std::env::var("RUSTY_MCROUTER_LISTEN")
        .unwrap_or_else(|_| DEFAULT_LISTEN_ADDR.to_string());
    let backend = std::env::var("RUSTY_MCROUTER_BACKEND")
        .unwrap_or_else(|_| DEFAULT_BACKEND_ADDR.to_string());

    let client = Client::connect(&backend).await?;
    let route = Arc::new(DestinationRoute::new(client));

    let server = Server::bind(&listen).await?;
    let bound = server.local_addr()?;

    // Machine-readable readiness line on stdout for process supervisors and
    // integration tests; human log goes to stderr.
    println!("READY {}", bound);
    eprintln!("rusty-mcrouter listening on {} -> backend {}", bound, backend);

    server
        .serve(move |req| {
            let route = Arc::clone(&route);
            async move {
                // v0: backend errors collapse to an empty reply (silent miss).
                // Add Reply::Error variants and propagate properly later.
                route
                    .route(req)
                    .await
                    .unwrap_or_else(|_| Reply::Get { hits: vec![] })
            }
        })
        .await?;

    Ok(())
}
