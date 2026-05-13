use rusty_mcrouter_core::{DestinationRoute, Route};
use rusty_mcrouter_net::{Client, Server};
use rusty_mcrouter_protocol::reply::Reply;
use std::sync::Arc;

const LISTEN_ADDR: &str = "127.0.0.1:5000";
const BACKEND_ADDR: &str = "127.0.0.1:11211";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::connect(BACKEND_ADDR).await?;
    let route = Arc::new(DestinationRoute::new(client));

    let server = Server::bind(LISTEN_ADDR).await?;
    eprintln!(
        "rusty-mcrouter listening on {} -> backend {}",
        LISTEN_ADDR, BACKEND_ADDR
    );

    server
        .serve(move |req| {
            let route = Arc::clone(&route);
            async move {
                // backend errors collapse to an empty reply (silent miss).
                // todo - add Reply::Error variants and propagate properly later.
                route
                    .route(req)
                    .await
                    .unwrap_or_else(|_| Reply::Get { hits: vec![] })
            }
        })
        .await?;

    Ok(())
}
