use rusty_mcrouter_net::{Client, NetError};
use rusty_mcrouter_protocol::{reply::Reply, request::Request};
use std::future::Future;
use thiserror::Error;
use tokio::sync::Mutex;

#[derive(Debug, Error)]
pub enum RouteError {
    #[error("backend error: {0}")]
    Backend(#[from] NetError),
}

pub trait Route: Send + Sync + 'static {
    fn route(&self, req: Request) -> impl Future<Output = Result<Reply, RouteError>> + Send;
}

/// Routes every request to a single fixed backend over one connection.
pub struct DestinationRoute {
    client: Mutex<Client>,
}

impl DestinationRoute {
    pub fn new(client: Client) -> Self {
        Self {
            client: Mutex::new(client),
        }
    }
}

impl Route for DestinationRoute {
    async fn route(&self, req: Request) -> Result<Reply, RouteError> {
        let mut client = self.client.lock().await;
        Ok(client.send(&req).await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    async fn mock_backend(reply: &'static [u8]) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 1024];
            let _ = stream.read(&mut buf).await.unwrap();
            stream.write_all(reply).await.unwrap();
        });
        addr
    }

    fn req_get(keys: &[&'static [u8]]) -> Request {
        Request::Get {
            keys: keys.iter().map(|k| Bytes::from_static(k)).collect(),
        }
    }

    #[tokio::test]
    async fn destination_route_forwards_request_to_backend_and_returns_reply() {
        let addr = mock_backend(b"VALUE foo 0 3\r\nbar\r\nEND\r\n").await;
        let client = Client::connect(addr).await.unwrap();
        let route = DestinationRoute::new(client);

        let reply = route.route(req_get(&[b"foo"])).await.unwrap();
        let Reply::Get { hits } = reply else {
            panic!("expected Reply::Get");
        };
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].key.as_ref(), b"foo");
        assert_eq!(hits[0].data.as_ref(), b"bar");
    }

    #[tokio::test]
    async fn destination_route_returns_empty_reply_on_miss() {
        let addr = mock_backend(b"END\r\n").await;
        let client = Client::connect(addr).await.unwrap();
        let route = DestinationRoute::new(client);

        let reply = route.route(req_get(&[b"foo"])).await.unwrap();
        assert_eq!(reply, Reply::Get { hits: vec![] });
    }

    #[tokio::test]
    async fn destination_route_propagates_backend_protocol_error() {
        let addr = mock_backend(b"WAT\r\n").await;
        let client = Client::connect(addr).await.unwrap();
        let route = DestinationRoute::new(client);

        let result = route.route(req_get(&[b"foo"])).await;
        assert!(matches!(result, Err(RouteError::Backend(_))));
    }

    #[tokio::test]
    async fn destination_route_can_be_shared_across_tasks_via_arc() {
        let addr = mock_backend(b"END\r\n").await;
        let client = Client::connect(addr).await.unwrap();
        let route = Arc::new(DestinationRoute::new(client));

        let route_clone = Arc::clone(&route);
        let result = tokio::spawn(async move { route_clone.route(req_get(&[b"foo"])).await })
            .await
            .unwrap();

        assert_eq!(result.unwrap(), Reply::Get { hits: vec![] });
    }
}
