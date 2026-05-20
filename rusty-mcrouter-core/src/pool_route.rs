use std::sync::Arc;

use rand::random_range;
use rusty_mcrouter_protocol::{reply::Reply, request::Request};

use crate::{
    destination_route::DestinationRoute,
    route::{Route, RouteError},
};

pub struct PoolRoute {
    // todo - clients, not destination routes
    children: Vec<Arc<DestinationRoute>>,
}

impl PoolRoute {
    pub fn new(children: Vec<Arc<DestinationRoute>>) -> Option<Self> {
        if children.is_empty() {
            return None;
        }

        Some(Self { children })
    }
}

impl Route for PoolRoute {
    async fn route(&self, req: Request) -> Result<Reply, RouteError> {
        // todo - hash, this is a random func
        self.children[random_range(0..self.children.len())]
            .route(req)
            .await
    }
}

#[cfg(test)]
mod tests {
    use crate::route::DynRoute;

    use super::*;
    use bytes::Bytes;
    use rusty_mcrouter_net::Client;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    async fn counting_mock_backend(reply: &'static [u8]) -> (SocketAddr, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let count_clone = Arc::clone(&count);
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 1024];
            loop {
                match stream.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(_) => {
                        count_clone.fetch_add(1, Ordering::Relaxed);
                        if stream.write_all(reply).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });
        (addr, count)
    }

    fn req_get(keys: &[&'static [u8]]) -> Request {
        Request::Get {
            keys: keys.iter().map(|k| Bytes::from_static(k)).collect(),
        }
    }

    async fn pool_of_one(reply: &'static [u8]) -> (PoolRoute, Arc<AtomicUsize>) {
        let (addr, count) = counting_mock_backend(reply).await;
        let client = Client::connect(addr).await.unwrap();
        let dr = Arc::new(DestinationRoute::new(client));
        let pool = PoolRoute::new(vec![dr]).expect("non-empty");
        (pool, count)
    }

    #[tokio::test]
    async fn new_returns_none_for_empty_children() {
        assert!(PoolRoute::new(vec![]).is_none());
    }

    #[tokio::test]
    async fn single_backend_pool_routes_to_that_backend() {
        let (pool, count) = pool_of_one(b"END\r\n").await;
        let reply = pool.route(req_get(&[b"foo"])).await.unwrap();
        assert_eq!(reply, Reply::Get { hits: vec![] });
        assert_eq!(count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn multi_backend_pool_distributes_traffic_across_children() {
        let (addr1, count1) = counting_mock_backend(b"END\r\n").await;
        let (addr2, count2) = counting_mock_backend(b"END\r\n").await;
        let c1 = Client::connect(addr1).await.unwrap();
        let c2 = Client::connect(addr2).await.unwrap();
        let pool = PoolRoute::new(vec![
            Arc::new(DestinationRoute::new(c1)),
            Arc::new(DestinationRoute::new(c2)),
        ])
        .expect("non-empty");

        for _ in 0..100 {
            let _ = pool.route(req_get(&[b"foo"])).await.unwrap();
        }

        let n1 = count1.load(Ordering::Relaxed);
        let n2 = count2.load(Ordering::Relaxed);
        assert!(n1 > 0, "backend 1 got 0 requests over 100 trials");
        assert!(n2 > 0, "backend 2 got 0 requests over 100 trials");
        assert_eq!(n1 + n2, 100);
    }

    #[tokio::test]
    async fn pool_route_works_through_dyn_route_trait_object() {
        let (pool, _) = pool_of_one(b"END\r\n").await;
        let route: Arc<dyn DynRoute> = Arc::new(pool);
        let reply = route.route_dyn(req_get(&[b"foo"])).await.unwrap();
        assert_eq!(reply, Reply::Get { hits: vec![] });
    }

    #[tokio::test]
    async fn pool_route_can_be_shared_across_tasks_via_arc() {
        let (pool, count) = pool_of_one(b"END\r\n").await;
        let pool = Arc::new(pool);

        let p = Arc::clone(&pool);
        let h = tokio::spawn(async move { p.route(req_get(&[b"foo"])).await });
        let _ = h.await.unwrap().unwrap();

        assert_eq!(count.load(Ordering::Relaxed), 1);
    }
}
