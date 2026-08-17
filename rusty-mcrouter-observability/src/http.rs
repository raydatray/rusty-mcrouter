// minimal http responder for /metrics. one endpoint, no framework:
// read the request head, answer GET /metrics with a fresh render,
// 404 everything else, close. no keep-alive - prometheus reconnects
// per scrape and this is not a general-purpose server.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::metrics::MetricsRegistry;

const MAX_REQUEST_HEAD: usize = 8 * 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

pub async fn serve(listener: TcpListener, registry: Arc<MetricsRegistry>) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            continue;
        };
        let registry = Arc::clone(&registry);
        tokio::spawn(async move {
            let _ = tokio::time::timeout(REQUEST_TIMEOUT, respond(stream, registry)).await;
        });
    }
}

async fn respond(mut stream: TcpStream, registry: Arc<MetricsRegistry>) {
    let mut head = Vec::new();
    let mut chunk = [0u8; 1024];
    // read until end of request head; body (if any) is ignored
    while !head.windows(4).any(|w| w == b"\r\n\r\n") {
        if head.len() > MAX_REQUEST_HEAD {
            return;
        }
        match stream.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(n) => head.extend_from_slice(&chunk[..n]),
        }
    }

    let request_line = head.split(|&b| b == b'\r').next().unwrap_or(b"");
    let response = if request_line.starts_with(b"GET /metrics ") {
        let body = registry.render();
        format!(
            "HTTP/1.1 200 OK\r\n\
             Content-Type: text/plain; version=0.0.4\r\n\
             Content-Length: {}\r\n\
             Connection: close\r\n\r\n{}",
            body.len(),
            body
        )
    } else {
        "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_string()
    };

    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.shutdown().await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::{MetricsSource, MetricsText};

    struct Static;
    impl MetricsSource for Static {
        fn encode(&self, out: &mut MetricsText) {
            out.counter("test_total", &[], 7);
        }
    }

    async fn start() -> std::net::SocketAddr {
        let mut registry = MetricsRegistry::new();
        registry.register(Box::new(Static));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(listener, Arc::new(registry)));
        addr
    }

    async fn request(addr: std::net::SocketAddr, req: &str) -> String {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(req.as_bytes()).await.unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        response
    }

    #[tokio::test]
    async fn get_metrics_renders_a_scrape() {
        let addr = start().await;
        let response = request(addr, "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert!(response.starts_with("HTTP/1.1 200 OK\r\n"), "{response}");
        assert!(response.contains("Content-Type: text/plain; version=0.0.4\r\n"));
        assert!(response.ends_with("\r\n\r\ntest_total 7\n"), "{response}");
    }

    #[tokio::test]
    async fn content_length_matches_the_body() {
        let addr = start().await;
        let response = request(addr, "GET /metrics HTTP/1.1\r\n\r\n").await;
        let (head, body) = response.split_once("\r\n\r\n").unwrap();
        let length: usize = head
            .lines()
            .find_map(|l| l.strip_prefix("Content-Length: "))
            .unwrap()
            .parse()
            .unwrap();
        assert_eq!(length, body.len());
    }

    #[tokio::test]
    async fn other_paths_get_404() {
        let addr = start().await;
        let response = request(addr, "GET /other HTTP/1.1\r\n\r\n").await;
        assert!(response.starts_with("HTTP/1.1 404 "), "{response}");
    }

    #[tokio::test]
    async fn non_get_gets_404() {
        let addr = start().await;
        let response = request(addr, "POST /metrics HTTP/1.1\r\n\r\n").await;
        assert!(response.starts_with("HTTP/1.1 404 "), "{response}");
    }

    #[tokio::test]
    async fn garbage_does_not_wedge_the_server() {
        let addr = start().await;
        // no head terminator: the request times out server-side; the
        // NEXT scrape must still work
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"garbage without terminator")
            .await
            .unwrap();
        drop(stream);

        let response = request(addr, "GET /metrics HTTP/1.1\r\n\r\n").await;
        assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
    }
}
