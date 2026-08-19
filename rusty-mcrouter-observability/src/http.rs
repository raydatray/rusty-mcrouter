use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::header::CONTENT_TYPE;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use rusty_mcrouter_observability_primitives::Counter;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;

use crate::metrics::MetricsRegistry;

const MAX_HTTP_TASKS: usize = 32;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const CONTENT_TYPE_VALUE: &str = "text/plain; version=0.0.4";

pub struct MetricsHttp {
    listener: TcpListener,
    registry: Arc<MetricsRegistry>,
    tasks: JoinSet<anyhow::Result<()>>,
    rejected: Arc<Counter>,
}

impl MetricsHttp {
    pub fn new(
        listener: TcpListener,
        registry: Arc<MetricsRegistry>,
        rejected: Arc<Counter>,
    ) -> Self {
        Self {
            listener,
            registry,
            tasks: JoinSet::new(),
            rejected,
        }
    }

    pub async fn step(&mut self) -> anyhow::Result<()> {
        tokio::select! {
            accepted = self.listener.accept() => {
                let (stream, _) = accepted.context("accept metrics connection")?;
                if self.tasks.len() >= MAX_HTTP_TASKS {
                    self.rejected.inc();
                    drop(stream);
                    return Ok(());
                }

                let registry = Arc::clone(&self.registry);
                self.tasks.spawn(async move { serve_connection(stream, registry).await });
            }

            Some(result) = self.tasks.join_next(), if !self.tasks.is_empty() => {
                match result {
                    Err(error) => return Err(error).context("metrics connection task failed"),
                    Ok(Err(error)) => tracing::debug!(%error, "metrics connection failed"),
                    Ok(Ok(())) => {}
                }
            }
        }
        Ok(())
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        loop {
            self.step().await?;
        }
    }

    pub async fn shutdown(&mut self) {
        self.tasks.shutdown().await;
    }
}

pub async fn serve_connection(
    stream: TcpStream,
    registry: Arc<MetricsRegistry>,
) -> anyhow::Result<()> {
    let io = TokioIo::new(stream);
    let service = service_fn(move |request| respond(request, Arc::clone(&registry)));

    tokio::time::timeout(
        REQUEST_TIMEOUT,
        http1::Builder::new()
            .keep_alive(false)
            .serve_connection(io, service),
    )
    .await
    .context("metrics connection timed out")?
    .context("serve metrics connection")?;
    Ok(())
}

async fn respond(
    request: Request<Incoming>,
    registry: Arc<MetricsRegistry>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    if request.method() == Method::GET && request.uri().path() == "/metrics" {
        let response = Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, CONTENT_TYPE_VALUE)
            .body(Full::new(Bytes::from(registry.render())))
            .expect("constant response is valid");
        return Ok(response);
    }

    Ok(Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Full::new(Bytes::new()))
        .expect("constant response is valid"))
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

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
        let server = MetricsHttp::new(listener, Arc::new(registry), Arc::new(Counter::default()));
        tokio::spawn(async move { server.run().await.unwrap() });
        addr
    }

    async fn request(addr: std::net::SocketAddr, request: &str) -> String {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        response
    }

    #[tokio::test]
    async fn get_metrics_renders_a_scrape() {
        let response = request(
            start().await,
            "GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n",
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 200 OK\r\n"), "{response}");
        assert!(response.contains("content-type: text/plain; version=0.0.4\r\n"));
        assert!(response.ends_with("\r\n\r\ntest_total 7\n"), "{response}");
    }

    #[tokio::test]
    async fn other_routes_and_methods_are_not_found() {
        let addr = start().await;
        for request_head in [
            "GET /nope HTTP/1.1\r\nHost: localhost\r\n\r\n",
            "POST /metrics HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n",
        ] {
            let response = request(addr, request_head).await;
            assert!(
                response.starts_with("HTTP/1.1 404 Not Found\r\n"),
                "{response}"
            );
        }
    }

    #[tokio::test]
    async fn malformed_http_does_not_stop_the_server() {
        let addr = start().await;
        let _ = request(addr, "garbage\r\n\r\n").await;
        let response = request(addr, "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert!(response.starts_with("HTTP/1.1 200 OK\r\n"), "{response}");
    }
}
