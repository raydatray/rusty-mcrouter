use bytes::Bytes;
use rusty_mcrouter_protocol::reply::ErrorReply;
use rusty_mcrouter_protocol::{Reply, Request};
use tokio::sync::{mpsc::Sender, oneshot};

/// A semantic request and the channel for its worker's reply. Client reply
/// plans and sequence numbers stay in the frontend connection.
pub struct ProxyRequest {
    pub request: Request,
    pub reply_tx: oneshot::Sender<Reply>,
}

/// Submit through a worker mailbox and await its reply. The same transport
/// serves connections and callers using a proxy handle.
pub async fn send_request(request_tx: &Sender<ProxyRequest>, request: Request) -> Reply {
    let (reply_tx, reply_rx) = oneshot::channel();

    if request_tx
        .send(ProxyRequest { request, reply_tx })
        .await
        .is_err()
    {
        return server_error(b"proxy unavailable");
    }

    reply_rx
        .await
        .unwrap_or_else(|_| server_error(b"proxy dropped request"))
}

fn server_error(message: &'static [u8]) -> Reply {
    Reply::Error(ErrorReply::Server(Some(Bytes::from_static(message))))
}

#[cfg(test)]
mod tests {
    use rusty_mcrouter_protocol::test_support::{get, server_error};
    use tokio::sync::mpsc;

    use super::*;

    #[tokio::test]
    async fn closed_mailbox_returns_proxy_unavailable() {
        let (request_tx, request_rx) = mpsc::channel(1);
        drop(request_rx);

        assert_eq!(
            send_request(&request_tx, get(b"key")).await,
            server_error(b"proxy unavailable"),
        );
    }

    #[tokio::test]
    async fn dropped_accepted_request_returns_proxy_dropped_request() {
        let (request_tx, mut request_rx) = mpsc::channel(1);
        let task = tokio::spawn(async move { send_request(&request_tx, get(b"key")).await });
        let request = request_rx.recv().await.unwrap();
        assert_eq!(request.request.key().as_bytes(), b"key");
        drop(request);

        assert_eq!(task.await.unwrap(), server_error(b"proxy dropped request"));
    }
}
