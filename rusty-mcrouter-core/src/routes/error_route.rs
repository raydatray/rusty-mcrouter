use bytes::Bytes;
use rusty_mcrouter_protocol::reply::ErrorReply;
use rusty_mcrouter_protocol::{Reply, Request};

use super::{Result, Route};

pub struct ErrorRoute {
    message: Option<Bytes>,
}

impl ErrorRoute {
    pub fn new(message: Option<String>) -> Self {
        Self {
            message: message.map(Bytes::from),
        }
    }
}

impl Route for ErrorRoute {
    async fn route(&self, _req: Request) -> Result<Reply> {
        Ok(match &self.message {
            Some(m) => Reply::Error(ErrorReply::Server(Some(m.clone()))),
            None => Reply::Error(ErrorReply::Error),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusty_mcrouter_protocol::test_support::{get, store};

    #[tokio::test]
    async fn with_message_returns_server_error_with_message() {
        let r = ErrorRoute::new(Some("boom".to_string()));
        let reply = r.route(get(b"k")).await.unwrap();
        assert_eq!(
            reply,
            Reply::Error(ErrorReply::Server(Some(Bytes::from_static(b"boom"))))
        );
    }

    #[tokio::test]
    async fn without_message_returns_bare_error() {
        let r = ErrorRoute::new(None);
        let reply = r.route(get(b"k")).await.unwrap();
        assert_eq!(reply, Reply::Error(ErrorReply::Error));
    }

    #[tokio::test]
    async fn ignores_request_payload() {
        let r = ErrorRoute::new(Some("nope".to_string()));
        let reply_get = r.route(get(b"k")).await.unwrap();
        let reply_store = r.route(store(b"k", b"v")).await.unwrap();
        assert_eq!(reply_get, reply_store);
    }
}
