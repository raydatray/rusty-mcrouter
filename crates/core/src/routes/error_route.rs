use bytes::Bytes;
use rusty_mcrouter_protocol::reply::ErrorReply;
use rusty_mcrouter_protocol::{Reply, Request};

use crate::routes::{Result, Route};
use crate::RouteContext;

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
    async fn route(&self, _context: &RouteContext, _request: Request) -> Result<Reply> {
        Ok(match &self.message {
            Some(m) => Reply::Error(ErrorReply::Server(Some(m.clone()))),
            None => Reply::Error(ErrorReply::Error),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusty_mcrouter_protocol::test_support::{get, protocol_error, server_error, store};

    use crate::context::test_routing_state;

    async fn execute(route: &ErrorRoute, request: Request) -> Result<Reply> {
        let state = test_routing_state();
        let context = state.context();
        route.route(&context, request).await
    }

    #[tokio::test]
    async fn with_message_returns_server_error_with_message() {
        let r = ErrorRoute::new(Some("boom".to_string()));
        let reply = execute(&r, get(b"k")).await.unwrap();
        assert_eq!(reply, server_error(b"boom"));
    }

    #[tokio::test]
    async fn without_message_returns_bare_error() {
        let r = ErrorRoute::new(None);
        let reply = execute(&r, get(b"k")).await.unwrap();
        assert_eq!(reply, protocol_error());
    }

    #[tokio::test]
    async fn ignores_request_payload() {
        let r = ErrorRoute::new(Some("nope".to_string()));
        let reply_get = execute(&r, get(b"k")).await.unwrap();
        let reply_store = execute(&r, store(b"k", b"v")).await.unwrap();
        assert_eq!(reply_get, reply_store);
    }
}
