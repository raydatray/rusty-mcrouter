use bytes::Bytes;
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
            Some(m) => Reply::ServerError(m.clone()),
            None => Reply::Error,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn with_message_returns_server_error_with_message() {
        let r = ErrorRoute::new(Some("boom".to_string()));
        let reply = r.route(Request::Get { keys: vec![] }).await.unwrap();
        assert_eq!(reply, Reply::ServerError(Bytes::from_static(b"boom")));
    }

    #[tokio::test]
    async fn without_message_returns_bare_error() {
        let r = ErrorRoute::new(None);
        let reply = r.route(Request::Get { keys: vec![] }).await.unwrap();
        assert_eq!(reply, Reply::Error);
    }

    #[tokio::test]
    async fn ignores_request_payload() {
        let r = ErrorRoute::new(Some("nope".to_string()));
        let reply_get = r.route(Request::Get { keys: vec![] }).await.unwrap();
        let reply_set = r
            .route(Request::Set {
                key: Bytes::from_static(b"k"),
                flags: 0,
                exptime: 0,
                data: Bytes::from_static(b"v"),
            })
            .await
            .unwrap();
        assert_eq!(reply_get, reply_set);
    }
}
