use rusty_mcrouter_protocol::{Reply, Request};

use crate::route::{Route, RouteError};

pub struct NullRoute;

impl Route for NullRoute {
    async fn route(&self, req: Request) -> Result<Reply, RouteError> {
        Ok(match req {
            // todo - add the other dummy replies as more request types added
            Request::Get { .. } => Reply::Get { hits: vec![] },
            Request::Set { .. } => Reply::Stored,
            Request::Delete { .. } => Reply::Deleted,
            Request::Add { .. } => Reply::Stored,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[tokio::test]
    async fn returns_miss_for_get() {
        let r = NullRoute;
        let reply = r
            .route(Request::Get {
                keys: vec![Bytes::from_static(b"foo")],
            })
            .await
            .unwrap();
        assert_eq!(reply, Reply::Get { hits: vec![] });
    }

    #[tokio::test]
    async fn returns_stored_for_set() {
        let r = NullRoute;
        let reply = r
            .route(Request::Set {
                key: Bytes::from_static(b"k"),
                flags: 0,
                exptime: 0,
                data: Bytes::from_static(b"v"),
            })
            .await
            .unwrap();
        assert_eq!(reply, Reply::Stored);
    }

    #[tokio::test]
    async fn multiple_keys_in_get_still_returns_empty_hits() {
        let r = NullRoute;
        let reply = r
            .route(Request::Get {
                keys: vec![
                    Bytes::from_static(b"a"),
                    Bytes::from_static(b"b"),
                    Bytes::from_static(b"c"),
                ],
            })
            .await
            .unwrap();
        assert_eq!(reply, Reply::Get { hits: vec![] });
    }

    #[tokio::test]
    async fn returns_deleted_for_delete() {
        let r = NullRoute;
        let reply = r
            .route(Request::Delete {
                key: Bytes::from_static(b"k"),
            })
            .await
            .unwrap();
        assert_eq!(reply, Reply::Deleted);
    }

    #[tokio::test]
    async fn returns_stored_for_add() {
        let r = NullRoute;
        let reply = r
            .route(Request::Add {
                key: Bytes::from_static(b"k"),
                flags: 0,
                exptime: 0,
                data: Bytes::from_static(b"v"),
            })
            .await
            .unwrap();
        assert_eq!(reply, Reply::Stored);
    }
}
