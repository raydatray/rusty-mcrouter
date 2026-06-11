use rusty_mcrouter_protocol::{Reply, Request};

use super::{Result, Route};

pub struct NullRoute;

impl Route for NullRoute {
    async fn route(&self, req: Request) -> Result<Reply> {
        Ok(match req {
            // todo - add the other dummy replies as more request types added
            Request::Get { .. } => Reply::Get { hits: vec![] },
            Request::Set { .. }
            | Request::Add { .. }
            | Request::Replace { .. }
            | Request::Append { .. }
            | Request::Prepend { .. } => Reply::Stored,
            Request::Delete { .. } => Reply::Deleted,
            Request::Incr { .. } | Request::Decr { .. } => Reply::NotFound,
            Request::Touch { .. } => Reply::Touched,
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
                key: Bytes::from_static(b"foo"),
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

    #[tokio::test]
    async fn returns_stored_for_replace() {
        let r = NullRoute;
        let reply = r
            .route(Request::Replace {
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
    async fn returns_stored_for_append() {
        let r = NullRoute;
        let reply = r
            .route(Request::Append {
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
    async fn returns_stored_for_prepend() {
        let r = NullRoute;
        let reply = r
            .route(Request::Prepend {
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
    async fn returns_not_found_for_incr() {
        let r = NullRoute;
        let reply = r
            .route(Request::Incr {
                key: Bytes::from_static(b"k"),
                delta: 1,
            })
            .await
            .unwrap();
        assert_eq!(reply, Reply::NotFound);
    }

    #[tokio::test]
    async fn returns_not_found_for_decr() {
        let r = NullRoute;
        let reply = r
            .route(Request::Decr {
                key: Bytes::from_static(b"k"),
                delta: 1,
            })
            .await
            .unwrap();
        assert_eq!(reply, Reply::NotFound);
    }

    #[tokio::test]
    async fn returns_touched_for_touch() {
        let r = NullRoute;
        let reply = r
            .route(Request::Touch {
                key: Bytes::from_static(b"k"),
                exptime: 60,
            })
            .await
            .unwrap();
        assert_eq!(reply, Reply::Touched);
    }
}
