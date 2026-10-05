use bytes::Bytes;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MetaReplyExpectation {
    Get(GetSuccessShape),
    Store {
        cas: bool,
        size: bool,
    },
    Delete,
    Arithmetic {
        value: bool,
        cas: bool,
        ttl: bool,
    },
    /// memcached may echo it plain or base64 regardless of the request encoding
    /// so do not retain encoding
    Debug {
        key: Bytes,
    },
    Version,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GetSuccessShape {
    Header,
    Value,
    HeaderOrValue,
}
