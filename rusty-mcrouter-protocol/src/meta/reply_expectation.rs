use bytes::Bytes;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MetaReplyExpectation {
    Get(GetSuccessShape),
    Store { cas: bool, size: bool },
    Delete,
    Arithmetic { value: bool, cas: bool, ttl: bool },
    /// `key` is the backend key (routing prefix removed) used to correlate
    /// the `ME` echo. memcached may echo it plain or base64 regardless of the
    /// request encoding, so no encoding is retained here.
    Debug { key: Bytes },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GetSuccessShape {
    Header,
    Value,
    HeaderOrValue,
}
