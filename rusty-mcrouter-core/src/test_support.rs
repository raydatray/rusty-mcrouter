//! Shared `#[cfg(test)]` request builders for core's own tests.

use bytes::BytesMut;
use rusty_mcrouter_protocol::meta::{DecodedMetaCommand, MetaRequestDecoder};
use rusty_mcrouter_protocol::Request;

/// Parses one Meta command (plus its body for `ms`) into a semantic request.
pub(crate) fn req(input: &[u8]) -> Request {
    let mut decoder = MetaRequestDecoder::new();
    let mut src = BytesMut::from(input);
    let DecodedMetaCommand::Request { request, .. } = decoder.decode(&mut src).unwrap().unwrap()
    else {
        panic!("expected a routable request, got a session-local command");
    };
    assert!(src.is_empty(), "trailing bytes after one command");
    request
}

pub(crate) fn req_get(key: &[u8]) -> Request {
    req(&[b"mg ", key, b" v\r\n"].concat())
}

pub(crate) fn req_store(key: &[u8], value: &[u8]) -> Request {
    req(&[
        b"ms ",
        key,
        format!(" {}\r\n", value.len()).as_bytes(),
        value,
        b"\r\n",
    ]
    .concat())
}

pub(crate) fn req_delete(key: &[u8]) -> Request {
    req(&[b"md ", key, b"\r\n"].concat())
}
