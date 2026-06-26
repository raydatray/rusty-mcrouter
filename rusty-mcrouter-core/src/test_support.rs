//! Shared `#[cfg(test)]` request builders for core's own tests.

use bytes::Bytes;
use rusty_mcrouter_protocol::Request;

pub(crate) fn req_get(key: &'static [u8]) -> Request {
    Request::Get {
        key: Bytes::from_static(key),
    }
}
