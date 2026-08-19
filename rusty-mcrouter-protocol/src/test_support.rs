//! Wire-text fixture constructors for tests, in this crate and downstream
//! (enable the `testing` feature). The codecs are the single source of
//! truth: fixtures are built by decoding real protocol bytes, so they can
//! never drift from what the proxy actually accepts and produces.
//!
//! Every helper panics on malformed input — a bad fixture should fail loud
//! at the test that wrote it, which `#[track_caller]` points at directly.

use bytes::{Bytes, BytesMut};

use crate::meta::{
    DecodedMetaCommand, MetaReplyDecoder, MetaReplyExpectation, MetaReplyPlan, MetaRequestDecoder,
    MetaRequestEncoder,
};
use crate::{Key, Reply, Request};

/// A validated cache key.
#[track_caller]
pub fn key(raw: &[u8]) -> Key {
    Key::new(Bytes::copy_from_slice(raw)).expect("test key must be valid")
}

/// Decodes exactly one complete frontend command (line plus body for `ms`),
/// yielding both artifacts of the frontend hop.
#[track_caller]
pub fn command(wire: &[u8]) -> (Request, MetaReplyPlan) {
    let mut decoder = MetaRequestDecoder::new();
    let mut src = BytesMut::from(wire);
    let decoded = decoder
        .decode(&mut src)
        .expect("test command must decode")
        .expect("test command must be complete");
    assert!(src.is_empty(), "trailing bytes after one test command");
    let DecodedMetaCommand::Request {
        request,
        reply_plan,
    } = decoded
    else {
        panic!("expected a routable request, got a session-local command");
    };
    (request, reply_plan)
}

/// The semantic request for one command; see [`command`].
#[track_caller]
pub fn request(wire: &[u8]) -> Request {
    command(wire).0
}

/// The hop-local reply plan for one command; see [`command`].
#[track_caller]
pub fn plan(wire: &[u8]) -> MetaReplyPlan {
    command(wire).1
}

/// The reply expectation the backend hop would hold for one command:
/// decodes `wire`, then encodes the request the way the proxy would.
#[track_caller]
pub fn expectation(wire: &[u8]) -> MetaReplyExpectation {
    let mut out = BytesMut::new();
    MetaRequestEncoder::new()
        .encode(&request(wire), &mut out)
        .expect("test request must encode")
}

/// A typed reply: what the proxy would decode from `backend` bytes after
/// sending `command` upstream.
#[track_caller]
pub fn reply(command: &[u8], backend: &[u8]) -> Reply {
    let expectation = expectation(command);
    let mut src = BytesMut::from(backend);
    let reply = MetaReplyDecoder::new()
        .decode(&expectation, &mut src)
        .expect("test reply must decode")
        .expect("test reply must be complete");
    assert!(src.is_empty(), "trailing bytes after one test reply");
    reply
}

/// `mg <key> v`: the plain value-fetching get.
#[track_caller]
pub fn get(key: &[u8]) -> Request {
    request(&[b"mg ", key, b" v\r\n"].concat())
}

/// `ms <key> <len>` plus body: the plain set.
#[track_caller]
pub fn store(key: &[u8], value: &[u8]) -> Request {
    let header = format!(" {}\r\n", value.len());
    request(&[b"ms ", key, header.as_bytes(), value, b"\r\n"].concat())
}

/// `md <key>`: the plain delete.
#[track_caller]
pub fn delete(key: &[u8]) -> Request {
    request(&[b"md ", key, b"\r\n"].concat())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::GetSuccessShape;
    use crate::reply::{GetHit, GetReply};
    use crate::request::StoreMode;

    #[test]
    fn builds_fixtures_from_wire_text() {
        let (request, plan) = command(b"mg user:1 v c q Otag\r\n");
        let Request::Get(get) = request else {
            panic!("expected get");
        };
        assert!(get.return_value && get.return_cas);
        assert_eq!(plan.opaque.as_deref(), Some(b"tag".as_slice()));

        assert_eq!(
            expectation(b"mg user:1 v\r\n"),
            MetaReplyExpectation::Get(GetSuccessShape::Value)
        );

        assert_eq!(
            reply(b"mg user:1 v c\r\n", b"VA 3 c42\r\nfoo\r\n"),
            Reply::Get(GetReply::Hit(GetHit {
                value: Some(Bytes::from_static(b"foo")),
                cas: Some(42),
                ..GetHit::default()
            }))
        );
    }

    #[test]
    fn convenience_builders_produce_plain_requests() {
        assert_eq!(key(b"user:1").as_bytes(), b"user:1");

        let Request::Get(get) = get(b"user:1") else {
            panic!("expected get");
        };
        assert_eq!(get.key.as_bytes(), b"user:1");
        assert!(get.return_value);

        let Request::Store(store) = store(b"user:1", b"value") else {
            panic!("expected store");
        };
        assert_eq!(store.value, b"value".as_slice());
        assert_eq!(store.mode, StoreMode::Set);

        let Request::Delete(delete) = delete(b"user:1") else {
            panic!("expected delete");
        };
        assert_eq!(delete.key.as_bytes(), b"user:1");
    }
}
