use bytes::{Bytes, BytesMut};

use crate::meta::{
    DecodedMetaCommand, MetaReplyDecoder, MetaReplyEncoder, MetaReplyExpectation, MetaReplyPlan,
    MetaRequestDecoder, MetaRequestEncoder,
};
use crate::reply::{
    ArithmeticReply, ArithmeticResult, DebugHit, DebugReply, ErrorReply, GetHit, GetReply,
    StoreReply, StoreResult,
};
use crate::request::{ArithmeticRequest, DebugRequest, DeleteRequest, GetRequest, StoreRequest};
use crate::{Key, Reply, Request};

#[track_caller]
pub fn key(raw: &[u8]) -> Key {
    Key::new(Bytes::copy_from_slice(raw)).expect("test key must be valid")
}

#[track_caller]
pub fn decode_command(wire: &[u8]) -> DecodedMetaCommand {
    let mut decoder = MetaRequestDecoder::new();
    let mut src = BytesMut::from(wire);
    let decoded = decoder
        .decode(&mut src)
        .expect("test command must decode")
        .expect("test command must be complete");
    assert!(src.is_empty(), "trailing bytes after one test command");
    decoded
}

#[track_caller]
pub fn command(wire: &[u8]) -> (Request, MetaReplyPlan) {
    let DecodedMetaCommand::Request {
        request,
        reply_plan,
    } = decode_command(wire)
    else {
        panic!("expected a routable request, got a session-local command");
    };
    (request, reply_plan)
}

#[track_caller]
pub fn request(wire: &[u8]) -> Request {
    command(wire).0
}

#[track_caller]
pub fn plan(wire: &[u8]) -> MetaReplyPlan {
    command(wire).1
}

#[track_caller]
pub fn encode_request(request: &Request) -> (Bytes, MetaReplyExpectation) {
    let mut out = BytesMut::new();
    let expectation = MetaRequestEncoder::new()
        .encode(request, &mut out)
        .expect("test request must encode");
    (out.freeze(), expectation)
}

#[track_caller]
pub fn backend_request(wire: &[u8]) -> Bytes {
    encode_request(&request(wire)).0
}

#[track_caller]
pub fn expectation(wire: &[u8]) -> MetaReplyExpectation {
    encode_request(&request(wire)).1
}

#[track_caller]
pub fn decode_reply(expectation: &MetaReplyExpectation, wire: &[u8]) -> Reply {
    let mut src = BytesMut::from(wire);
    let reply = MetaReplyDecoder::new()
        .decode(expectation, &mut src)
        .expect("test reply must decode")
        .expect("test reply must be complete");
    assert!(src.is_empty(), "trailing bytes after one test reply");
    reply
}

#[track_caller]
pub fn reply(command: &[u8], backend: &[u8]) -> Reply {
    let expectation = expectation(command);
    decode_reply(&expectation, backend)
}

#[track_caller]
pub fn encode_reply(reply: &Reply, plan: &MetaReplyPlan) -> Bytes {
    let mut out = BytesMut::new();
    MetaReplyEncoder::new()
        .encode(reply, plan, &mut out)
        .expect("test reply must encode");
    out.freeze()
}

#[track_caller]
pub fn response(frontend: &[u8], backend: &[u8]) -> Bytes {
    let (request, plan) = command(frontend);
    let (_, expectation) = encode_request(&request);
    let reply = decode_reply(&expectation, backend);
    encode_reply(&reply, &plan)
}

#[track_caller]
pub fn get(key: &[u8]) -> Request {
    request(&[b"mg ", key, b" v\r\n"].concat())
}

#[track_caller]
pub fn store(key: &[u8], value: &[u8]) -> Request {
    let header = format!(" {}\r\n", value.len());
    request(&[b"ms ", key, header.as_bytes(), value, b"\r\n"].concat())
}

#[track_caller]
pub fn delete(key: &[u8]) -> Request {
    request(&[b"md ", key, b"\r\n"].concat())
}

#[track_caller]
pub fn arithmetic(key: &[u8]) -> Request {
    request(&[b"ma ", key, b"\r\n"].concat())
}

#[track_caller]
pub fn debug(key: &[u8]) -> Request {
    request(&[b"me ", key, b"\r\n"].concat())
}

#[track_caller]
pub fn get_hit(value: &[u8]) -> Reply {
    let header = format!("VA {}\r\n", value.len());
    reply(
        b"mg fixture v\r\n",
        &[header.as_bytes(), value, b"\r\n"].concat(),
    )
}

#[track_caller]
pub fn get_miss() -> Reply {
    reply(b"mg fixture v\r\n", b"EN\r\n")
}

#[track_caller]
pub fn store_success() -> Reply {
    reply(b"ms fixture 0\r\n\r\n", b"HD\r\n")
}

#[track_caller]
pub fn delete_success() -> Reply {
    reply(b"md fixture\r\n", b"HD\r\n")
}

#[track_caller]
pub fn arithmetic_value(value: u64) -> Reply {
    let value = value.to_string();
    let backend = format!("VA {}\r\n{value}\r\n", value.len());
    reply(b"ma fixture v\r\n", backend.as_bytes())
}

#[track_caller]
pub fn debug_miss() -> Reply {
    reply(b"me fixture\r\n", b"EN\r\n")
}

#[track_caller]
pub fn protocol_error() -> Reply {
    reply(b"mg fixture\r\n", b"ERROR\r\n")
}

#[track_caller]
pub fn client_error(message: &[u8]) -> Reply {
    reply(
        b"mg fixture\r\n",
        &[b"CLIENT_ERROR ", message, b"\r\n"].concat(),
    )
}

#[track_caller]
pub fn bare_client_error() -> Reply {
    reply(b"mg fixture\r\n", b"CLIENT_ERROR\r\n")
}

#[track_caller]
pub fn server_error(message: &[u8]) -> Reply {
    reply(
        b"mg fixture\r\n",
        &[b"SERVER_ERROR ", message, b"\r\n"].concat(),
    )
}

#[track_caller]
pub fn bare_server_error() -> Reply {
    reply(b"mg fixture\r\n", b"SERVER_ERROR\r\n")
}

#[track_caller]
pub fn version(payload: &[u8]) -> Reply {
    decode_reply(
        &MetaReplyExpectation::Version,
        &[b"VERSION ".as_slice(), payload, b"\r\n"].concat(),
    )
}

#[track_caller]
pub fn expect_get_request(request: Request) -> GetRequest {
    let Request::Get(get) = request else {
        panic!("expected get request, got {request:?}");
    };
    get
}

#[track_caller]
pub fn expect_store_request(request: Request) -> StoreRequest {
    let Request::Store(store) = request else {
        panic!("expected store request, got {request:?}");
    };
    store
}

#[track_caller]
pub fn expect_delete_request(request: Request) -> DeleteRequest {
    let Request::Delete(delete) = request else {
        panic!("expected delete request, got {request:?}");
    };
    delete
}

#[track_caller]
pub fn expect_arithmetic_request(request: Request) -> ArithmeticRequest {
    let Request::Arithmetic(arithmetic) = request else {
        panic!("expected arithmetic request, got {request:?}");
    };
    arithmetic
}

#[track_caller]
pub fn expect_debug_request(request: Request) -> DebugRequest {
    let Request::Debug(debug) = request else {
        panic!("expected debug request, got {request:?}");
    };
    debug
}

#[track_caller]
pub fn expect_get_hit(reply: Reply) -> GetHit {
    let Reply::Get(GetReply::Hit(hit)) = reply else {
        panic!("expected get hit, got {reply:?}");
    };
    hit
}

#[track_caller]
pub fn expect_get_value(reply: Reply) -> Bytes {
    expect_get_hit(reply)
        .value
        .expect("expected get hit with value")
}

#[track_caller]
pub fn expect_store_success(reply: Reply) -> StoreResult {
    let Reply::Store(StoreReply::Success(result)) = reply else {
        panic!("expected successful store, got {reply:?}");
    };
    result
}

#[track_caller]
pub fn expect_arithmetic_success(reply: Reply) -> ArithmeticResult {
    let Reply::Arithmetic(ArithmeticReply::Success(result)) = reply else {
        panic!("expected successful arithmetic reply, got {reply:?}");
    };
    result
}

#[track_caller]
pub fn expect_debug_hit(reply: Reply) -> DebugHit {
    let Reply::Debug(DebugReply::Hit(hit)) = reply else {
        panic!("expected debug hit, got {reply:?}");
    };
    hit
}

#[track_caller]
pub fn expect_error(reply: Reply) -> ErrorReply {
    let Reply::Error(error) = reply else {
        panic!("expected protocol error, got {reply:?}");
    };
    error
}

#[track_caller]
pub fn expect_version(reply: Reply) -> Bytes {
    let Reply::Version(version) = reply else {
        panic!("expected version reply, got {reply:?}");
    };
    version
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::GetSuccessShape;
    use crate::request::StoreMode;

    #[test]
    fn builds_fixtures_from_wire_text() {
        let (request, plan) = command(b"mg user:1 v c q Otag\r\n");
        let get = expect_get_request(request);
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

        let get = expect_get_request(get(b"user:1"));
        assert_eq!(get.key.as_bytes(), b"user:1");
        assert!(get.return_value);

        let store = expect_store_request(store(b"user:1", b"value"));
        assert_eq!(store.value, b"value".as_slice());
        assert_eq!(store.mode, StoreMode::Set);

        let delete = expect_delete_request(delete(b"user:1"));
        assert_eq!(delete.key.as_bytes(), b"user:1");

        let arithmetic = expect_arithmetic_request(arithmetic(b"user:1"));
        assert_eq!(arithmetic.key.as_bytes(), b"user:1");

        let debug = expect_debug_request(debug(b"user:1"));
        assert_eq!(debug.key.as_bytes(), b"user:1");
    }

    #[test]
    fn exposes_each_codec_stage_and_complete_response() {
        assert!(matches!(
            decode_command(b"mn\r\n"),
            DecodedMetaCommand::NoOp
        ));

        let request = request(b"mg /region/cluster/key v c Otag\r\n");
        let (backend, reply_expectation) = encode_request(&request);
        assert_eq!(backend, b"mg key v c\r\n".as_slice());
        assert_eq!(
            backend_request(b"mg /region/cluster/key v c Otag\r\n"),
            backend
        );
        assert_eq!(
            reply_expectation,
            expectation(b"mg /region/cluster/key v c Otag\r\n")
        );

        let reply = decode_reply(&reply_expectation, b"VA 3 c42\r\nfoo\r\n");
        let plan = plan(b"mg /region/cluster/key v c Otag\r\n");
        assert_eq!(
            encode_reply(&reply, &plan),
            b"VA 3 c42 Otag\r\nfoo\r\n".as_slice()
        );
        assert_eq!(
            response(
                b"mg /region/cluster/key v c Otag\r\n",
                b"VA 3 c42\r\nfoo\r\n"
            ),
            b"VA 3 c42 Otag\r\nfoo\r\n".as_slice()
        );

        assert_eq!(expect_version(version(b"1.6.39")), b"1.6.39".as_slice());
    }

    #[test]
    fn builds_and_extracts_common_replies() {
        assert_eq!(expect_get_value(get_hit(b"value")), b"value".as_slice());
        assert_eq!(get_miss(), Reply::Get(GetReply::Miss));
        assert_eq!(
            expect_store_success(store_success()),
            StoreResult::default()
        );
        assert_eq!(delete_success(), reply(b"md fixture\r\n", b"HD\r\n"));
        assert_eq!(
            expect_arithmetic_success(arithmetic_value(42)).value,
            Some(42)
        );
        assert_eq!(debug_miss(), Reply::Debug(DebugReply::Miss));
        assert_eq!(expect_error(protocol_error()), ErrorReply::Error);
        assert_eq!(
            expect_error(client_error(b"bad command")),
            ErrorReply::Client(Some(Bytes::from_static(b"bad command")))
        );
        assert_eq!(expect_error(bare_client_error()), ErrorReply::Client(None));
        assert_eq!(
            expect_error(server_error(b"warming up")),
            ErrorReply::Server(Some(Bytes::from_static(b"warming up")))
        );
        assert_eq!(expect_error(bare_server_error()), ErrorReply::Server(None));
    }
}
