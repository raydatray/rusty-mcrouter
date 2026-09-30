use bytes::BytesMut;
use thiserror::Error;

use crate::key::MAX_KEY_BYTES;
use crate::meta::{command, wire, MetaReplyExpectation};
use crate::{Key, Request};

#[derive(Debug, Default)]
pub struct MetaRequestEncoder;

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum MetaRequestEncodeError {
    #[error("backend key is empty after removing its routing prefix")]
    EmptyBackendKey,

    #[error("base64-encoded backend key exceeds the {maximum}-byte limit")]
    EncodedKeyTooLong { maximum: usize },

    #[error("Meta request value exceeds the {maximum}-byte limit")]
    ValueTooLarge { maximum: usize },

    #[error("Meta request exceeds the {maximum}-byte line limit")]
    FrameTooLarge { maximum: usize },
}

impl MetaRequestEncoder {
    pub const fn new() -> Self {
        Self
    }

    pub fn encode(
        &self,
        request: &Request,
        out: &mut BytesMut,
    ) -> Result<MetaReplyExpectation, MetaRequestEncodeError> {
        let checkpoint = out.len();
        let result = match request {
            Request::Get(request) => command::get::encode_request(request, out),
            Request::Store(request) => command::store::encode_request(request, out),
            Request::Delete(request) => command::delete::encode_request(request, out),
            Request::Arithmetic(request) => command::arithmetic::encode_request(request, out),
            Request::Debug(request) => command::debug::encode_request(request, out),
        };
        if result.is_err() {
            out.truncate(checkpoint);
        }
        result
    }

    /// Encodes a backend-only health probe. Takes no `Request` since it cannot enter routing graph.
    pub fn encode_version_probe(&self, out: &mut BytesMut) -> MetaReplyExpectation {
        out.extend_from_slice(b"version\r\n");
        MetaReplyExpectation::Version
    }
}

/// Writes the backend form of `key`: the routing prefix is stripped, and a
/// binary key is base64-encoded (the returned bool asks for the `b` flag).
pub fn write_backend_key(out: &mut BytesMut, key: &Key) -> Result<bool, MetaRequestEncodeError> {
    let key = key.key_without_routing_prefix();
    if key.is_empty() {
        return Err(MetaRequestEncodeError::EmptyBackendKey);
    }

    if is_text_key(key) {
        out.extend_from_slice(key);
        return Ok(false);
    }

    wire::write_base64_key(out, key).map_err(encoded_key_too_long)?;
    Ok(true)
}

fn is_text_key(key: &[u8]) -> bool {
    !key.iter().any(|byte| *byte <= b' ' || *byte == 0x7f)
}

pub fn write_u64_flag(out: &mut BytesMut, flag: u8, value: u64) {
    wire::write_bare_flag(out, flag);
    wire::write_u64(out, value);
}

pub fn write_i32_flag(out: &mut BytesMut, flag: u8, value: i32) {
    wire::write_bare_flag(out, flag);
    wire::write_i64(out, i64::from(value));
}

pub fn write_mode_flag(out: &mut BytesMut, mode: u8) {
    wire::write_bare_flag(out, b'M');
    out.extend_from_slice(&[mode]);
}

fn encoded_key_too_long(_: wire::EncodedKeyTooLong) -> MetaRequestEncodeError {
    MetaRequestEncodeError::EncodedKeyTooLong {
        maximum: MAX_KEY_BYTES,
    }
}

pub fn command_line_too_long(error: wire::LineTooLong) -> MetaRequestEncodeError {
    MetaRequestEncodeError::FrameTooLarge {
        maximum: error.maximum,
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::test_support::{
        backend_request, encode_request, expect_get_request, expect_store_request, key, request,
        store,
    };
    use crate::{
        meta::{request_decoder::MAX_VALUE_BYTES, GetSuccessShape},
        request::GetTemporalInstruction,
    };

    #[test]
    fn encodes_basic_get() {
        assert_eq!(backend_request(b"mg key\r\n"), b"mg key\r\n".as_slice());
    }

    #[test]
    fn encodes_basic_store() {
        assert_eq!(
            backend_request(b"ms key 3\r\nfoo\r\n"),
            b"ms key 3\r\nfoo\r\n".as_slice()
        );
    }

    #[test]
    fn encodes_basic_delete() {
        assert_eq!(backend_request(b"md key\r\n"), b"md key\r\n".as_slice());
    }

    #[test]
    fn encodes_basic_arithmetic() {
        assert_eq!(backend_request(b"ma key\r\n"), b"ma key\r\n".as_slice());
    }

    #[test]
    fn encodes_basic_debug() {
        assert_eq!(backend_request(b"me key\r\n"), b"me key\r\n".as_slice());
    }

    #[test]
    fn encodes_version_probe_and_returns_its_expectation() {
        let mut out = BytesMut::from(&b"existing"[..]);

        let expectation = MetaRequestEncoder::new().encode_version_probe(&mut out);

        assert_eq!(expectation, MetaReplyExpectation::Version);
        assert_eq!(out, b"existingversion\r\n".as_slice());
    }

    #[test]
    fn returns_debug_reply_expectation_with_backend_key() {
        let request = request(b"me /region/cluster/key\r\n");
        let (out, expectation) = encode_request(&request);

        assert_eq!(
            expectation,
            MetaReplyExpectation::Debug {
                key: Bytes::from_static(b"key"),
            }
        );
        assert_eq!(out, b"me key\r\n".as_slice());
    }

    #[test]
    fn base64_encodes_binary_debug_key() {
        let request = request(b"me AAE= b\r\n");
        let (out, expectation) = encode_request(&request);

        assert_eq!(
            expectation,
            MetaReplyExpectation::Debug {
                key: Bytes::from_static(b"\0\x01"),
            }
        );
        assert_eq!(out, b"me AAE= b\r\n".as_slice());
    }

    #[test]
    fn returns_arithmetic_reply_expectation() {
        let header = request(b"ma key\r\n");
        assert_eq!(
            encode_request(&header).1,
            MetaReplyExpectation::Arithmetic {
                value: false,
                cas: false,
                ttl: false,
            }
        );

        let value = request(b"ma key v\r\n");
        assert_eq!(
            encode_request(&value).1,
            MetaReplyExpectation::Arithmetic {
                value: true,
                cas: false,
                ttl: false,
            }
        );
    }

    #[test]
    fn strips_arithmetic_frontend_metadata_and_routing_prefix() {
        assert_eq!(
            backend_request(
                b"ma /region/cluster/key Otag N30 J5 D2 T60 MD q t c v k C42 E43 Pproxy Lpath/\r\n",
            ),
            b"ma key v c C42 E43 J5 D2 MD N30 T60 t\r\n".as_slice()
        );
    }

    #[test]
    fn canonicalizes_arithmetic_modes_and_defaults() {
        assert_eq!(
            backend_request(b"ma key MI D1\r\n"),
            b"ma key\r\n".as_slice()
        );
        assert_eq!(backend_request(b"ma key M+\r\n"), b"ma key\r\n".as_slice());
        assert_eq!(
            backend_request(b"ma key M-\r\n"),
            b"ma key MD\r\n".as_slice()
        );
    }

    #[test]
    fn preserves_arithmetic_temporal_order() {
        assert_eq!(
            backend_request(b"ma key t T60 N30\r\n"),
            b"ma key t T60 N30\r\n".as_slice()
        );
    }

    #[test]
    fn base64_encodes_binary_arithmetic_key() {
        assert_eq!(
            backend_request(b"ma AAE= b\r\n"),
            b"ma AAE= b\r\n".as_slice()
        );
    }

    #[test]
    fn returns_delete_reply_expectation() {
        let request = request(b"md key\r\n");

        assert_eq!(encode_request(&request).1, MetaReplyExpectation::Delete);
    }

    #[test]
    fn strips_delete_frontend_metadata_and_routing_prefix() {
        assert_eq!(
            backend_request(
                b"md /region/cluster/key C42 E43 F7 I k Otag q T60 x Pproxy Lpath/\r\n",
            ),
            b"md key C42 E43 F7 I T60 x\r\n".as_slice()
        );
    }

    #[test]
    fn base64_encodes_binary_delete_key() {
        assert_eq!(
            backend_request(b"md AAE= b\r\n"),
            b"md AAE= b\r\n".as_slice()
        );
    }

    #[test]
    fn returns_store_reply_expectation() {
        let request = request(b"ms key 3\r\nfoo\r\n");

        assert_eq!(
            encode_request(&request).1,
            MetaReplyExpectation::Store {
                cas: false,
                size: false,
            }
        );
    }

    #[test]
    fn strips_store_frontend_metadata_and_routing_prefix() {
        assert_eq!(
            backend_request(
                b"ms /region/cluster/key 3 c C42 E43 F7 I k Otag q s T60 MA N30 Pproxy Lpath/\r\nfoo\r\n",
            ),
            b"ms key 3 c s C42 E43 F7 I T60 MA N30\r\nfoo\r\n".as_slice()
        );
    }

    #[test]
    fn encodes_store_modes_canonically() {
        for (wire_mode, expected) in [
            (b'S', b"ms key 0\r\n\r\n".as_slice()),
            (b'E', b"ms key 0 ME\r\n\r\n".as_slice()),
            (b'R', b"ms key 0 MR\r\n\r\n".as_slice()),
            (b'A', b"ms key 0 MA\r\n\r\n".as_slice()),
            (b'P', b"ms key 0 MP\r\n\r\n".as_slice()),
        ] {
            let input = [b"ms key 0 M".as_slice(), &[wire_mode], b"\r\n\r\n"].concat();
            assert_eq!(backend_request(&input), expected);
        }
    }

    #[test]
    fn encodes_binary_store_key_and_value() {
        assert_eq!(
            backend_request(b"ms AAE= 4 b\r\na\0b\n\r\n"),
            b"ms AAE= 4 b\r\na\0b\n\r\n".as_slice()
        );
    }

    #[test]
    fn rejects_oversized_store_value_atomically() {
        let mut store = expect_store_request(store(b"key", b""));
        store.value = Bytes::from(vec![b'x'; MAX_VALUE_BYTES + 1]);
        let request = Request::Store(store);
        let mut out = BytesMut::from(&b"existing"[..]);

        assert_eq!(
            MetaRequestEncoder::new().encode(&request, &mut out),
            Err(MetaRequestEncodeError::ValueTooLarge {
                maximum: MAX_VALUE_BYTES,
            })
        );
        assert_eq!(out, b"existing".as_slice());
    }

    #[test]
    fn returns_get_reply_shape() {
        let header = request(b"mg key\r\n");
        assert_eq!(
            encode_request(&header).1,
            MetaReplyExpectation::Get(GetSuccessShape::Header)
        );

        let value = request(b"mg key v\r\n");
        assert_eq!(
            encode_request(&value).1,
            MetaReplyExpectation::Get(GetSuccessShape::Value)
        );

        let conditional = request(b"mg key v C42\r\n");
        assert_eq!(
            encode_request(&conditional).1,
            MetaReplyExpectation::Get(GetSuccessShape::HeaderOrValue)
        );
    }

    #[test]
    fn strips_frontend_metadata_and_routing_prefix() {
        assert_eq!(
            backend_request(
                b"mg /region/cluster/key Otag s N30 T40 t R50 c f h k l C99 E100 u v q Pproxy Lpath/\r\n",
            ),
            b"mg key v f c s h l C99 E100 u N30 T40 t R50\r\n".as_slice()
        );
    }

    #[test]
    fn preserves_hash_stop_suffix_on_backend_key() {
        assert_eq!(
            backend_request(b"mg /region/cluster/key|#|suffix\r\n"),
            b"mg key|#|suffix\r\n".as_slice()
        );
    }

    #[test]
    fn base64_encodes_binary_backend_key_without_allocation() {
        assert_eq!(
            backend_request(b"mg AAE= b v\r\n"),
            b"mg AAE= b v\r\n".as_slice()
        );
    }

    #[test]
    fn encodes_numeric_boundaries() {
        let mut request = expect_get_request(request(b"mg key\r\n"));
        request.check_cas = Some(u64::MAX);
        request.override_cas = Some(0);
        request
            .temporal
            .push(GetTemporalInstruction::Vivify(i32::MIN))
            .unwrap();
        request
            .temporal
            .push(GetTemporalInstruction::UpdateTtl(i32::MAX))
            .unwrap();

        assert_eq!(
            encode_request(&Request::Get(request)).0,
            b"mg key C18446744073709551615 E0 N-2147483648 T2147483647\r\n".as_slice()
        );
    }

    #[test]
    fn rejects_empty_backend_key_atomically() {
        let request = request(b"mg /region/cluster/\r\n");
        let mut out = BytesMut::from(&b"existing"[..]);

        assert_eq!(
            MetaRequestEncoder::new().encode(&request, &mut out),
            Err(MetaRequestEncodeError::EmptyBackendKey)
        );
        assert_eq!(out, b"existing".as_slice());
    }

    #[test]
    fn rejects_binary_key_that_expands_past_wire_limit() {
        let mut request = expect_get_request(request(b"mg fixture\r\n"));
        request.key = key(&[0; 188]);
        let request = Request::Get(request);
        let mut out = BytesMut::new();

        assert_eq!(
            MetaRequestEncoder::new().encode(&request, &mut out),
            Err(MetaRequestEncodeError::EncodedKeyTooLong {
                maximum: MAX_KEY_BYTES,
            })
        );
    }
}
