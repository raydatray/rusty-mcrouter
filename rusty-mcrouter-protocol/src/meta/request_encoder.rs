use bytes::BytesMut;
use thiserror::Error;

use crate::{
    key::{Key, MAX_KEY_BYTES},
    request::{ArithmeticMode, ArithmeticRequest, ArithmeticTemporalInstruction, Request},
};

use super::{command, wire, MetaReplyExpectation, MAX_COMMAND_LINE_BYTES};

#[derive(Debug, Default)]
pub struct MetaRequestEncoder;

impl MetaRequestEncoder {
    pub const fn new() -> Self {
        Self
    }

    /// Appends one loud backend request. On error, `out` is unchanged.
    pub fn encode(
        &self,
        request: &Request,
        out: &mut BytesMut,
    ) -> Result<MetaReplyExpectation, MetaRequestEncodeError> {
        let checkpoint = out.len();
        let result = match request {
            Request::Get(request) => {
                command::get::encode_request(request, out).map(MetaReplyExpectation::Get)
            }
            Request::Store(request) => {
                command::store::encode_request(request, out).map(|()| MetaReplyExpectation::Store {
                    cas: request.return_cas,
                    size: request.return_size,
                })
            }
            Request::Delete(request) => {
                command::delete::encode_request(request, out).map(|()| MetaReplyExpectation::Delete)
            }
            Request::Arithmetic(request) => {
                encode_arithmetic(request, out).map(|()| MetaReplyExpectation::Arithmetic {
                    value: request.return_value,
                    cas: request.return_cas,
                    ttl: request.temporal.iter().any(|instruction| {
                        matches!(instruction, ArithmeticTemporalInstruction::ReturnTtl)
                    }),
                })
            }
            Request::Debug(request) => encode_debug(request, out),
        };
        if result.is_err() {
            out.truncate(checkpoint);
        }
        result
    }
}

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

fn encode_arithmetic(
    request: &ArithmeticRequest,
    out: &mut BytesMut,
) -> Result<(), MetaRequestEncodeError> {
    let line_start = out.len();
    out.extend_from_slice(b"ma ");
    let key_is_base64 = write_backend_key(out, &request.key)?;

    if key_is_base64 {
        wire::write_bare_flag(out, b'b');
    }
    if request.return_value {
        wire::write_bare_flag(out, b'v');
    }
    if request.return_cas {
        wire::write_bare_flag(out, b'c');
    }
    if let Some(cas) = request.compare_cas {
        write_u64_flag(out, b'C', cas);
    }
    if let Some(cas) = request.override_cas {
        write_u64_flag(out, b'E', cas);
    }
    if let Some(initial) = request.initial_value {
        write_u64_flag(out, b'J', initial);
    }
    if request.delta != 1 {
        write_u64_flag(out, b'D', request.delta);
    }
    if request.mode == ArithmeticMode::Decrement {
        write_mode_flag(out, b'D');
    }
    for instruction in request.temporal.iter() {
        match instruction {
            ArithmeticTemporalInstruction::Vivify(ttl) => write_i32_flag(out, b'N', *ttl),
            ArithmeticTemporalInstruction::UpdateTtl(ttl) => write_i32_flag(out, b'T', *ttl),
            ArithmeticTemporalInstruction::ReturnTtl => wire::write_bare_flag(out, b't'),
        }
    }

    wire::finish_line(out, line_start, MAX_COMMAND_LINE_BYTES).map_err(command_line_too_long)?;
    Ok(())
}

fn encode_debug(
    request: &crate::request::DebugRequest,
    out: &mut BytesMut,
) -> Result<MetaReplyExpectation, MetaRequestEncodeError> {
    let line_start = out.len();
    out.extend_from_slice(b"me ");
    let key_is_base64 = write_backend_key(out, &request.key)?;
    if key_is_base64 {
        wire::write_bare_flag(out, b'b');
    }
    wire::finish_line(out, line_start, MAX_COMMAND_LINE_BYTES).map_err(command_line_too_long)?;

    Ok(MetaReplyExpectation::Debug {
        key: request.key.clone_without_routing_prefix(),
    })
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

    let mut scratch = [0; wire::MAX_BASE64_KEY_BYTES];
    let encoded = wire::encode_base64_key(key, &mut scratch).map_err(|_| {
        MetaRequestEncodeError::EncodedKeyTooLong {
            maximum: MAX_KEY_BYTES,
        }
    })?;
    out.extend_from_slice(encoded);
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

pub fn command_line_too_long(error: wire::LineTooLong) -> MetaRequestEncodeError {
    MetaRequestEncodeError::FrameTooLarge {
        maximum: error.maximum,
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::{
        meta::{DecodedMetaCommand, GetSuccessShape, MetaRequestDecoder, MAX_VALUE_BYTES},
        request::{
            GetRequest, GetTemporalInstruction, GetTemporalInstructions, StoreMode, StoreRequest,
        },
    };

    fn parse(input: &[u8]) -> Request {
        let mut decoder = MetaRequestDecoder::new();
        let mut src = BytesMut::from(input);
        let DecodedMetaCommand::Request { request, .. } =
            decoder.decode(&mut src).unwrap().unwrap()
        else {
            panic!("expected request");
        };
        request
    }

    fn encode(request: &Request) -> Result<BytesMut, MetaRequestEncodeError> {
        let mut out = BytesMut::new();
        let _expectation = MetaRequestEncoder::new().encode(request, &mut out)?;
        Ok(out)
    }

    fn get(key: Key) -> GetRequest {
        GetRequest {
            key,
            return_value: false,
            return_client_flags: false,
            return_cas: false,
            return_size: false,
            return_hit_state: false,
            return_last_access: false,
            check_cas: None,
            override_cas: None,
            no_lru_bump: false,
            temporal: GetTemporalInstructions::default(),
        }
    }

    fn store(key: Key, value: Bytes) -> StoreRequest {
        StoreRequest {
            key,
            value,
            return_cas: false,
            return_size: false,
            mode: StoreMode::Set,
            client_flags: None,
            ttl: None,
            compare_cas: None,
            override_cas: None,
            invalidate: false,
            vivify_ttl: None,
        }
    }

    #[test]
    fn encodes_basic_get() {
        assert_eq!(
            encode(&parse(b"mg key\r\n")).unwrap(),
            b"mg key\r\n".as_slice()
        );
    }

    #[test]
    fn encodes_basic_store() {
        let request = parse(b"ms key 3\r\nfoo\r\n");
        assert_eq!(encode(&request).unwrap(), b"ms key 3\r\nfoo\r\n".as_slice());
    }

    #[test]
    fn encodes_basic_delete() {
        let request = parse(b"md key\r\n");
        assert_eq!(encode(&request).unwrap(), b"md key\r\n".as_slice());
    }

    #[test]
    fn encodes_basic_arithmetic() {
        let request = parse(b"ma key\r\n");
        assert_eq!(encode(&request).unwrap(), b"ma key\r\n".as_slice());
    }

    #[test]
    fn encodes_basic_debug() {
        let request = parse(b"me key\r\n");
        assert_eq!(encode(&request).unwrap(), b"me key\r\n".as_slice());
    }

    #[test]
    fn returns_debug_reply_expectation_with_backend_key() {
        let request = parse(b"me /region/cluster/key\r\n");
        let mut out = BytesMut::new();

        assert_eq!(
            MetaRequestEncoder::new().encode(&request, &mut out),
            Ok(MetaReplyExpectation::Debug {
                key: Bytes::from_static(b"key"),
            })
        );
        assert_eq!(out, b"me key\r\n".as_slice());
    }

    #[test]
    fn base64_encodes_binary_debug_key() {
        let request = parse(b"me AAE= b\r\n");
        let mut out = BytesMut::new();

        assert_eq!(
            MetaRequestEncoder::new().encode(&request, &mut out),
            Ok(MetaReplyExpectation::Debug {
                key: Bytes::from_static(b"\0\x01"),
            })
        );
        assert_eq!(out, b"me AAE= b\r\n".as_slice());
    }

    #[test]
    fn returns_arithmetic_reply_expectation() {
        let encoder = MetaRequestEncoder::new();
        let mut out = BytesMut::new();
        let header = parse(b"ma key\r\n");
        assert_eq!(
            encoder.encode(&header, &mut out),
            Ok(MetaReplyExpectation::Arithmetic {
                value: false,
                cas: false,
                ttl: false,
            })
        );

        out.clear();
        let value = parse(b"ma key v\r\n");
        assert_eq!(
            encoder.encode(&value, &mut out),
            Ok(MetaReplyExpectation::Arithmetic {
                value: true,
                cas: false,
                ttl: false,
            })
        );
    }

    #[test]
    fn strips_arithmetic_frontend_metadata_and_routing_prefix() {
        let request = parse(
            b"ma /region/cluster/key Otag N30 J5 D2 T60 MD q t c v k C42 E43 Pproxy Lpath/\r\n",
        );

        assert_eq!(
            encode(&request).unwrap(),
            b"ma key v c C42 E43 J5 D2 MD N30 T60 t\r\n".as_slice()
        );
    }

    #[test]
    fn canonicalizes_arithmetic_modes_and_defaults() {
        assert_eq!(
            encode(&parse(b"ma key MI D1\r\n")).unwrap(),
            b"ma key\r\n".as_slice()
        );
        assert_eq!(
            encode(&parse(b"ma key M+\r\n")).unwrap(),
            b"ma key\r\n".as_slice()
        );
        assert_eq!(
            encode(&parse(b"ma key M-\r\n")).unwrap(),
            b"ma key MD\r\n".as_slice()
        );
    }

    #[test]
    fn preserves_arithmetic_temporal_order() {
        let request = parse(b"ma key t T60 N30\r\n");

        assert_eq!(
            encode(&request).unwrap(),
            b"ma key t T60 N30\r\n".as_slice()
        );
    }

    #[test]
    fn base64_encodes_binary_arithmetic_key() {
        let request = parse(b"ma AAE= b\r\n");

        assert_eq!(encode(&request).unwrap(), b"ma AAE= b\r\n".as_slice());
    }

    #[test]
    fn returns_delete_reply_expectation() {
        let request = parse(b"md key\r\n");
        let mut out = BytesMut::new();

        assert_eq!(
            MetaRequestEncoder::new().encode(&request, &mut out),
            Ok(MetaReplyExpectation::Delete)
        );
    }

    #[test]
    fn strips_delete_frontend_metadata_and_routing_prefix() {
        let request =
            parse(b"md /region/cluster/key C42 E43 F7 I k Otag q T60 x Pproxy Lpath/\r\n");

        assert_eq!(
            encode(&request).unwrap(),
            b"md key C42 E43 F7 I T60 x\r\n".as_slice()
        );
    }

    #[test]
    fn base64_encodes_binary_delete_key() {
        let request = parse(b"md AAE= b\r\n");

        assert_eq!(encode(&request).unwrap(), b"md AAE= b\r\n".as_slice());
    }

    #[test]
    fn returns_store_reply_expectation() {
        let request = parse(b"ms key 3\r\nfoo\r\n");
        let mut out = BytesMut::new();

        assert_eq!(
            MetaRequestEncoder::new().encode(&request, &mut out),
            Ok(MetaReplyExpectation::Store {
                cas: false,
                size: false,
            })
        );
    }

    #[test]
    fn strips_store_frontend_metadata_and_routing_prefix() {
        let request = parse(
            b"ms /region/cluster/key 3 c C42 E43 F7 I k Otag q s T60 MA N30 Pproxy Lpath/\r\nfoo\r\n",
        );

        assert_eq!(
            encode(&request).unwrap(),
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
            assert_eq!(encode(&parse(&input)).unwrap(), expected);
        }
    }

    #[test]
    fn encodes_binary_store_key_and_value() {
        let request = parse(b"ms AAE= 4 b\r\na\0b\n\r\n");

        assert_eq!(
            encode(&request).unwrap(),
            b"ms AAE= 4 b\r\na\0b\n\r\n".as_slice()
        );
    }

    #[test]
    fn rejects_oversized_store_value_atomically() {
        let request = Request::Store(store(
            Key::new(Bytes::from_static(b"key")).unwrap(),
            Bytes::from(vec![b'x'; MAX_VALUE_BYTES + 1]),
        ));
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
        let mut out = BytesMut::new();
        let encoder = MetaRequestEncoder::new();

        let header = parse(b"mg key\r\n");
        assert_eq!(
            encoder.encode(&header, &mut out),
            Ok(MetaReplyExpectation::Get(GetSuccessShape::Header))
        );

        out.clear();
        let value = parse(b"mg key v\r\n");
        assert_eq!(
            encoder.encode(&value, &mut out),
            Ok(MetaReplyExpectation::Get(GetSuccessShape::Value))
        );

        out.clear();
        let conditional = parse(b"mg key v C42\r\n");
        assert_eq!(
            encoder.encode(&conditional, &mut out),
            Ok(MetaReplyExpectation::Get(GetSuccessShape::HeaderOrValue))
        );
    }

    #[test]
    fn strips_frontend_metadata_and_routing_prefix() {
        let request = parse(
            b"mg /region/cluster/key Otag s N30 T40 t R50 c f h k l C99 E100 u v q Pproxy Lpath/\r\n",
        );

        assert_eq!(
            encode(&request).unwrap(),
            b"mg key v f c s h l C99 E100 u N30 T40 t R50\r\n".as_slice()
        );
    }

    #[test]
    fn preserves_hash_stop_suffix_on_backend_key() {
        let request = parse(b"mg /region/cluster/key|#|suffix\r\n");

        assert_eq!(encode(&request).unwrap(), b"mg key|#|suffix\r\n".as_slice());
    }

    #[test]
    fn base64_encodes_binary_backend_key_without_allocation() {
        let request = parse(b"mg AAE= b v\r\n");

        assert_eq!(encode(&request).unwrap(), b"mg AAE= b v\r\n".as_slice());
    }

    #[test]
    fn encodes_numeric_boundaries() {
        let mut request = get(Key::new(Bytes::from_static(b"key")).unwrap());
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
            encode(&Request::Get(request)).unwrap(),
            b"mg key C18446744073709551615 E0 N-2147483648 T2147483647\r\n".as_slice()
        );
    }

    #[test]
    fn rejects_empty_backend_key_atomically() {
        let request = Request::Get(get(
            Key::new(Bytes::from_static(b"/region/cluster/")).unwrap()
        ));
        let mut out = BytesMut::from(&b"existing"[..]);

        assert_eq!(
            MetaRequestEncoder::new().encode(&request, &mut out),
            Err(MetaRequestEncodeError::EmptyBackendKey)
        );
        assert_eq!(out, b"existing".as_slice());
    }

    #[test]
    fn rejects_binary_key_that_expands_past_wire_limit() {
        let request = Request::Get(get(Key::new(Bytes::from(vec![0; 188])).unwrap()));

        assert_eq!(
            encode(&request),
            Err(MetaRequestEncodeError::EncodedKeyTooLong {
                maximum: MAX_KEY_BYTES,
            })
        );
    }
}
