use bytes::{Bytes, BytesMut};
use thiserror::Error;

use crate::reply::{ErrorReply, Reply};

use super::command;
use super::tokens::{find_line, parse_usize, split_tokens, FindLine};
use super::{GetSuccessShape, MetaReplyExpectation};

pub const MAX_REPLY_LINE_BYTES: usize = 32 * 1024;
pub const MAX_REPLY_VALUE_BYTES: usize = 1024 * 1024;
pub const MAX_DEBUG_FIELDS: usize = 64;

pub const INVALID_RESPONSE: &str = "invalid Meta backend response";
pub const SHAPE_MISMATCH: &str = "Meta backend response does not match request";

/// A stateless decoder: each call frames one complete reply — the line plus,
/// for an expected `VA`, its value block — and parses it in a single pass.
#[derive(Debug, Default)]
pub struct MetaReplyDecoder;

impl MetaReplyDecoder {
    pub const fn new() -> Self {
        Self
    }

    /// Decodes at most one complete Meta reply. `Ok(None)` means the reply
    /// is not fully buffered yet; nothing is consumed until it is.
    pub fn decode(
        &self,
        expectation: &MetaReplyExpectation,
        src: &mut BytesMut,
    ) -> Result<Option<Reply>, MetaReplyDecodeError> {
        let (line_end, line_frame_len) = match find_line(src, MAX_REPLY_LINE_BYTES) {
            FindLine::Incomplete => return Ok(None),
            FindLine::OverLimit => {
                return Err(MetaReplyDecodeError::FrameTooLarge {
                    maximum: MAX_REPLY_LINE_BYTES,
                });
            }
            FindLine::Line { end, frame_len } => (end, frame_len),
        };

        let value_length = expected_value_length(expectation, &src[..line_end])?;
        let frame_len = match value_length {
            Some(length) => line_frame_len + length + 2,
            None => line_frame_len,
        };
        if src.len() < frame_len {
            return Ok(None);
        }

        let frame = src.split_to(frame_len).freeze();
        let value = match value_length {
            Some(length) => {
                if &frame[frame_len - 2..] != b"\r\n" {
                    return Err(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE));
                }
                Some(frame.slice(line_frame_len..line_frame_len + length))
            }
            None => None,
        };
        parse_line(expectation, &frame[..line_end], value).map(Some)
    }

    pub fn decode_eof(&self, src: &BytesMut) -> Result<(), MetaReplyDecodeError> {
        if src.is_empty() {
            Ok(())
        } else {
            Err(MetaReplyDecodeError::UnexpectedEof)
        }
    }
}

/// Pre-parses the `VA <length>` token so framing can wait for the complete
/// value block — but only when `expectation` allows a value reply. For
/// header-only expectations a bogus backend `VA` fails in the command parser
/// immediately, without waiting for a body that may never arrive.
fn expected_value_length(
    expectation: &MetaReplyExpectation,
    line: &[u8],
) -> Result<Option<usize>, MetaReplyDecodeError> {
    let value_allowed = match expectation {
        MetaReplyExpectation::Get(shape) => *shape != GetSuccessShape::Header,
        MetaReplyExpectation::Arithmetic { value, .. } => *value,
        _ => false,
    };
    if !value_allowed || !line.starts_with(b"VA ") {
        return Ok(None);
    }

    let length = split_tokens(line)
        .nth(1)
        .ok_or(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE))
        .and_then(|raw| parse_usize(raw).map_err(invalid_response))?;
    if length > MAX_REPLY_VALUE_BYTES {
        return Err(MetaReplyDecodeError::ValueTooLarge {
            maximum: MAX_REPLY_VALUE_BYTES,
        });
    }
    Ok(Some(length))
}

/// Parses one complete reply: the line and, when framing sized one from the
/// line's `VA` length token, its value.
fn parse_line(
    expectation: &MetaReplyExpectation,
    line: &[u8],
    value: Option<Bytes>,
) -> Result<Reply, MetaReplyDecodeError> {
    if let Some(error) = parse_error_reply(line)? {
        return Ok(Reply::Error(error));
    }
    if line.first() == Some(&b' ') {
        return Err(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE));
    }

    match expectation {
        MetaReplyExpectation::Get(shape) => command::get::parse_reply(*shape, line, value),
        MetaReplyExpectation::Store { cas, size } => command::store::parse_reply(*cas, *size, line),
        MetaReplyExpectation::Delete => command::delete::parse_reply(line),
        MetaReplyExpectation::Arithmetic {
            value: expect_value,
            cas,
            ttl,
        } => command::arithmetic::parse_reply(*expect_value, *cas, *ttl, line, value),
        MetaReplyExpectation::Debug { key } => command::debug::parse_reply(key, line),
    }
}

fn parse_error_reply(line: &[u8]) -> Result<Option<ErrorReply>, MetaReplyDecodeError> {
    if line == b"ERROR" {
        return Ok(Some(ErrorReply::Error));
    }
    for (prefix, constructor) in [
        (
            b"CLIENT_ERROR".as_slice(),
            ErrorReply::Client as fn(Option<Bytes>) -> ErrorReply,
        ),
        (
            b"SERVER_ERROR".as_slice(),
            ErrorReply::Server as fn(Option<Bytes>) -> ErrorReply,
        ),
    ] {
        if line == prefix {
            return Ok(Some(constructor(None)));
        }
        if line.starts_with(prefix) {
            if line.get(prefix.len()) != Some(&b' ') {
                return Err(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE));
            }
            let message = &line[prefix.len() + 1..];
            return Ok(Some(constructor(
                (!message.is_empty()).then(|| Bytes::copy_from_slice(message)),
            )));
        }
    }
    if line.starts_with(b"ERROR") {
        return Err(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE));
    }
    Ok(None)
}

/// Consumes a `VA` line's length token and yields the value block framing
/// sized from it. [`expected_value_length`] validated both before the
/// command parser ran; either missing here means the backend and framing
/// disagree on the line's shape.
pub fn framed_value(
    length_token: Option<&[u8]>,
    value: Option<Bytes>,
) -> Result<Bytes, MetaReplyDecodeError> {
    if length_token.is_none() {
        return Err(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE));
    }
    value.ok_or(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE))
}

/// Maps any token-level failure to the one reply-decode error: a
/// misbehaving backend gets no diagnostics, just a torn-down connection.
pub fn invalid_response<E>(_: E) -> MetaReplyDecodeError {
    MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE)
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum MetaReplyDecodeError {
    #[error("Meta reply frame exceeds the {maximum}-byte limit")]
    FrameTooLarge { maximum: usize },

    #[error("Meta reply value exceeds the {maximum}-byte limit")]
    ValueTooLarge { maximum: usize },

    #[error("{0}")]
    InvalidResponse(&'static str),

    #[error("backend connection ended with a partial Meta reply")]
    UnexpectedEof,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reply::{
        ArithmeticReply, ArithmeticResult, DebugField, DebugHit, DebugReply, DeleteReply, GetHit,
        GetReply, RecacheState, StoreReply, StoreResult,
    };

    const HEADER: MetaReplyExpectation = MetaReplyExpectation::Get(GetSuccessShape::Header);
    const VALUE: MetaReplyExpectation = MetaReplyExpectation::Get(GetSuccessShape::Value);
    const CONDITIONAL: MetaReplyExpectation =
        MetaReplyExpectation::Get(GetSuccessShape::HeaderOrValue);
    const STORE: MetaReplyExpectation = MetaReplyExpectation::Store {
        cas: false,
        size: false,
    };
    const STORE_WITH_FIELDS: MetaReplyExpectation = MetaReplyExpectation::Store {
        cas: true,
        size: true,
    };
    const DELETE: MetaReplyExpectation = MetaReplyExpectation::Delete;
    const ARITHMETIC_HEADER: MetaReplyExpectation = MetaReplyExpectation::Arithmetic {
        value: false,
        cas: false,
        ttl: false,
    };
    const ARITHMETIC_VALUE_WITH_FIELDS: MetaReplyExpectation = MetaReplyExpectation::Arithmetic {
        value: true,
        cas: true,
        ttl: true,
    };

    fn decode(expectation: &MetaReplyExpectation, input: &[u8]) -> Reply {
        let decoder = MetaReplyDecoder::new();
        let mut src = BytesMut::from(input);
        let reply = decoder.decode(expectation, &mut src).unwrap().unwrap();
        assert!(src.is_empty());
        reply
    }

    fn debug_expectation(key: &'static [u8]) -> MetaReplyExpectation {
        MetaReplyExpectation::Debug {
            key: Bytes::from_static(key),
        }
    }

    #[test]
    fn decodes_get_miss() {
        assert_eq!(decode(&HEADER, b"EN\r\n"), Reply::Get(GetReply::Miss));
    }

    #[test]
    fn decodes_debug_hit_and_miss() {
        let expectation = debug_expectation(b"key");
        assert_eq!(
            decode(
                &expectation,
                b"ME key exp=60 la=2 cas=42 fetch=yes cls=1 size=3\r\n"
            ),
            Reply::Debug(DebugReply::Hit(DebugHit {
                fields: vec![
                    DebugField {
                        name: Bytes::from_static(b"exp"),
                        value: Bytes::from_static(b"60"),
                    },
                    DebugField {
                        name: Bytes::from_static(b"la"),
                        value: Bytes::from_static(b"2"),
                    },
                    DebugField {
                        name: Bytes::from_static(b"cas"),
                        value: Bytes::from_static(b"42"),
                    },
                    DebugField {
                        name: Bytes::from_static(b"fetch"),
                        value: Bytes::from_static(b"yes"),
                    },
                    DebugField {
                        name: Bytes::from_static(b"cls"),
                        value: Bytes::from_static(b"1"),
                    },
                    DebugField {
                        name: Bytes::from_static(b"size"),
                        value: Bytes::from_static(b"3"),
                    },
                ],
            }))
        );
        assert_eq!(
            decode(&expectation, b"EN\r\n"),
            Reply::Debug(DebugReply::Miss)
        );
    }

    #[test]
    fn validates_base64_debug_key() {
        let expectation = debug_expectation(b"\0\x01");
        assert!(matches!(
            decode(&expectation, b"ME AAE= exp=60\r\n"),
            Reply::Debug(DebugReply::Hit(_))
        ));
    }

    #[test]
    fn accepts_base64_echo_of_a_text_debug_key() {
        // memcached 1.6.45 echoes the key base64-encoded whenever the item
        // was stored with the `b` flag, even for a plain-text `me` request:
        //   >> me key64
        //   << ME a2V5NjQ= exp=-1 la=0 cas=41 fetch=no cls=1 size=67
        let expectation = debug_expectation(b"key64");
        assert_eq!(
            decode(
                &expectation,
                b"ME a2V5NjQ= exp=-1 la=0 cas=41 fetch=no cls=1 size=67\r\n"
            ),
            Reply::Debug(DebugReply::Hit(DebugHit {
                fields: vec![
                    DebugField {
                        name: Bytes::from_static(b"exp"),
                        value: Bytes::from_static(b"-1"),
                    },
                    DebugField {
                        name: Bytes::from_static(b"la"),
                        value: Bytes::from_static(b"0"),
                    },
                    DebugField {
                        name: Bytes::from_static(b"cas"),
                        value: Bytes::from_static(b"41"),
                    },
                    DebugField {
                        name: Bytes::from_static(b"fetch"),
                        value: Bytes::from_static(b"no"),
                    },
                    DebugField {
                        name: Bytes::from_static(b"cls"),
                        value: Bytes::from_static(b"1"),
                    },
                    DebugField {
                        name: Bytes::from_static(b"size"),
                        value: Bytes::from_static(b"67"),
                    },
                ],
            }))
        );
    }

    #[test]
    fn rejects_debug_key_mismatch_and_malformed_fields() {
        let expectation = debug_expectation(b"key");
        for input in [
            b"ME other exp=60\r\n".as_slice(),
            // valid base64, but decodes to "other", not "key"
            b"ME b3RoZXI= exp=60\r\n".as_slice(),
            b"ME key malformed\r\n".as_slice(),
            b"ME key =value\r\n".as_slice(),
            b"EN extra\r\n".as_slice(),
        ] {
            let decoder = MetaReplyDecoder::new();
            let mut src = BytesMut::from(input);
            assert!(
                decoder.decode(&expectation, &mut src).is_err(),
                "input={input:?}"
            );
        }
    }

    #[test]
    fn bounds_debug_fields() {
        let expectation = debug_expectation(b"key");
        let mut accepted = Vec::from(&b"ME key"[..]);
        for index in 0..MAX_DEBUG_FIELDS {
            accepted.extend_from_slice(format!(" f{index}=v").as_bytes());
        }
        accepted.extend_from_slice(b"\r\n");
        assert!(matches!(
            decode(&expectation, &accepted),
            Reply::Debug(DebugReply::Hit(_))
        ));

        let mut rejected = accepted[..accepted.len() - 2].to_vec();
        rejected.extend_from_slice(b" extra=v\r\n");
        let decoder = MetaReplyDecoder::new();
        let mut src = BytesMut::from(rejected.as_slice());
        assert_eq!(
            decoder.decode(&expectation, &mut src),
            Err(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE))
        );
    }

    #[test]
    fn decodes_all_store_outcomes() {
        for (input, expected) in [
            (
                b"HD c42 s3\r\n".as_slice(),
                StoreReply::Success(StoreResult {
                    cas: Some(42),
                    size: Some(3),
                }),
            ),
            (
                b"NS c0 s3\r\n".as_slice(),
                StoreReply::NotStored(StoreResult {
                    cas: Some(0),
                    size: Some(3),
                }),
            ),
            (
                b"EX c41 s3\r\n".as_slice(),
                StoreReply::Exists(StoreResult {
                    cas: Some(41),
                    size: Some(3),
                }),
            ),
            (
                b"NF c0 s3\r\n".as_slice(),
                StoreReply::NotFound(StoreResult {
                    cas: Some(0),
                    size: Some(3),
                }),
            ),
        ] {
            assert_eq!(decode(&STORE_WITH_FIELDS, input), Reply::Store(expected));
        }
    }

    #[test]
    fn decodes_all_delete_outcomes() {
        for (input, expected) in [
            (b"HD\r\n".as_slice(), DeleteReply::Success),
            (b"NS\r\n".as_slice(), DeleteReply::NotStored),
            (b"EX\r\n".as_slice(), DeleteReply::Exists),
            (b"NF\r\n".as_slice(), DeleteReply::NotFound),
        ] {
            assert_eq!(decode(&DELETE, input), Reply::Delete(expected));
        }
    }

    #[test]
    fn decodes_arithmetic_header_success() {
        assert_eq!(
            decode(&ARITHMETIC_HEADER, b"HD\r\n"),
            Reply::Arithmetic(ArithmeticReply::Success(ArithmeticResult::default()))
        );
    }

    #[test]
    fn decodes_arithmetic_value_at_every_split_point() {
        let input = b"VA 20 t-1 c42\r\n18446744073709551615\r\n";

        for split in 0..=input.len() {
            let decoder = MetaReplyDecoder::new();
            let mut src = BytesMut::new();
            src.extend_from_slice(&input[..split]);
            if split < input.len() {
                assert_eq!(
                    decoder.decode(&ARITHMETIC_VALUE_WITH_FIELDS, &mut src),
                    Ok(None),
                    "split={split}"
                );
                src.extend_from_slice(&input[split..]);
            }

            assert_eq!(
                decoder
                    .decode(&ARITHMETIC_VALUE_WITH_FIELDS, &mut src)
                    .unwrap()
                    .unwrap(),
                Reply::Arithmetic(ArithmeticReply::Success(ArithmeticResult {
                    value: Some(u64::MAX),
                    cas: Some(42),
                    ttl: Some(-1),
                }))
            );
            assert!(src.is_empty(), "split={split}");
        }
    }

    #[test]
    fn decodes_all_arithmetic_failures() {
        for (input, expected) in [
            (
                b"NS\r\n".as_slice(),
                ArithmeticReply::NotStored(ArithmeticResult::default()),
            ),
            (
                b"EX c41\r\n".as_slice(),
                ArithmeticReply::Exists(ArithmeticResult {
                    cas: Some(41),
                    ..ArithmeticResult::default()
                }),
            ),
            (
                b"NF\r\n".as_slice(),
                ArithmeticReply::NotFound(ArithmeticResult::default()),
            ),
        ] {
            assert_eq!(
                decode(&ARITHMETIC_HEADER, input),
                Reply::Arithmetic(expected)
            );
        }
    }

    #[test]
    fn rejects_invalid_arithmetic_shape_fields_and_value() {
        for (expectation, input) in [
            (&ARITHMETIC_HEADER, b"VA 1\r\n1\r\n".as_slice()),
            (&ARITHMETIC_VALUE_WITH_FIELDS, b"HD c42 t1\r\n".as_slice()),
            (
                &ARITHMETIC_VALUE_WITH_FIELDS,
                b"VA 1 c42\r\n1\r\n".as_slice(),
            ),
            (
                &ARITHMETIC_VALUE_WITH_FIELDS,
                b"VA 1 c42 t1\r\nx\r\n".as_slice(),
            ),
        ] {
            let decoder = MetaReplyDecoder::new();
            let mut src = BytesMut::from(input);
            assert!(
                decoder.decode(expectation, &mut src).is_err(),
                "input={input:?}"
            );
        }
    }

    #[test]
    fn rejects_invalid_delete_codes_and_attributes() {
        for input in [b"EN\r\n".as_slice(), b"HD c1\r\n".as_slice()] {
            let decoder = MetaReplyDecoder::new();
            let mut src = BytesMut::from(input);
            assert!(
                decoder.decode(&DELETE, &mut src).is_err(),
                "input={input:?}"
            );
        }
    }

    #[test]
    fn decodes_store_attributes_in_any_order() {
        assert_eq!(
            decode(&STORE_WITH_FIELDS, b"HD s3 c42\r\n"),
            Reply::Store(StoreReply::Success(StoreResult {
                cas: Some(42),
                size: Some(3),
            }))
        );
    }

    #[test]
    fn rejects_store_reply_missing_expected_fields() {
        let decoder = MetaReplyDecoder::new();
        let mut src = BytesMut::from(&b"HD c42\r\n"[..]);

        assert_eq!(
            decoder.decode(&STORE_WITH_FIELDS, &mut src),
            Err(MetaReplyDecodeError::InvalidResponse(SHAPE_MISMATCH))
        );
    }

    #[test]
    fn rejects_invalid_store_codes_and_attributes() {
        for input in [
            b"EN\r\n".as_slice(),
            b"HD f1\r\n".as_slice(),
            b"HD c1 c2\r\n".as_slice(),
        ] {
            let decoder = MetaReplyDecoder::new();
            let mut src = BytesMut::from(input);
            assert!(decoder.decode(&STORE, &mut src).is_err(), "input={input:?}");
        }
    }

    #[test]
    fn decodes_header_hit_attributes_in_any_order() {
        assert_eq!(
            decode(&HEADER, b"HD X l9 h1 t-1 s3 f7 c42 W\r\n"),
            Reply::Get(GetReply::Hit(GetHit {
                value: None,
                client_flags: Some(7),
                cas: Some(42),
                size: Some(3),
                ttl: Some(-1),
                hit_before: Some(true),
                last_access_seconds: Some(9),
                recache: RecacheState::Won,
                stale: true,
            }))
        );
    }

    #[test]
    fn decodes_value_hit_at_every_split_point() {
        let input = b"VA 5 c42 s5\r\na\0b\nc\r\n";

        for split in 0..=input.len() {
            let decoder = MetaReplyDecoder::new();
            let mut src = BytesMut::new();
            src.extend_from_slice(&input[..split]);
            if split < input.len() {
                assert_eq!(decoder.decode(&VALUE, &mut src), Ok(None), "split={split}");
                src.extend_from_slice(&input[split..]);
            }

            let Reply::Get(GetReply::Hit(hit)) = decoder.decode(&VALUE, &mut src).unwrap().unwrap()
            else {
                panic!("expected value hit at split={split}");
            };
            assert_eq!(hit.value.as_deref(), Some(b"a\0b\nc".as_slice()));
            assert_eq!(hit.cas, Some(42));
            assert_eq!(hit.size, Some(5));
            assert!(src.is_empty(), "split={split}");
        }
    }

    #[test]
    fn conditional_value_accepts_header_or_value() {
        assert!(matches!(
            decode(&CONDITIONAL, b"HD c42\r\n"),
            Reply::Get(GetReply::Hit(GetHit { value: None, .. }))
        ));
        assert!(matches!(
            decode(&CONDITIONAL, b"VA 1 c43\r\nx\r\n"),
            Reply::Get(GetReply::Hit(GetHit { value: Some(_), .. }))
        ));
    }

    #[test]
    fn rejects_success_shape_mismatch() {
        for (expectation, input) in [
            (&HEADER, b"VA 1\r\nx\r\n".as_slice()),
            (&VALUE, b"HD\r\n".as_slice()),
        ] {
            let decoder = MetaReplyDecoder::new();
            let mut src = BytesMut::from(input);
            assert_eq!(
                decoder.decode(expectation, &mut src),
                Err(MetaReplyDecodeError::InvalidResponse(SHAPE_MISMATCH))
            );
        }
    }

    #[test]
    fn decodes_standard_error_replies() {
        assert_eq!(
            decode(&HEADER, b"ERROR\r\n"),
            Reply::Error(ErrorReply::Error)
        );
        assert_eq!(
            decode(&HEADER, b"CLIENT_ERROR bad command\r\n"),
            Reply::Error(ErrorReply::Client(Some(Bytes::from_static(b"bad command"))))
        );
        assert_eq!(
            decode(&HEADER, b"SERVER_ERROR\r\n"),
            Reply::Error(ErrorReply::Server(None))
        );
    }

    #[test]
    fn rejects_malformed_attributes_and_value_frames() {
        for input in [
            b"HD c1 c2\r\n".as_slice(),
            b"HD h2\r\n".as_slice(),
            b"HD unknown\r\n".as_slice(),
            b"HD W Z\r\n".as_slice(),
            b"VA nope\r\n".as_slice(),
        ] {
            let decoder = MetaReplyDecoder::new();
            let mut src = BytesMut::from(input);
            assert_eq!(
                decoder.decode(&CONDITIONAL, &mut src),
                Err(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE)),
                "input={input:?}"
            );
        }

        let decoder = MetaReplyDecoder::new();
        let mut src = BytesMut::from(&b"VA 1\r\nx\nx"[..]);
        assert_eq!(
            decoder.decode(&VALUE, &mut src),
            Err(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE))
        );
    }

    #[test]
    fn validates_value_size_attribute() {
        let decoder = MetaReplyDecoder::new();
        let mut src = BytesMut::from(&b"VA 3 s4\r\nfoo\r\n"[..]);

        assert_eq!(
            decoder.decode(&VALUE, &mut src),
            Err(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE))
        );
    }

    #[test]
    fn rejects_oversized_values_before_waiting_for_body() {
        let decoder = MetaReplyDecoder::new();
        let input = format!("VA {}\r\n", MAX_REPLY_VALUE_BYTES + 1);
        let mut src = BytesMut::from(input.as_bytes());

        assert_eq!(
            decoder.decode(&VALUE, &mut src),
            Err(MetaReplyDecodeError::ValueTooLarge {
                maximum: MAX_REPLY_VALUE_BYTES,
            })
        );
    }

    #[test]
    fn eof_rejects_partial_line_or_value() {
        let decoder = MetaReplyDecoder::new();
        let mut src = BytesMut::from(&b"HD c"[..]);
        assert_eq!(decoder.decode(&HEADER, &mut src), Ok(None));
        assert_eq!(
            decoder.decode_eof(&src),
            Err(MetaReplyDecodeError::UnexpectedEof)
        );

        let decoder = MetaReplyDecoder::new();
        let mut src = BytesMut::from(&b"VA 3\r\nfo"[..]);
        assert_eq!(decoder.decode(&VALUE, &mut src), Ok(None));
        assert_eq!(
            decoder.decode_eof(&src),
            Err(MetaReplyDecodeError::UnexpectedEof)
        );
    }
}
