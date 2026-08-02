use base64::{engine::general_purpose::STANDARD, Engine as _};
use bytes::{Bytes, BytesMut};
use thiserror::Error;

use crate::reply::{
    ArithmeticReply, ArithmeticResult, DebugField, DebugHit, DebugReply, DeleteReply, ErrorReply,
    GetHit, GetReply, RecacheState, Reply, StoreReply, StoreResult,
};

use super::line_scanner::{scan_line, LineScan};
use super::tokens::{flags, split_tokens, FlagBudget};
use super::{numbers, GetSuccessShape, MetaReplyExpectation};

pub const MAX_REPLY_LINE_BYTES: usize = 32 * 1024;
pub const MAX_REPLY_VALUE_BYTES: usize = 1024 * 1024;
pub const MAX_DEBUG_FIELDS: usize = 64;

const INVALID_RESPONSE: &str = "invalid Meta backend response";
const SHAPE_MISMATCH: &str = "Meta backend response does not match request";

#[derive(Debug)]
pub struct MetaReplyDecoder {
    state: ReplyDecodeState,
}

#[derive(Debug)]
enum ReplyDecodeState {
    Line {
        scanned: usize,
    },
    Value {
        length: usize,
        pending: PendingValue,
    },
}

impl Default for ReplyDecodeState {
    fn default() -> Self {
        Self::Line { scanned: 0 }
    }
}

#[derive(Debug)]
enum PendingValue {
    Get(GetHit),
    Arithmetic(ArithmeticResult),
}

impl Default for MetaReplyDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl MetaReplyDecoder {
    pub const fn new() -> Self {
        Self {
            state: ReplyDecodeState::Line { scanned: 0 },
        }
    }

    /// Each pass takes the state out by value and puts back whatever is
    /// still incomplete, so no arm ever re-proves which variant it holds.
    pub fn decode(
        &mut self,
        expectation: &MetaReplyExpectation,
        src: &mut BytesMut,
    ) -> Result<Option<Reply>, MetaReplyDecodeError> {
        loop {
            match std::mem::take(&mut self.state) {
                ReplyDecodeState::Line { scanned } => {
                    let frame = match scan_line(scanned, src, MAX_REPLY_LINE_BYTES) {
                        LineScan::Incomplete { scanned } => {
                            self.state = ReplyDecodeState::Line { scanned };
                            return Ok(None);
                        }
                        LineScan::OverLimit => {
                            return Err(MetaReplyDecodeError::FrameTooLarge {
                                maximum: MAX_REPLY_LINE_BYTES,
                            });
                        }
                        LineScan::Frame(frame) => frame,
                    };

                    match parse_line(expectation, &frame.bytes[..frame.line_end])? {
                        ParsedLine::Reply(reply) => return Ok(Some(reply)),
                        ParsedLine::Value { length, pending } => {
                            self.state = ReplyDecodeState::Value { length, pending };
                        }
                    }
                }
                ReplyDecodeState::Value { length, pending } => {
                    let frame_len =
                        length
                            .checked_add(2)
                            .ok_or(MetaReplyDecodeError::ValueTooLarge {
                                maximum: MAX_REPLY_VALUE_BYTES,
                            })?;
                    if src.len() < frame_len {
                        self.state = ReplyDecodeState::Value { length, pending };
                        return Ok(None);
                    }
                    if &src[length..frame_len] != b"\r\n" {
                        return Err(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE));
                    }

                    let frame = src.split_to(frame_len).freeze();
                    return match pending {
                        PendingValue::Get(mut hit) => {
                            if hit.size.is_some_and(|size| size != length as u64) {
                                return Err(MetaReplyDecodeError::InvalidResponse(
                                    INVALID_RESPONSE,
                                ));
                            }
                            hit.value = Some(frame.slice(..length));
                            Ok(Some(Reply::Get(GetReply::Hit(hit))))
                        }
                        PendingValue::Arithmetic(mut result) => {
                            result.value = Some(parse_u64(&frame[..length])?);
                            Ok(Some(Reply::Arithmetic(ArithmeticReply::Success(result))))
                        }
                    };
                }
            }
        }
    }

    pub fn decode_eof(&self, src: &BytesMut) -> Result<(), MetaReplyDecodeError> {
        match self.state {
            ReplyDecodeState::Line { .. } if src.is_empty() => Ok(()),
            _ => Err(MetaReplyDecodeError::UnexpectedEof),
        }
    }
}

enum ParsedLine {
    Reply(Reply),
    Value {
        length: usize,
        pending: PendingValue,
    },
}

fn parse_line(
    expectation: &MetaReplyExpectation,
    line: &[u8],
) -> Result<ParsedLine, MetaReplyDecodeError> {
    if let Some(error) = parse_error_reply(line)? {
        return Ok(ParsedLine::Reply(Reply::Error(error)));
    }
    if line.first() == Some(&b' ') {
        return Err(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE));
    }

    match expectation {
        MetaReplyExpectation::Get(shape) => parse_get_line(*shape, line),
        MetaReplyExpectation::Store { cas, size } => parse_store_line(*cas, *size, line),
        MetaReplyExpectation::Delete => parse_delete_line(line),
        MetaReplyExpectation::Arithmetic { value, cas, ttl } => {
            parse_arithmetic_line(*value, *cas, *ttl, line)
        }
        MetaReplyExpectation::Debug { key } => parse_debug_line(key, line),
    }
}

fn parse_debug_line(expected_key: &Bytes, line: &[u8]) -> Result<ParsedLine, MetaReplyDecodeError> {
    let mut tokens = split_tokens(line);
    match tokens.next() {
        Some(b"EN") => {
            if tokens.next().is_some() {
                return Err(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE));
            }
            Ok(ParsedLine::Reply(Reply::Debug(DebugReply::Miss)))
        }
        Some(b"ME") => {
            let returned_key = tokens
                .next()
                .ok_or(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE))?;
            // memcached echoes the key as stored on the item: plain, or
            // base64 when the item was created with the `b` flag. The request
            // encoding does not determine the response encoding (verified
            // against memcached 1.6.45), so accept either form.
            let key_matches = returned_key == expected_key.as_ref()
                || STANDARD
                    .decode(returned_key)
                    .is_ok_and(|decoded| decoded == expected_key.as_ref());
            if !key_matches {
                return Err(MetaReplyDecodeError::InvalidResponse(SHAPE_MISMATCH));
            }

            let mut fields = Vec::new();
            for token in tokens {
                if fields.len() == MAX_DEBUG_FIELDS {
                    return Err(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE));
                }
                let Some(separator) = token.iter().position(|byte| *byte == b'=') else {
                    return Err(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE));
                };
                if separator == 0 {
                    return Err(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE));
                }
                fields.push(DebugField {
                    name: Bytes::copy_from_slice(&token[..separator]),
                    value: Bytes::copy_from_slice(&token[separator + 1..]),
                });
            }
            Ok(ParsedLine::Reply(Reply::Debug(DebugReply::Hit(DebugHit {
                fields,
            }))))
        }
        _ => Err(MetaReplyDecodeError::InvalidResponse(SHAPE_MISMATCH)),
    }
}

fn parse_arithmetic_line(
    expect_value: bool,
    expect_cas: bool,
    expect_ttl: bool,
    line: &[u8],
) -> Result<ParsedLine, MetaReplyDecodeError> {
    let mut tokens = split_tokens(line);
    let code = tokens
        .next()
        .ok_or(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE))?;

    match code {
        b"HD" => {
            if expect_value {
                return Err(MetaReplyDecodeError::InvalidResponse(SHAPE_MISMATCH));
            }
            let result = parse_arithmetic_attributes(tokens)?;
            validate_arithmetic_success(&result, expect_cas, expect_ttl)?;
            Ok(ParsedLine::Reply(Reply::Arithmetic(
                ArithmeticReply::Success(result),
            )))
        }
        b"VA" => {
            if !expect_value {
                return Err(MetaReplyDecodeError::InvalidResponse(SHAPE_MISMATCH));
            }
            let length = tokens
                .next()
                .ok_or(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE))
                .and_then(parse_usize)?;
            if length > MAX_REPLY_VALUE_BYTES {
                return Err(MetaReplyDecodeError::ValueTooLarge {
                    maximum: MAX_REPLY_VALUE_BYTES,
                });
            }
            let result = parse_arithmetic_attributes(tokens)?;
            validate_arithmetic_success(&result, expect_cas, expect_ttl)?;
            Ok(ParsedLine::Value {
                length,
                pending: PendingValue::Arithmetic(result),
            })
        }
        b"NS" => Ok(ParsedLine::Reply(Reply::Arithmetic(
            ArithmeticReply::NotStored(parse_arithmetic_attributes(tokens)?),
        ))),
        b"EX" => Ok(ParsedLine::Reply(Reply::Arithmetic(
            ArithmeticReply::Exists(parse_arithmetic_attributes(tokens)?),
        ))),
        b"NF" => Ok(ParsedLine::Reply(Reply::Arithmetic(
            ArithmeticReply::NotFound(parse_arithmetic_attributes(tokens)?),
        ))),
        _ => Err(MetaReplyDecodeError::InvalidResponse(SHAPE_MISMATCH)),
    }
}

fn parse_arithmetic_attributes<'a>(
    tokens: impl Iterator<Item = &'a [u8]>,
) -> Result<ArithmeticResult, MetaReplyDecodeError> {
    let mut result = ArithmeticResult::default();

    for flag in flags(tokens, FlagBudget::Unlimited) {
        // a misbehaving backend gets no diagnostics, just a torn-down connection
        let (flag, argument) =
            flag.map_err(|_| MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE))?;
        match flag {
            b'c' => result.cas = Some(parse_u64(argument)?),
            b't' => result.ttl = Some(parse_i64(argument)?),
            _ => return Err(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE)),
        }
    }
    Ok(result)
}

fn validate_arithmetic_success(
    result: &ArithmeticResult,
    expect_cas: bool,
    expect_ttl: bool,
) -> Result<(), MetaReplyDecodeError> {
    if (expect_cas && result.cas.is_none()) || (expect_ttl && result.ttl.is_none()) {
        Err(MetaReplyDecodeError::InvalidResponse(SHAPE_MISMATCH))
    } else {
        Ok(())
    }
}

fn parse_delete_line(line: &[u8]) -> Result<ParsedLine, MetaReplyDecodeError> {
    let mut tokens = split_tokens(line);
    let code = tokens
        .next()
        .ok_or(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE))?;
    if tokens.next().is_some() {
        return Err(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE));
    }

    let reply = match code {
        b"HD" => DeleteReply::Success,
        b"NS" => DeleteReply::NotStored,
        b"EX" => DeleteReply::Exists,
        b"NF" => DeleteReply::NotFound,
        _ => return Err(MetaReplyDecodeError::InvalidResponse(SHAPE_MISMATCH)),
    };
    Ok(ParsedLine::Reply(Reply::Delete(reply)))
}

fn parse_get_line(shape: GetSuccessShape, line: &[u8]) -> Result<ParsedLine, MetaReplyDecodeError> {
    let mut tokens = split_tokens(line);
    match tokens.next() {
        Some(b"EN") => {
            if tokens.next().is_some() {
                return Err(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE));
            }
            Ok(ParsedLine::Reply(Reply::Get(GetReply::Miss)))
        }
        Some(b"HD") => {
            if shape == GetSuccessShape::Value {
                return Err(MetaReplyDecodeError::InvalidResponse(SHAPE_MISMATCH));
            }
            let hit = parse_get_attributes(tokens)?;
            Ok(ParsedLine::Reply(Reply::Get(GetReply::Hit(hit))))
        }
        Some(b"VA") => {
            if shape == GetSuccessShape::Header {
                return Err(MetaReplyDecodeError::InvalidResponse(SHAPE_MISMATCH));
            }
            let length = tokens
                .next()
                .ok_or(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE))
                .and_then(parse_usize)?;
            if length > MAX_REPLY_VALUE_BYTES {
                return Err(MetaReplyDecodeError::ValueTooLarge {
                    maximum: MAX_REPLY_VALUE_BYTES,
                });
            }
            let hit = parse_get_attributes(tokens)?;
            Ok(ParsedLine::Value {
                length,
                pending: PendingValue::Get(hit),
            })
        }
        _ => Err(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE)),
    }
}

fn parse_store_line(
    expect_cas: bool,
    expect_size: bool,
    line: &[u8],
) -> Result<ParsedLine, MetaReplyDecodeError> {
    let mut tokens = split_tokens(line);
    let code = tokens
        .next()
        .ok_or(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE))?;
    let result = parse_store_attributes(tokens)?;
    if (expect_cas && result.cas.is_none()) || (expect_size && result.size.is_none()) {
        return Err(MetaReplyDecodeError::InvalidResponse(SHAPE_MISMATCH));
    }

    let reply = match code {
        b"HD" => StoreReply::Success(result),
        b"NS" => StoreReply::NotStored(result),
        b"EX" => StoreReply::Exists(result),
        b"NF" => StoreReply::NotFound(result),
        _ => return Err(MetaReplyDecodeError::InvalidResponse(SHAPE_MISMATCH)),
    };
    Ok(ParsedLine::Reply(Reply::Store(reply)))
}

fn parse_store_attributes<'a>(
    tokens: impl Iterator<Item = &'a [u8]>,
) -> Result<StoreResult, MetaReplyDecodeError> {
    let mut result = StoreResult::default();

    for flag in flags(tokens, FlagBudget::Unlimited) {
        // a misbehaving backend gets no diagnostics, just a torn-down connection
        let (flag, argument) =
            flag.map_err(|_| MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE))?;
        match flag {
            b'c' => result.cas = Some(parse_u64(argument)?),
            b's' => result.size = Some(parse_u64(argument)?),
            _ => return Err(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE)),
        }
    }
    Ok(result)
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

fn parse_get_attributes<'a>(
    tokens: impl Iterator<Item = &'a [u8]>,
) -> Result<GetHit, MetaReplyDecodeError> {
    let mut hit = GetHit::default();

    for flag in flags(tokens, FlagBudget::Unlimited) {
        // a misbehaving backend gets no diagnostics, just a torn-down connection
        let (flag, argument) =
            flag.map_err(|_| MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE))?;
        match flag {
            b'c' => hit.cas = Some(parse_u64(argument)?),
            b'f' => hit.client_flags = Some(parse_u32(argument)?),
            b's' => hit.size = Some(parse_u64(argument)?),
            b't' => hit.ttl = Some(parse_i64(argument)?),
            b'h' => {
                hit.hit_before = Some(match argument {
                    b"0" => false,
                    b"1" => true,
                    _ => return Err(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE)),
                });
            }
            b'l' => hit.last_access_seconds = Some(parse_u64(argument)?),
            b'W' => {
                require_no_argument(argument)?;
                if hit.recache != RecacheState::None {
                    return Err(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE));
                }
                hit.recache = RecacheState::Won;
            }
            b'Z' => {
                require_no_argument(argument)?;
                if hit.recache != RecacheState::None {
                    return Err(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE));
                }
                hit.recache = RecacheState::AlreadyWon;
            }
            b'X' => {
                require_no_argument(argument)?;
                hit.stale = true;
            }
            _ => return Err(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE)),
        }
    }
    Ok(hit)
}

fn require_no_argument(argument: &[u8]) -> Result<(), MetaReplyDecodeError> {
    if argument.is_empty() {
        Ok(())
    } else {
        Err(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE))
    }
}

fn parse_usize(raw: &[u8]) -> Result<usize, MetaReplyDecodeError> {
    numbers::parse_usize(raw).ok_or(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE))
}

fn parse_u64(raw: &[u8]) -> Result<u64, MetaReplyDecodeError> {
    numbers::parse_u64(raw).ok_or(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE))
}

fn parse_u32(raw: &[u8]) -> Result<u32, MetaReplyDecodeError> {
    numbers::parse_u32(raw).ok_or(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE))
}

fn parse_i64(raw: &[u8]) -> Result<i64, MetaReplyDecodeError> {
    numbers::parse_i64(raw).ok_or(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE))
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
        let mut decoder = MetaReplyDecoder::new();
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
            let mut decoder = MetaReplyDecoder::new();
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
        let mut decoder = MetaReplyDecoder::new();
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
            let mut decoder = MetaReplyDecoder::new();
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
            let mut decoder = MetaReplyDecoder::new();
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
            let mut decoder = MetaReplyDecoder::new();
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
        let mut decoder = MetaReplyDecoder::new();
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
            let mut decoder = MetaReplyDecoder::new();
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
            let mut decoder = MetaReplyDecoder::new();
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
            let mut decoder = MetaReplyDecoder::new();
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
            let mut decoder = MetaReplyDecoder::new();
            let mut src = BytesMut::from(input);
            assert_eq!(
                decoder.decode(&CONDITIONAL, &mut src),
                Err(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE)),
                "input={input:?}"
            );
        }

        let mut decoder = MetaReplyDecoder::new();
        let mut src = BytesMut::from(&b"VA 1\r\nx\nx"[..]);
        assert_eq!(
            decoder.decode(&VALUE, &mut src),
            Err(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE))
        );
    }

    #[test]
    fn validates_value_size_attribute() {
        let mut decoder = MetaReplyDecoder::new();
        let mut src = BytesMut::from(&b"VA 3 s4\r\nfoo\r\n"[..]);

        assert_eq!(
            decoder.decode(&VALUE, &mut src),
            Err(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE))
        );
    }

    #[test]
    fn rejects_oversized_values_before_waiting_for_body() {
        let mut decoder = MetaReplyDecoder::new();
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
        let mut decoder = MetaReplyDecoder::new();
        let mut src = BytesMut::from(&b"HD c"[..]);
        assert_eq!(decoder.decode(&HEADER, &mut src), Ok(None));
        assert_eq!(
            decoder.decode_eof(&src),
            Err(MetaReplyDecodeError::UnexpectedEof)
        );

        let mut decoder = MetaReplyDecoder::new();
        let mut src = BytesMut::from(&b"VA 3\r\nfo"[..]);
        assert_eq!(decoder.decode(&VALUE, &mut src), Ok(None));
        assert_eq!(
            decoder.decode_eof(&src),
            Err(MetaReplyDecodeError::UnexpectedEof)
        );
    }
}
