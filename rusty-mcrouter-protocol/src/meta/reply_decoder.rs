use bytes::{Bytes, BytesMut};
use memchr::memchr;
use thiserror::Error;

use crate::reply::{ErrorReply, GetHit, GetReply, RecacheState, Reply};

use super::{numbers, seen_flags::SeenFlags, GetSuccessShape, MetaReplyExpectation};

pub const MAX_REPLY_LINE_BYTES: usize = 32 * 1024;
pub const MAX_REPLY_VALUE_BYTES: usize = 1024 * 1024;

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

#[derive(Debug)]
enum PendingValue {
    Get(GetHit),
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

    pub fn decode(
        &mut self,
        expectation: &MetaReplyExpectation,
        src: &mut BytesMut,
    ) -> Result<Option<Reply>, MetaReplyDecodeError> {
        loop {
            match &mut self.state {
                ReplyDecodeState::Line { scanned } => {
                    if *scanned > src.len() {
                        *scanned = 0;
                    }
                    let search_start = *scanned;
                    let Some(newline) =
                        memchr(b'\n', &src[search_start..]).map(|offset| search_start + offset)
                    else {
                        if src.len() >= MAX_REPLY_LINE_BYTES {
                            return Err(MetaReplyDecodeError::FrameTooLarge {
                                maximum: MAX_REPLY_LINE_BYTES,
                            });
                        }
                        *scanned = src.len();
                        return Ok(None);
                    };

                    let frame_len = newline + 1;
                    if frame_len > MAX_REPLY_LINE_BYTES {
                        return Err(MetaReplyDecodeError::FrameTooLarge {
                            maximum: MAX_REPLY_LINE_BYTES,
                        });
                    }
                    let line_end = if newline > 0 && src[newline - 1] == b'\r' {
                        newline - 1
                    } else {
                        newline
                    };
                    let frame = src.split_to(frame_len).freeze();
                    *scanned = 0;

                    match parse_line(expectation, &frame[..line_end])? {
                        ParsedLine::Reply(reply) => return Ok(Some(reply)),
                        ParsedLine::Value { length, pending } => {
                            self.state = ReplyDecodeState::Value { length, pending };
                        }
                    }
                }
                ReplyDecodeState::Value { length, .. } => {
                    let frame_len =
                        length
                            .checked_add(2)
                            .ok_or(MetaReplyDecodeError::ValueTooLarge {
                                maximum: MAX_REPLY_VALUE_BYTES,
                            })?;
                    if src.len() < frame_len {
                        return Ok(None);
                    }
                    if &src[*length..frame_len] != b"\r\n" {
                        return Err(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE));
                    }

                    let ReplyDecodeState::Value { length, pending } =
                        std::mem::replace(&mut self.state, ReplyDecodeState::Line { scanned: 0 })
                    else {
                        unreachable!();
                    };
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
    }
}

fn parse_get_line(shape: GetSuccessShape, line: &[u8]) -> Result<ParsedLine, MetaReplyDecodeError> {
    let mut tokens = line
        .split(|byte| *byte == b' ')
        .filter(|token| !token.is_empty());
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
    let mut seen = SeenFlags::default();

    for token in tokens {
        let (&flag, argument) = token
            .split_first()
            .ok_or(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE))?;
        if !seen.insert(flag) {
            return Err(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE));
        }

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
    fn decode(expectation: &MetaReplyExpectation, input: &[u8]) -> Reply {
        let mut decoder = MetaReplyDecoder::new();
        let mut src = BytesMut::from(input);
        let reply = decoder.decode(expectation, &mut src).unwrap().unwrap();
        assert!(src.is_empty());
        reply
    }

    #[test]
    fn decodes_get_miss() {
        assert_eq!(decode(&HEADER, b"EN\r\n"), Reply::Get(GetReply::Miss));
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
