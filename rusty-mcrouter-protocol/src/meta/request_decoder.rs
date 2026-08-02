use base64::{engine::general_purpose::STANDARD, Engine as _};
use bytes::{Bytes, BytesMut};
use thiserror::Error;

use crate::errors::ParseError;
use crate::key::{Key, MAX_KEY_BYTES};
use crate::reply::ErrorReply;
use crate::{
    meta::{KeyEncoding, MetaOutputToken, MetaQuietPolicy, MetaReplyPlan},
    request::{GetRequest, GetTemporalInstruction, GetTemporalInstructions, Request},
};

pub const MAX_COMMAND_LINE_BYTES: usize = 32 * 1024;
pub const MAX_VALUE_BYTES: usize = 1024 * 1024;
pub const MAX_FLAGS: usize = 64;
pub const MAX_OPAQUE_BYTES: usize = 31;

const MAX_ZERO_COPY_KEY_FRAME: usize = 1024;
const MAX_RETAINED_KEY_BUFFER: usize = 64 * 1024;
const BAD_COMMAND_LINE: &[u8] = b"bad command line format";
const INVALID_FLAG: &[u8] = b"invalid flag";
const DUPLICATE_FLAG: &[u8] = b"duplicate flag";

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DecodedMetaCommand {
    Request {
        request: Request,
        reply_plan: MetaReplyPlan,
    },
    NoOp, // mn
}

#[derive(Debug)]
pub struct MetaRequestDecoder {
    state: RequestDecodeState,
}

#[derive(Debug)]
enum RequestDecodeState {
    Command { scanned: usize },
}

impl Default for MetaRequestDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl MetaRequestDecoder {
    pub const fn new() -> Self {
        Self {
            state: RequestDecodeState::Command { scanned: 0 },
        }
    }

    /// decodes at most one complete Meta command.
    ///
    /// `Ok(None)` leaves an incomplete frame untouched. a recoverable error
    /// consumes exactly one complete command, while a fatal error requires the
    /// session to close the connection.
    pub fn decode(
        &mut self,
        src: &mut BytesMut,
    ) -> Result<Option<DecodedMetaCommand>, MetaRequestDecodeError> {
        let RequestDecodeState::Command { scanned } = &mut self.state;
        if *scanned > src.len() {
            // Incomplete input should retain its prefix, but avoid indexing a
            // stale cursor if a caller replaces the buffer unexpectedly.
            *scanned = 0;
        }

        let search_start = *scanned;
        let Some(newline) = src[search_start..]
            .iter()
            .position(|byte| *byte == b'\n')
            .map(|offset| search_start + offset)
        else {
            if src.len() >= MAX_COMMAND_LINE_BYTES {
                return Err(FatalDecodeError::FrameTooLarge {
                    maximum: MAX_COMMAND_LINE_BYTES,
                }
                .into());
            }
            *scanned = src.len();
            return Ok(None);
        };

        let frame_len = newline + 1;
        if frame_len > MAX_COMMAND_LINE_BYTES {
            return Err(FatalDecodeError::FrameTooLarge {
                maximum: MAX_COMMAND_LINE_BYTES,
            }
            .into());
        }

        let line_end = if newline > 0 && src[newline - 1] == b'\r' {
            newline - 1
        } else {
            newline
        };
        let retain_key_frame =
            frame_len <= MAX_ZERO_COPY_KEY_FRAME && src.capacity() <= MAX_RETAINED_KEY_BUFFER;
        let frame = src.split_to(frame_len).freeze();
        *scanned = 0;
        let key_frame = retain_key_frame.then_some(&frame);
        parse_command(&frame[..line_end], key_frame).map(Some)
    }

    pub fn decode_eof(&self, src: &BytesMut) -> Result<(), MetaRequestDecodeError> {
        if src.is_empty() {
            Ok(())
        } else {
            Err(FatalDecodeError::UnexpectedEof.into())
        }
    }
}

fn parse_command(
    line: &[u8],
    key_frame: Option<&Bytes>,
) -> Result<DecodedMetaCommand, MetaRequestDecodeError> {
    if line.first() == Some(&b' ') {
        return Err(MetaRequestDecodeError::Recoverable(ErrorReply::Error));
    }
    if line == b"mn" {
        return Ok(DecodedMetaCommand::NoOp);
    }
    if line.starts_with(b"mn ") {
        return Err(recoverable_client_error(BAD_COMMAND_LINE));
    }

    let mut tokens = line
        .split(|byte| *byte == b' ')
        .filter(|token| !token.is_empty());
    match tokens.next() {
        Some(b"mg") => parse_get(tokens, key_frame),
        _ => Err(MetaRequestDecodeError::Recoverable(ErrorReply::Error)),
    }
}

fn parse_get<'a>(
    mut tokens: impl Iterator<Item = &'a [u8]>,
    key_frame: Option<&Bytes>,
) -> Result<DecodedMetaCommand, MetaRequestDecodeError> {
    let raw_key = tokens
        .next()
        .ok_or_else(|| recoverable_client_error(BAD_COMMAND_LINE))?;
    let mut return_value = false;
    let mut return_client_flags = false;
    let mut return_cas = false;
    let mut return_size = false;
    let mut return_hit_state = false;
    let mut return_last_access = false;
    let mut check_cas = None;
    let mut override_cas = None;
    let mut no_lru_bump = false;
    let mut temporal = GetTemporalInstructions::default();
    let mut reply_plan = MetaReplyPlan::default();
    let mut seen = [false; 256];
    let mut flag_count = 0;
    let mut return_key = false;

    for token in tokens {
        flag_count += 1;
        if flag_count > MAX_FLAGS {
            return Err(recoverable_client_error(BAD_COMMAND_LINE));
        }

        let (&flag, argument) = token
            .split_first()
            .ok_or_else(|| recoverable_client_error(INVALID_FLAG))?;
        if !flag.is_ascii_alphabetic() {
            return Err(recoverable_client_error(INVALID_FLAG));
        }
        if seen[flag as usize] {
            return Err(recoverable_client_error(DUPLICATE_FLAG));
        }
        seen[flag as usize] = true;

        match flag {
            b'b' => {
                require_no_argument(argument)?;
                reply_plan.key_encoding = KeyEncoding::Base64;
            }
            b'c' => {
                require_no_argument(argument)?;
                return_cas = true;
                push_output(&mut reply_plan, MetaOutputToken::Cas)?;
            }
            b'C' => check_cas = Some(parse_u64(argument)?),
            b'f' => {
                require_no_argument(argument)?;
                return_client_flags = true;
                push_output(&mut reply_plan, MetaOutputToken::ClientFlags)?;
            }
            b'h' => {
                require_no_argument(argument)?;
                return_hit_state = true;
                push_output(&mut reply_plan, MetaOutputToken::HitState)?;
            }
            b'k' => {
                require_no_argument(argument)?;
                return_key = true;
                push_output(&mut reply_plan, MetaOutputToken::Key)?;
            }
            b'l' => {
                require_no_argument(argument)?;
                return_last_access = true;
                push_output(&mut reply_plan, MetaOutputToken::LastAccess)?;
            }
            b'O' => {
                if argument.is_empty() || argument.len() > MAX_OPAQUE_BYTES {
                    return Err(recoverable_client_error(BAD_COMMAND_LINE));
                }
                reply_plan.opaque = Some(Bytes::copy_from_slice(argument));
                push_output(&mut reply_plan, MetaOutputToken::Opaque)?;
            }
            b'q' => {
                require_no_argument(argument)?;
                reply_plan.quiet = MetaQuietPolicy::SuppressMiss;
            }
            b's' => {
                require_no_argument(argument)?;
                return_size = true;
                push_output(&mut reply_plan, MetaOutputToken::Size)?;
            }
            b't' => {
                require_no_argument(argument)?;
                push_temporal(&mut temporal, GetTemporalInstruction::ReturnTtl)?;
                push_output(&mut reply_plan, MetaOutputToken::Ttl)?;
            }
            b'u' => {
                require_no_argument(argument)?;
                no_lru_bump = true;
            }
            b'v' => {
                require_no_argument(argument)?;
                return_value = true;
            }
            b'E' => override_cas = Some(parse_u64(argument)?),
            b'N' => push_temporal(
                &mut temporal,
                GetTemporalInstruction::Vivify(parse_i32(argument)?),
            )?,
            b'R' => push_temporal(
                &mut temporal,
                GetTemporalInstruction::WinForRecache(parse_i32(argument)?),
            )?,
            b'T' => push_temporal(
                &mut temporal,
                GetTemporalInstruction::UpdateTtl(parse_i32(argument)?),
            )?,
            b'P' | b'L' => {
                if argument.is_empty() {
                    return Err(recoverable_client_error(BAD_COMMAND_LINE));
                }
            }
            _ => return Err(recoverable_client_error(INVALID_FLAG)),
        }
    }

    let key = parse_key(raw_key, reply_plan.key_encoding, key_frame)?;
    if return_key {
        reply_plan.external_key = Some(Bytes::copy_from_slice(key.as_bytes()));
    }

    Ok(DecodedMetaCommand::Request {
        request: Request::Get(GetRequest {
            key,
            return_value,
            return_client_flags,
            return_cas,
            return_size,
            return_hit_state,
            return_last_access,
            check_cas,
            override_cas,
            no_lru_bump,
            temporal,
        }),
        reply_plan,
    })
}

fn parse_key(
    raw: &[u8],
    encoding: KeyEncoding,
    frame: Option<&Bytes>,
) -> Result<Key, MetaRequestDecodeError> {
    if raw.len() > MAX_KEY_BYTES {
        return Err(recoverable_client_error(BAD_COMMAND_LINE));
    }

    let bytes = match encoding {
        KeyEncoding::Text => {
            if raw.is_empty() || raw.iter().any(|byte| *byte <= b' ' || *byte == 0x7f) {
                return Err(recoverable_client_error(BAD_COMMAND_LINE));
            }
            match frame {
                Some(frame) => {
                    let start = (raw.as_ptr() as usize)
                        .checked_sub(frame.as_ptr() as usize)
                        .ok_or_else(|| recoverable_client_error(BAD_COMMAND_LINE))?;
                    let end = start
                        .checked_add(raw.len())
                        .filter(|end| *end <= frame.len())
                        .ok_or_else(|| recoverable_client_error(BAD_COMMAND_LINE))?;
                    frame.slice(start..end)
                }
                None => Bytes::copy_from_slice(raw),
            }
        }
        KeyEncoding::Base64 => Bytes::from(
            STANDARD
                .decode(raw)
                .map_err(|_| recoverable_client_error(BAD_COMMAND_LINE))?,
        ),
    };

    Key::new(bytes).map_err(|_| recoverable_client_error(BAD_COMMAND_LINE))
}

fn require_no_argument(argument: &[u8]) -> Result<(), MetaRequestDecodeError> {
    if argument.is_empty() {
        Ok(())
    } else {
        Err(recoverable_client_error(BAD_COMMAND_LINE))
    }
}

fn parse_u64(raw: &[u8]) -> Result<u64, MetaRequestDecodeError> {
    parse_number(raw)
}

fn parse_i32(raw: &[u8]) -> Result<i32, MetaRequestDecodeError> {
    parse_number(raw)
}

fn parse_number<T: std::str::FromStr>(raw: &[u8]) -> Result<T, MetaRequestDecodeError> {
    if raw.is_empty() {
        return Err(recoverable_client_error(BAD_COMMAND_LINE));
    }
    std::str::from_utf8(raw)
        .ok()
        .and_then(|raw| raw.parse().ok())
        .ok_or_else(|| recoverable_client_error(BAD_COMMAND_LINE))
}

fn push_temporal(
    temporal: &mut GetTemporalInstructions,
    instruction: GetTemporalInstruction,
) -> Result<(), MetaRequestDecodeError> {
    temporal.push(instruction).map_err(parse_capacity_error)
}

fn push_output(
    reply_plan: &mut MetaReplyPlan,
    token: MetaOutputToken,
) -> Result<(), MetaRequestDecodeError> {
    reply_plan
        .output_order
        .push(token)
        .map_err(parse_capacity_error)
}

fn parse_capacity_error(_: ParseError) -> MetaRequestDecodeError {
    recoverable_client_error(BAD_COMMAND_LINE)
}

fn recoverable_client_error(message: &'static [u8]) -> MetaRequestDecodeError {
    MetaRequestDecodeError::Recoverable(ErrorReply::Client(Some(Bytes::from_static(message))))
}

/// an error produced while incrementally decoding a frontend Meta command
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum MetaRequestDecodeError {
    /// one complete malformed command was consumed. the session should encode
    /// this reply and may continue decoding the connection.
    #[error("recoverable Meta request error")]
    Recoverable(ErrorReply),

    /// frame alignment is not trustworthy. the session must close the
    /// connection rather than attempt to decode another command.
    #[error(transparent)]
    Fatal(#[from] FatalDecodeError),
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum FatalDecodeError {
    #[error("Meta frame exceeds the {maximum}-byte limit")]
    FrameTooLarge { maximum: usize },

    #[error("connection ended with a partial Meta frame")]
    UnexpectedEof,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(input: &[u8]) -> DecodedMetaCommand {
        let mut decoder = MetaRequestDecoder::new();
        let mut src = BytesMut::from(input);
        let command = decoder.decode(&mut src).unwrap().unwrap();
        assert!(src.is_empty());
        command
    }

    fn decode_error(input: &[u8]) -> MetaRequestDecodeError {
        let mut decoder = MetaRequestDecoder::new();
        let mut src = BytesMut::from(input);
        let error = decoder.decode(&mut src).unwrap_err();
        assert!(src.is_empty());
        error
    }

    #[test]
    fn decodes_basic_get() {
        let DecodedMetaCommand::Request {
            request: Request::Get(request),
            reply_plan,
        } = decode(b"mg user:1\r\n")
        else {
            panic!("expected get request");
        };

        assert_eq!(request.key.as_bytes(), b"user:1");
        assert!(!request.return_value);
        assert!(!request.return_cas);
        assert_eq!(request.temporal.iter().count(), 0);
        assert_eq!(reply_plan, MetaReplyPlan::default());
    }

    #[test]
    fn separates_get_semantics_from_frontend_reply_plan() {
        let DecodedMetaCommand::Request {
            request: Request::Get(request),
            reply_plan,
        } = decode(b"mg key Otag s N30 T40 t R50 c f h k l C99 E100 u v q Pproxy Lpath/\r\n")
        else {
            panic!("expected get request");
        };

        assert!(request.return_value);
        assert!(request.return_client_flags);
        assert!(request.return_cas);
        assert!(request.return_size);
        assert!(request.return_hit_state);
        assert!(request.return_last_access);
        assert_eq!(request.check_cas, Some(99));
        assert_eq!(request.override_cas, Some(100));
        assert!(request.no_lru_bump);
        assert_eq!(
            request.temporal.iter().cloned().collect::<Vec<_>>(),
            vec![
                GetTemporalInstruction::Vivify(30),
                GetTemporalInstruction::UpdateTtl(40),
                GetTemporalInstruction::ReturnTtl,
                GetTemporalInstruction::WinForRecache(50),
            ]
        );

        assert_eq!(reply_plan.quiet, MetaQuietPolicy::SuppressMiss);
        assert_eq!(reply_plan.opaque.as_deref(), Some(b"tag".as_slice()));
        assert_eq!(reply_plan.external_key.as_deref(), Some(b"key".as_slice()));
        assert_eq!(reply_plan.key_encoding, KeyEncoding::Text);
        assert_eq!(
            reply_plan.output_order.iter().copied().collect::<Vec<_>>(),
            vec![
                MetaOutputToken::Opaque,
                MetaOutputToken::Size,
                MetaOutputToken::Ttl,
                MetaOutputToken::Cas,
                MetaOutputToken::ClientFlags,
                MetaOutputToken::HitState,
                MetaOutputToken::Key,
                MetaOutputToken::LastAccess,
            ]
        );
    }

    #[test]
    fn normalizes_base64_get_key() {
        let DecodedMetaCommand::Request {
            request: Request::Get(request),
            reply_plan,
        } = decode(b"mg a2V5 b k\r\n")
        else {
            panic!("expected get request");
        };

        assert_eq!(request.key.as_bytes(), b"key");
        assert_eq!(reply_plan.key_encoding, KeyEncoding::Base64);
        assert_eq!(reply_plan.external_key.as_deref(), Some(b"key".as_slice()));
    }

    #[test]
    fn decodes_get_at_every_split_point() {
        let input = b"mg /region/cluster/key Otag s T40 t c v\r\n";

        for split in 0..=input.len() {
            let mut decoder = MetaRequestDecoder::new();
            let mut src = BytesMut::new();
            src.extend_from_slice(&input[..split]);

            if split < input.len() {
                assert_eq!(decoder.decode(&mut src), Ok(None), "split={split}");
                assert_eq!(src, input[..split], "split={split}");
                src.extend_from_slice(&input[split..]);
            }

            let DecodedMetaCommand::Request {
                request: Request::Get(request),
                reply_plan,
            } = decoder.decode(&mut src).unwrap().unwrap()
            else {
                panic!("expected get request at split={split}");
            };
            assert_eq!(request.key.as_bytes(), b"/region/cluster/key");
            assert!(request.return_value);
            assert_eq!(reply_plan.opaque.as_deref(), Some(b"tag".as_slice()));
            assert!(src.is_empty(), "split={split}");
        }
    }

    #[test]
    fn decodes_pipelined_get_and_noop_in_order() {
        let mut decoder = MetaRequestDecoder::new();
        let mut src = BytesMut::from(&b"mg first v\r\nmn\r\nmg second\n"[..]);

        let DecodedMetaCommand::Request {
            request: Request::Get(first),
            ..
        } = decoder.decode(&mut src).unwrap().unwrap()
        else {
            panic!("expected first get");
        };
        assert_eq!(first.key.as_bytes(), b"first");
        assert!(first.return_value);

        assert_eq!(decoder.decode(&mut src), Ok(Some(DecodedMetaCommand::NoOp)));

        let DecodedMetaCommand::Request {
            request: Request::Get(second),
            ..
        } = decoder.decode(&mut src).unwrap().unwrap()
        else {
            panic!("expected second get");
        };
        assert_eq!(second.key.as_bytes(), b"second");
        assert!(src.is_empty());
    }

    #[test]
    fn preserves_reverse_temporal_and_output_order() {
        let DecodedMetaCommand::Request {
            request: Request::Get(request),
            reply_plan,
        } = decode(b"mg key k l h f c t R50 T40 N30 s Otag\r\n")
        else {
            panic!("expected get request");
        };

        assert_eq!(
            request.temporal.iter().cloned().collect::<Vec<_>>(),
            vec![
                GetTemporalInstruction::ReturnTtl,
                GetTemporalInstruction::WinForRecache(50),
                GetTemporalInstruction::UpdateTtl(40),
                GetTemporalInstruction::Vivify(30),
            ]
        );
        assert_eq!(
            reply_plan.output_order.iter().copied().collect::<Vec<_>>(),
            vec![
                MetaOutputToken::Key,
                MetaOutputToken::LastAccess,
                MetaOutputToken::HitState,
                MetaOutputToken::ClientFlags,
                MetaOutputToken::Cas,
                MetaOutputToken::Ttl,
                MetaOutputToken::Size,
                MetaOutputToken::Opaque,
            ]
        );
    }

    #[test]
    fn rejects_duplicate_and_unknown_get_flags() {
        assert_eq!(
            decode_error(b"mg key v v\r\n"),
            recoverable_client_error(DUPLICATE_FLAG)
        );
        assert_eq!(
            decode_error(b"mg key z\r\n"),
            recoverable_client_error(INVALID_FLAG)
        );
        assert_eq!(
            decode_error(b"mg key 1\r\n"),
            recoverable_client_error(INVALID_FLAG)
        );
    }

    #[test]
    fn rejects_malformed_get_flag_arguments() {
        for input in [
            b"mg key v1\r\n".as_slice(),
            b"mg key C\r\n".as_slice(),
            b"mg key C18446744073709551616\r\n".as_slice(),
            b"mg key E-1\r\n".as_slice(),
            b"mg key T2147483648\r\n".as_slice(),
            b"mg key N-2147483649\r\n".as_slice(),
            b"mg key Rnope\r\n".as_slice(),
            b"mg key P\r\n".as_slice(),
            b"mg key L\r\n".as_slice(),
            b"mg key bvalue\r\n".as_slice(),
        ] {
            assert_eq!(
                decode_error(input),
                recoverable_client_error(BAD_COMMAND_LINE),
                "input={input:?}"
            );
        }
    }

    #[test]
    fn rejects_invalid_get_keys() {
        assert_eq!(
            decode_error(b"mg\r\n"),
            recoverable_client_error(BAD_COMMAND_LINE)
        );
        assert_eq!(
            decode_error(b"mg \x01bad\r\n"),
            recoverable_client_error(BAD_COMMAND_LINE)
        );
        assert_eq!(
            decode_error(b"mg not-base64! b\r\n"),
            recoverable_client_error(BAD_COMMAND_LINE)
        );

        let mut oversized = Vec::from(&b"mg "[..]);
        oversized.extend(std::iter::repeat(b'k').take(MAX_KEY_BYTES + 1));
        oversized.extend_from_slice(b"\r\n");
        assert_eq!(
            decode_error(&oversized),
            recoverable_client_error(BAD_COMMAND_LINE)
        );
    }

    #[test]
    fn enforces_opaque_length_limit() {
        let mut accepted = Vec::from(&b"mg key O"[..]);
        accepted.extend(std::iter::repeat(b'x').take(MAX_OPAQUE_BYTES));
        accepted.extend_from_slice(b"\r\n");
        let DecodedMetaCommand::Request { reply_plan, .. } = decode(&accepted) else {
            panic!("expected get request");
        };
        assert_eq!(reply_plan.opaque.unwrap().len(), MAX_OPAQUE_BYTES);

        let mut rejected = Vec::from(&b"mg key O"[..]);
        rejected.extend(std::iter::repeat(b'x').take(MAX_OPAQUE_BYTES + 1));
        rejected.extend_from_slice(b"\r\n");
        assert_eq!(
            decode_error(&rejected),
            recoverable_client_error(BAD_COMMAND_LINE)
        );
    }

    #[test]
    fn accepts_a_command_at_the_exact_line_limit() {
        let prefix = b"mg key P";
        let hint_len = MAX_COMMAND_LINE_BYTES - prefix.len() - 1;
        let mut input = Vec::with_capacity(MAX_COMMAND_LINE_BYTES);
        input.extend_from_slice(prefix);
        input.extend(std::iter::repeat(b'x').take(hint_len));
        input.push(b'\n');
        assert_eq!(input.len(), MAX_COMMAND_LINE_BYTES);

        let DecodedMetaCommand::Request {
            request: Request::Get(request),
            ..
        } = decode(&input)
        else {
            panic!("expected get request");
        };
        assert_eq!(request.key.as_bytes(), b"key");
    }

    #[test]
    fn incomplete_line_is_left_untouched() {
        let mut decoder = MetaRequestDecoder::new();
        let mut src = BytesMut::from(&b"mn"[..]);

        assert_eq!(decoder.decode(&mut src), Ok(None));
        assert_eq!(src, b"mn".as_slice());
    }

    #[test]
    fn resumes_line_scanning_after_fragmented_reads() {
        let mut decoder = MetaRequestDecoder::new();
        let mut src = BytesMut::from(&b"mg user"[..]);

        assert_eq!(decoder.decode(&mut src), Ok(None));
        assert!(matches!(
            &decoder.state,
            RequestDecodeState::Command { scanned } if *scanned == src.len()
        ));

        src.extend_from_slice(b":1");
        assert_eq!(decoder.decode(&mut src), Ok(None));
        assert!(matches!(
            &decoder.state,
            RequestDecodeState::Command { scanned } if *scanned == src.len()
        ));

        src.extend_from_slice(b"\r\n");
        let DecodedMetaCommand::Request {
            request: Request::Get(request),
            ..
        } = decoder.decode(&mut src).unwrap().unwrap()
        else {
            panic!("expected get request");
        };
        assert_eq!(request.key.as_bytes(), b"user:1");
        assert!(matches!(
            decoder.state,
            RequestDecodeState::Command { scanned: 0 }
        ));
    }

    #[test]
    fn decodes_noop_with_lf_or_crlf() {
        for input in [b"mn\n".as_slice(), b"mn\r\n".as_slice()] {
            let mut decoder = MetaRequestDecoder::new();
            let mut src = BytesMut::from(input);

            assert_eq!(decoder.decode(&mut src), Ok(Some(DecodedMetaCommand::NoOp)));
            assert!(src.is_empty());
        }
    }

    #[test]
    fn consumes_one_pipelined_command_at_a_time() {
        let mut decoder = MetaRequestDecoder::new();
        let mut src = BytesMut::from(&b"mn\r\nmn\n"[..]);

        assert_eq!(decoder.decode(&mut src), Ok(Some(DecodedMetaCommand::NoOp)));
        assert_eq!(src, b"mn\n".as_slice());
        assert_eq!(decoder.decode(&mut src), Ok(Some(DecodedMetaCommand::NoOp)));
        assert!(src.is_empty());
    }

    #[test]
    fn malformed_noop_is_recoverable_and_consumed() {
        let mut decoder = MetaRequestDecoder::new();
        let mut src = BytesMut::from(&b"mn unexpected\r\nmn\r\n"[..]);

        assert_eq!(
            decoder.decode(&mut src),
            Err(recoverable_client_error(b"bad command line format"))
        );
        assert_eq!(src, b"mn\r\n".as_slice());
    }

    #[test]
    fn unknown_command_is_recoverable_and_consumed() {
        let mut decoder = MetaRequestDecoder::new();
        let mut src = BytesMut::from(&b"get key\r\nmn\r\n"[..]);

        assert_eq!(
            decoder.decode(&mut src),
            Err(MetaRequestDecodeError::Recoverable(ErrorReply::Error))
        );
        assert_eq!(src, b"mn\r\n".as_slice());
    }

    #[test]
    fn oversized_partial_line_is_fatal_and_untouched() {
        let mut decoder = MetaRequestDecoder::new();
        let mut src = BytesMut::from(vec![b'x'; MAX_COMMAND_LINE_BYTES + 1].as_slice());
        let original = src.clone();

        assert_eq!(
            decoder.decode(&mut src),
            Err(MetaRequestDecodeError::Fatal(
                FatalDecodeError::FrameTooLarge {
                    maximum: MAX_COMMAND_LINE_BYTES,
                }
            ))
        );
        assert_eq!(src, original);
    }

    #[test]
    fn full_unterminated_line_is_fatal_and_untouched() {
        let mut decoder = MetaRequestDecoder::new();
        let mut src = BytesMut::from(vec![b'x'; MAX_COMMAND_LINE_BYTES].as_slice());
        let original = src.clone();

        assert_eq!(
            decoder.decode(&mut src),
            Err(MetaRequestDecodeError::Fatal(
                FatalDecodeError::FrameTooLarge {
                    maximum: MAX_COMMAND_LINE_BYTES,
                }
            ))
        );
        assert_eq!(src, original);
    }

    #[test]
    fn oversized_complete_line_is_fatal_and_untouched() {
        let mut input = vec![b'x'; MAX_COMMAND_LINE_BYTES];
        input.push(b'\n');
        let mut decoder = MetaRequestDecoder::new();
        let mut src = BytesMut::from(input.as_slice());
        let original = src.clone();

        assert_eq!(
            decoder.decode(&mut src),
            Err(MetaRequestDecodeError::Fatal(
                FatalDecodeError::FrameTooLarge {
                    maximum: MAX_COMMAND_LINE_BYTES,
                }
            ))
        );
        assert_eq!(src, original);
    }

    #[test]
    fn eof_requires_an_empty_buffer() {
        let decoder = MetaRequestDecoder::new();

        assert_eq!(decoder.decode_eof(&BytesMut::new()), Ok(()));
        assert_eq!(
            decoder.decode_eof(&BytesMut::from(&b"mn"[..])),
            Err(MetaRequestDecodeError::Fatal(
                FatalDecodeError::UnexpectedEof
            ))
        );
    }
}
