use base64::{engine::general_purpose::STANDARD, Engine as _};
use bytes::{Buf, Bytes, BytesMut};
use thiserror::Error;

use crate::key::{Key, MAX_KEY_BYTES};
use crate::meta::{KeyEncoding, MetaOutputToken, MetaReplyPlan};
use crate::reply::ErrorReply;
use crate::request::Request;

use super::command;
use super::tokens::{find_line, split_tokens, FindLine, FlagError};

pub const MAX_COMMAND_LINE_BYTES: usize = 32 * 1024;
pub const MAX_VALUE_BYTES: usize = 1024 * 1024;
/// memcached's meta parser rejects `mg`/`ms`/`md` lines with more than 20
/// space-separated tokens as "options flags are too long"; `ma` and `me` use
/// a different upstream parser with no token budget (verified against
/// memcached 1.6.45). Upstream applies the budget before validating flags; we
/// count in-line instead, so a line that is over budget *and* malformed
/// earlier reports the earlier flag error. The accepted/rejected request sets
/// are identical.
pub const MAX_LINE_TOKENS: usize = 20;
pub const MAX_OPAQUE_BYTES: usize = 31;

pub const BAD_COMMAND_LINE: &[u8] = b"bad command line format";
pub const INVALID_FLAG: &[u8] = b"invalid flag";
const DUPLICATE_FLAG: &[u8] = b"duplicate flag";
const BAD_DATA_CHUNK: &[u8] = b"bad data chunk";
const OBJECT_TOO_LARGE: &[u8] = b"object too large for cache";
const OPTIONS_FLAGS_TOO_LONG: &[u8] = b"options flags are too long";

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DecodedMetaCommand {
    Request {
        request: Request,
        reply_plan: MetaReplyPlan,
    },
    NoOp, // mn
}

/// Nearly stateless: each call frames one complete command — the line plus,
/// for `ms`, its value block — and parses it in a single pass. The one piece
/// of state is the swallow counter: an `ms` header may declare a body too
/// large to ever buffer, and memcached semantics require consuming and
/// discarding that body before reporting the error.
#[derive(Debug, Default)]
pub struct MetaRequestDecoder {
    swallow: Option<Swallow>,
}

/// `remaining` body bytes to discard before reporting `reply`.
#[derive(Debug)]
struct Swallow {
    remaining: usize,
    reply: ErrorReply,
}

impl MetaRequestDecoder {
    pub const fn new() -> Self {
        Self { swallow: None }
    }

    /// Decodes at most one complete Meta command.
    ///
    /// `Ok(None)` means the command is not fully buffered yet; nothing is
    /// consumed until it is. A recoverable error consumes exactly one
    /// complete command, while a fatal error requires the session to close
    /// the connection.
    pub fn decode(
        &mut self,
        src: &mut BytesMut,
    ) -> Result<Option<DecodedMetaCommand>, MetaRequestDecodeError> {
        if self.swallow.is_some() {
            return self.swallow_buffered(src);
        }

        let (line_end, line_frame_len) = match find_line(src, MAX_COMMAND_LINE_BYTES) {
            FindLine::Incomplete => return Ok(None),
            FindLine::OverLimit => {
                return Err(FatalDecodeError::FrameTooLarge {
                    maximum: MAX_COMMAND_LINE_BYTES,
                }
                .into());
            }
            FindLine::Line { end, frame_len } => (end, frame_len),
        };

        let line = &src[..line_end];
        if line == b"ms" || line.starts_with(b"ms ") {
            return self.decode_store(line_end, line_frame_len, src);
        }

        let command = parse_command(line);
        src.advance(line_frame_len);
        command.map(Some)
    }

    pub fn decode_eof(&self, src: &BytesMut) -> Result<(), MetaRequestDecodeError> {
        if self.swallow.is_none() && src.is_empty() {
            Ok(())
        } else {
            Err(FatalDecodeError::UnexpectedEof.into())
        }
    }

    /// Frames `ms`: the value length is pre-parsed from the (complete)
    /// command line so the body can be framed — or swallowed when it exceeds
    /// what we are willing to buffer — before the header is fully validated.
    fn decode_store(
        &mut self,
        line_end: usize,
        line_frame_len: usize,
        src: &mut BytesMut,
    ) -> Result<Option<DecodedMetaCommand>, MetaRequestDecodeError> {
        let value_len = match command::store::parse_value_length(&src[..line_end]) {
            Ok(value_len) => value_len,
            Err(error) => {
                // A malformed datalen never swallows a body: frame alignment
                // resumes at the next line, matching memcached.
                src.advance(line_frame_len);
                return Err(error);
            }
        };
        if value_len > MAX_VALUE_BYTES {
            src.advance(line_frame_len);
            self.swallow = Some(Swallow {
                remaining: value_len + 2,
                reply: ErrorReply::Server(Some(Bytes::from_static(OBJECT_TOO_LARGE))),
            });
            return self.swallow_buffered(src);
        }

        let frame_len = line_frame_len + value_len + 2;
        if src.len() < frame_len {
            return Ok(None);
        }

        let frame = src.split_to(frame_len).freeze();
        // Header errors take precedence over a malformed body terminator,
        // matching memcached, which validates the header before the chunk.
        let command = command::store::parse_request(
            &frame[..line_end],
            frame.slice(line_frame_len..line_frame_len + value_len),
        )?;
        if &frame[frame_len - 2..] != b"\r\n" {
            return Err(recoverable_client_error(BAD_DATA_CHUNK));
        }
        Ok(Some(command))
    }

    /// Discards buffered bytes of a swallowed body, reporting the pending
    /// error once the body has been fully consumed.
    fn swallow_buffered(
        &mut self,
        src: &mut BytesMut,
    ) -> Result<Option<DecodedMetaCommand>, MetaRequestDecodeError> {
        let Some(mut swallow) = self.swallow.take() else {
            return Ok(None);
        };
        let consumed = swallow.remaining.min(src.len());
        src.advance(consumed);
        swallow.remaining -= consumed;
        if swallow.remaining != 0 {
            self.swallow = Some(swallow);
            return Ok(None);
        }
        Err(MetaRequestDecodeError::Recoverable(swallow.reply))
    }
}

fn parse_command(line: &[u8]) -> Result<DecodedMetaCommand, MetaRequestDecodeError> {
    if line.first() == Some(&b' ') {
        return Err(MetaRequestDecodeError::Recoverable(ErrorReply::Error));
    }
    if line == b"mn" {
        return Ok(DecodedMetaCommand::NoOp);
    }
    if line.starts_with(b"mn ") {
        return Err(recoverable_client_error(BAD_COMMAND_LINE));
    }

    let mut tokens = split_tokens(line);
    match tokens.next() {
        Some(b"mg") => command::get::parse_request(tokens),
        Some(b"md") => command::delete::parse_request(tokens),
        Some(b"ma") => command::arithmetic::parse_request(tokens),
        Some(b"me") => command::debug::parse_request(tokens),
        _ => Err(MetaRequestDecodeError::Recoverable(ErrorReply::Error)),
    }
}

pub fn parse_key(raw: &[u8], encoding: KeyEncoding) -> Result<Key, MetaRequestDecodeError> {
    if raw.len() > MAX_KEY_BYTES {
        return Err(recoverable_client_error(BAD_COMMAND_LINE));
    }

    let bytes = match encoding {
        KeyEncoding::Text => {
            if raw.is_empty() || raw.iter().any(|byte| *byte <= b' ' || *byte == 0x7f) {
                return Err(recoverable_client_error(BAD_COMMAND_LINE));
            }
            Bytes::copy_from_slice(raw)
        }
        KeyEncoding::Base64 => Bytes::from(
            STANDARD
                .decode(raw)
                .map_err(|_| recoverable_client_error(BAD_COMMAND_LINE))?,
        ),
    };

    Key::new(bytes).map_err(|_| recoverable_client_error(BAD_COMMAND_LINE))
}

/// `P` and `L` are proxy hints that carry no cache semantics; they are
/// validated (argument required) and dropped on every command.
pub fn require_hint_argument(argument: &[u8]) -> Result<(), MetaRequestDecodeError> {
    if argument.is_empty() {
        Err(recoverable_client_error(BAD_COMMAND_LINE))
    } else {
        Ok(())
    }
}

/// `O<token>`: retain the opaque for the local reply and record its output
/// position. Never forwarded to the backend.
pub fn parse_opaque(
    argument: &[u8],
    reply_plan: &mut MetaReplyPlan,
) -> Result<(), MetaRequestDecodeError> {
    if argument.is_empty() || argument.len() > MAX_OPAQUE_BYTES {
        return Err(recoverable_client_error(BAD_COMMAND_LINE));
    }
    reply_plan.opaque = Some(Bytes::copy_from_slice(argument));
    reply_plan
        .output_order
        .push(MetaOutputToken::Opaque)
        .map_err(bad_command_line)
}

/// Parses the key under the plan's encoding and, for `k`, retains the
/// client-visible form for the local reply.
pub fn resolve_key(
    raw_key: &[u8],
    return_key: bool,
    reply_plan: &mut MetaReplyPlan,
) -> Result<Key, MetaRequestDecodeError> {
    let key = parse_key(raw_key, reply_plan.key_encoding)?;
    if return_key {
        reply_plan.external_key = Some(key.clone_bytes());
    }
    Ok(key)
}

pub fn flag_error(error: FlagError) -> MetaRequestDecodeError {
    match error {
        FlagError::OverBudget => recoverable_client_error(OPTIONS_FLAGS_TOO_LONG),
        FlagError::InvalidToken => recoverable_client_error(INVALID_FLAG),
        FlagError::Duplicate => recoverable_client_error(DUPLICATE_FLAG),
    }
}

/// Maps any token-level failure to memcached's generic complaint; the
/// request decoder never gives numeric or shape diagnostics beyond it.
pub fn bad_command_line<E>(_: E) -> MetaRequestDecodeError {
    recoverable_client_error(BAD_COMMAND_LINE)
}

pub fn recoverable_client_error(message: &'static [u8]) -> MetaRequestDecodeError {
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
    use crate::meta::MetaQuietPolicy;
    use crate::request::{
        ArithmeticMode, ArithmeticTemporalInstruction, GetTemporalInstruction, StoreMode,
    };

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
    fn decodes_basic_store() {
        let DecodedMetaCommand::Request {
            request: Request::Store(request),
            reply_plan,
        } = decode(b"ms key 3\r\nfoo\r\n")
        else {
            panic!("expected store request");
        };

        assert_eq!(request.key.as_bytes(), b"key");
        assert_eq!(request.value, b"foo".as_slice());
        assert_eq!(request.mode, StoreMode::Set);
        assert_eq!(request.client_flags, None);
        assert_eq!(request.ttl, None);
        assert_eq!(request.compare_cas, None);
        assert_eq!(request.override_cas, None);
        assert!(!request.invalidate);
        assert_eq!(request.vivify_ttl, None);
        assert!(!request.return_cas);
        assert!(!request.return_size);
        assert_eq!(reply_plan, MetaReplyPlan::default());
    }

    #[test]
    fn separates_store_semantics_from_frontend_reply_plan() {
        let DecodedMetaCommand::Request {
            request: Request::Store(request),
            reply_plan,
        } = decode(b"ms key 3 c C42 E43 F7 I k Otag q s T60 MA N30 Pproxy Lpath/\r\nfoo\r\n")
        else {
            panic!("expected store request");
        };

        assert_eq!(request.mode, StoreMode::Append);
        assert_eq!(request.client_flags, Some(7));
        assert_eq!(request.ttl, Some(60));
        assert_eq!(request.compare_cas, Some(42));
        assert_eq!(request.override_cas, Some(43));
        assert!(request.invalidate);
        assert_eq!(request.vivify_ttl, Some(30));
        assert!(request.return_cas);
        assert!(request.return_size);

        assert_eq!(reply_plan.quiet, MetaQuietPolicy::SuppressSuccess);
        assert_eq!(reply_plan.opaque.as_deref(), Some(b"tag".as_slice()));
        assert_eq!(reply_plan.external_key.as_deref(), Some(b"key".as_slice()));
        assert_eq!(
            reply_plan.output_order.iter().copied().collect::<Vec<_>>(),
            vec![
                MetaOutputToken::Cas,
                MetaOutputToken::Key,
                MetaOutputToken::Opaque,
                MetaOutputToken::Size,
            ]
        );
    }

    #[test]
    fn decodes_all_store_modes() {
        for (wire_mode, expected) in [
            (b'S', StoreMode::Set),
            (b'E', StoreMode::Add),
            (b'R', StoreMode::Replace),
            (b'A', StoreMode::Append),
            (b'P', StoreMode::Prepend),
        ] {
            let input = [b"ms key 0 M".as_slice(), &[wire_mode], b"\r\n\r\n"].concat();
            let DecodedMetaCommand::Request {
                request: Request::Store(request),
                ..
            } = decode(&input)
            else {
                panic!("expected store request");
            };
            assert_eq!(request.mode, expected);
        }
    }

    #[test]
    fn normalizes_base64_store_key() {
        let DecodedMetaCommand::Request {
            request: Request::Store(request),
            reply_plan,
        } = decode(b"ms a2V5 1 b k\r\nx\r\n")
        else {
            panic!("expected store request");
        };

        assert_eq!(request.key.as_bytes(), b"key");
        assert_eq!(reply_plan.key_encoding, KeyEncoding::Base64);
        assert_eq!(reply_plan.external_key.as_deref(), Some(b"key".as_slice()));
    }

    #[test]
    fn decodes_basic_delete() {
        let DecodedMetaCommand::Request {
            request: Request::Delete(request),
            reply_plan,
        } = decode(b"md key\r\n")
        else {
            panic!("expected delete request");
        };

        assert_eq!(request.key.as_bytes(), b"key");
        assert_eq!(request.compare_cas, None);
        assert_eq!(request.override_cas, None);
        assert_eq!(request.client_flags, None);
        assert!(!request.invalidate);
        assert_eq!(request.ttl, None);
        assert!(!request.remove_value);
        assert_eq!(reply_plan, MetaReplyPlan::default());
    }

    #[test]
    fn separates_delete_semantics_from_frontend_reply_plan() {
        let DecodedMetaCommand::Request {
            request: Request::Delete(request),
            reply_plan,
        } = decode(b"md key C42 E43 F7 I k Otag q T60 x Pproxy Lpath/\r\n")
        else {
            panic!("expected delete request");
        };

        assert_eq!(request.compare_cas, Some(42));
        assert_eq!(request.override_cas, Some(43));
        assert_eq!(request.client_flags, Some(7));
        assert!(request.invalidate);
        assert_eq!(request.ttl, Some(60));
        assert!(request.remove_value);
        assert_eq!(reply_plan.quiet, MetaQuietPolicy::SuppressSuccess);
        assert_eq!(reply_plan.opaque.as_deref(), Some(b"tag".as_slice()));
        assert_eq!(reply_plan.external_key.as_deref(), Some(b"key".as_slice()));
        assert_eq!(
            reply_plan.output_order.iter().copied().collect::<Vec<_>>(),
            vec![MetaOutputToken::Key, MetaOutputToken::Opaque]
        );
    }

    #[test]
    fn decodes_delete_at_every_split_point() {
        let input = b"md /region/cluster/key Otag C42 I T60\r\n";

        for split in 0..=input.len() {
            let mut decoder = MetaRequestDecoder::new();
            let mut src = BytesMut::new();
            src.extend_from_slice(&input[..split]);
            if split < input.len() {
                assert_eq!(decoder.decode(&mut src), Ok(None), "split={split}");
                src.extend_from_slice(&input[split..]);
            }

            let DecodedMetaCommand::Request {
                request: Request::Delete(request),
                reply_plan,
            } = decoder.decode(&mut src).unwrap().unwrap()
            else {
                panic!("expected delete request at split={split}");
            };
            assert_eq!(request.key.as_bytes(), b"/region/cluster/key");
            assert_eq!(request.compare_cas, Some(42));
            assert!(request.invalidate);
            assert_eq!(reply_plan.opaque.as_deref(), Some(b"tag".as_slice()));
            assert!(src.is_empty(), "split={split}");
        }
    }

    #[test]
    fn normalizes_base64_delete_key() {
        let DecodedMetaCommand::Request {
            request: Request::Delete(request),
            reply_plan,
        } = decode(b"md a2V5 b k\r\n")
        else {
            panic!("expected delete request");
        };

        assert_eq!(request.key.as_bytes(), b"key");
        assert_eq!(reply_plan.key_encoding, KeyEncoding::Base64);
        assert_eq!(reply_plan.external_key.as_deref(), Some(b"key".as_slice()));
    }

    #[test]
    fn rejects_invalid_delete_flags() {
        assert_eq!(
            decode_error(b"md key I I\r\n"),
            recoverable_client_error(DUPLICATE_FLAG)
        );
        assert_eq!(
            decode_error(b"md key v\r\n"),
            recoverable_client_error(INVALID_FLAG)
        );
        assert_eq!(
            decode_error(b"md key Cnope\r\n"),
            recoverable_client_error(BAD_COMMAND_LINE)
        );
    }

    #[test]
    fn decodes_basic_arithmetic() {
        let DecodedMetaCommand::Request {
            request: Request::Arithmetic(request),
            reply_plan,
        } = decode(b"ma key\r\n")
        else {
            panic!("expected arithmetic request");
        };

        assert_eq!(request.key.as_bytes(), b"key");
        assert_eq!(request.mode, ArithmeticMode::Increment);
        assert_eq!(request.delta, 1);
        assert_eq!(request.initial_value, None);
        assert_eq!(request.compare_cas, None);
        assert_eq!(request.override_cas, None);
        assert!(!request.return_value);
        assert!(!request.return_cas);
        assert_eq!(request.temporal.iter().count(), 0);
        assert_eq!(reply_plan, MetaReplyPlan::default());
    }

    #[test]
    fn separates_arithmetic_semantics_from_frontend_reply_plan() {
        let DecodedMetaCommand::Request {
            request: Request::Arithmetic(request),
            reply_plan,
        } = decode(b"ma key Otag N30 J5 D2 T60 MD q t c v k C42 E43 Pproxy Lpath/\r\n")
        else {
            panic!("expected arithmetic request");
        };

        assert_eq!(request.mode, ArithmeticMode::Decrement);
        assert_eq!(request.delta, 2);
        assert_eq!(request.initial_value, Some(5));
        assert_eq!(request.compare_cas, Some(42));
        assert_eq!(request.override_cas, Some(43));
        assert!(request.return_value);
        assert!(request.return_cas);
        assert_eq!(
            request.temporal.iter().cloned().collect::<Vec<_>>(),
            vec![
                ArithmeticTemporalInstruction::Vivify(30),
                ArithmeticTemporalInstruction::UpdateTtl(60),
                ArithmeticTemporalInstruction::ReturnTtl,
            ]
        );

        assert_eq!(reply_plan.quiet, MetaQuietPolicy::SuppressSuccess);
        assert_eq!(reply_plan.opaque.as_deref(), Some(b"tag".as_slice()));
        assert_eq!(reply_plan.external_key.as_deref(), Some(b"key".as_slice()));
        assert_eq!(
            reply_plan.output_order.iter().copied().collect::<Vec<_>>(),
            vec![
                MetaOutputToken::Opaque,
                MetaOutputToken::Ttl,
                MetaOutputToken::Cas,
                MetaOutputToken::Key,
            ]
        );
    }

    #[test]
    fn decodes_all_arithmetic_modes() {
        for (wire_mode, expected) in [
            (b'I', ArithmeticMode::Increment),
            (b'+', ArithmeticMode::Increment),
            (b'D', ArithmeticMode::Decrement),
            (b'-', ArithmeticMode::Decrement),
        ] {
            let input = [b"ma key M".as_slice(), &[wire_mode], b"\r\n"].concat();
            let DecodedMetaCommand::Request {
                request: Request::Arithmetic(request),
                ..
            } = decode(&input)
            else {
                panic!("expected arithmetic request");
            };
            assert_eq!(request.mode, expected);
        }
    }

    #[test]
    fn decodes_arithmetic_at_every_split_point() {
        let input = b"ma /region/cluster/key N30 T60 t D2 v\r\n";

        for split in 0..=input.len() {
            let mut decoder = MetaRequestDecoder::new();
            let mut src = BytesMut::new();
            src.extend_from_slice(&input[..split]);
            if split < input.len() {
                assert_eq!(decoder.decode(&mut src), Ok(None), "split={split}");
                src.extend_from_slice(&input[split..]);
            }

            let DecodedMetaCommand::Request {
                request: Request::Arithmetic(request),
                ..
            } = decoder.decode(&mut src).unwrap().unwrap()
            else {
                panic!("expected arithmetic request at split={split}");
            };
            assert_eq!(request.key.as_bytes(), b"/region/cluster/key");
            assert_eq!(request.delta, 2);
            assert!(request.return_value);
            assert!(src.is_empty(), "split={split}");
        }
    }

    #[test]
    fn normalizes_base64_arithmetic_key() {
        let DecodedMetaCommand::Request {
            request: Request::Arithmetic(request),
            reply_plan,
        } = decode(b"ma a2V5 b k\r\n")
        else {
            panic!("expected arithmetic request");
        };

        assert_eq!(request.key.as_bytes(), b"key");
        assert_eq!(reply_plan.key_encoding, KeyEncoding::Base64);
        assert_eq!(reply_plan.external_key.as_deref(), Some(b"key".as_slice()));
    }

    #[test]
    fn rejects_invalid_arithmetic_flags() {
        assert_eq!(
            decode_error(b"ma key J5\r\n"),
            recoverable_client_error(BAD_COMMAND_LINE)
        );
        assert_eq!(
            decode_error(b"ma key MX\r\n"),
            recoverable_client_error(BAD_COMMAND_LINE)
        );
        assert_eq!(
            decode_error(b"ma key Dnope\r\n"),
            recoverable_client_error(BAD_COMMAND_LINE)
        );
        assert_eq!(
            decode_error(b"ma key v v\r\n"),
            recoverable_client_error(DUPLICATE_FLAG)
        );
        assert_eq!(
            decode_error(b"ma key s\r\n"),
            recoverable_client_error(INVALID_FLAG)
        );
    }

    #[test]
    fn decodes_basic_debug() {
        let DecodedMetaCommand::Request {
            request: Request::Debug(request),
            reply_plan,
        } = decode(b"me key\r\n")
        else {
            panic!("expected debug request");
        };

        assert_eq!(request.key.as_bytes(), b"key");
        assert_eq!(reply_plan.external_key.as_deref(), Some(b"key".as_slice()));
        assert_eq!(reply_plan.key_encoding, KeyEncoding::Text);
        assert_eq!(reply_plan.output_order.iter().count(), 0);
    }

    #[test]
    fn decodes_debug_at_every_split_point() {
        let input = b"me /region/cluster/key Pproxy Lpath/\r\n";

        for split in 0..=input.len() {
            let mut decoder = MetaRequestDecoder::new();
            let mut src = BytesMut::new();
            src.extend_from_slice(&input[..split]);
            if split < input.len() {
                assert_eq!(decoder.decode(&mut src), Ok(None), "split={split}");
                src.extend_from_slice(&input[split..]);
            }

            let DecodedMetaCommand::Request {
                request: Request::Debug(request),
                reply_plan,
            } = decoder.decode(&mut src).unwrap().unwrap()
            else {
                panic!("expected debug request at split={split}");
            };
            assert_eq!(request.key.as_bytes(), b"/region/cluster/key");
            assert_eq!(
                reply_plan.external_key.as_deref(),
                Some(b"/region/cluster/key".as_slice())
            );
            assert!(src.is_empty(), "split={split}");
        }
    }

    #[test]
    fn normalizes_base64_debug_key() {
        let DecodedMetaCommand::Request {
            request: Request::Debug(request),
            reply_plan,
        } = decode(b"me a2V5 b\r\n")
        else {
            panic!("expected debug request");
        };

        assert_eq!(request.key.as_bytes(), b"key");
        assert_eq!(reply_plan.external_key.as_deref(), Some(b"key".as_slice()));
        assert_eq!(reply_plan.key_encoding, KeyEncoding::Base64);
    }

    #[test]
    fn rejects_invalid_debug_flags() {
        assert_eq!(
            decode_error(b"me key b b\r\n"),
            recoverable_client_error(DUPLICATE_FLAG)
        );
        assert_eq!(
            decode_error(b"me key q\r\n"),
            recoverable_client_error(INVALID_FLAG)
        );
        assert_eq!(
            decode_error(b"me key P\r\n"),
            recoverable_client_error(BAD_COMMAND_LINE)
        );
    }

    #[test]
    fn decodes_binary_store_payload_at_every_split_point() {
        let input = b"ms key 6\r\na\r\nb\0c\r\n";

        for split in 0..=input.len() {
            let mut decoder = MetaRequestDecoder::new();
            let mut src = BytesMut::new();
            src.extend_from_slice(&input[..split]);
            if split < input.len() {
                assert_eq!(decoder.decode(&mut src), Ok(None), "split={split}");
                src.extend_from_slice(&input[split..]);
            }

            let DecodedMetaCommand::Request {
                request: Request::Store(request),
                ..
            } = decoder.decode(&mut src).unwrap().unwrap()
            else {
                panic!("expected store request at split={split}");
            };
            assert_eq!(request.value, b"a\r\nb\0c".as_slice());
            assert!(src.is_empty(), "split={split}");
        }
    }

    #[test]
    fn decodes_empty_store_value() {
        let DecodedMetaCommand::Request {
            request: Request::Store(request),
            ..
        } = decode(b"ms key 0\r\n\r\n")
        else {
            panic!("expected store request");
        };

        assert!(request.value.is_empty());
    }

    #[test]
    fn malformed_store_terminator_is_consumed_and_recoverable() {
        let mut decoder = MetaRequestDecoder::new();
        let mut src = BytesMut::from(&b"ms key 3\r\nfooXXmn\r\n"[..]);

        assert_eq!(
            decoder.decode(&mut src),
            Err(recoverable_client_error(BAD_DATA_CHUNK))
        );
        assert_eq!(src, b"mn\r\n".as_slice());
        assert_eq!(decoder.decode(&mut src), Ok(Some(DecodedMetaCommand::NoOp)));
    }

    #[test]
    fn invalid_store_header_swallows_declared_body() {
        let mut decoder = MetaRequestDecoder::new();
        let mut src = BytesMut::from(&b"ms key 3 z\r\nfoo\r\nmn\r\n"[..]);

        assert_eq!(
            decoder.decode(&mut src),
            Err(recoverable_client_error(INVALID_FLAG))
        );
        assert_eq!(src, b"mn\r\n".as_slice());
    }

    #[test]
    fn invalid_store_header_reports_once_fragmented_body_arrives() {
        let mut decoder = MetaRequestDecoder::new();
        let mut src = BytesMut::from(&b"ms key 3 z\r\nf"[..]);

        assert_eq!(decoder.decode(&mut src), Ok(None));

        src.extend_from_slice(b"oo\r\nmn\r\n");
        assert_eq!(
            decoder.decode(&mut src),
            Err(recoverable_client_error(INVALID_FLAG))
        );
        assert_eq!(src, b"mn\r\n".as_slice());
    }

    #[test]
    fn malformed_store_length_does_not_swallow_following_command() {
        let mut decoder = MetaRequestDecoder::new();
        let mut src = BytesMut::from(&b"ms key nope\r\nmn\r\n"[..]);

        assert_eq!(
            decoder.decode(&mut src),
            Err(recoverable_client_error(BAD_COMMAND_LINE))
        );
        assert_eq!(src, b"mn\r\n".as_slice());
    }

    #[test]
    fn oversized_store_swallows_body_before_server_error() {
        let value_len = MAX_VALUE_BYTES + 1;
        let mut input = format!("ms key {value_len}\r\n").into_bytes();
        input.extend(std::iter::repeat(b'x').take(value_len));
        input.extend_from_slice(b"\r\nmn\r\n");
        let mut decoder = MetaRequestDecoder::new();
        let mut src = BytesMut::from(input.as_slice());

        assert_eq!(
            decoder.decode(&mut src),
            Err(MetaRequestDecodeError::Recoverable(ErrorReply::Server(
                Some(Bytes::from_static(OBJECT_TOO_LARGE))
            )))
        );
        assert_eq!(src, b"mn\r\n".as_slice());
    }

    #[test]
    fn eof_rejects_partial_store_body_or_swallow() {
        let mut decoder = MetaRequestDecoder::new();
        let mut src = BytesMut::from(&b"ms key 3\r\nfo"[..]);
        assert_eq!(decoder.decode(&mut src), Ok(None));
        assert_eq!(
            decoder.decode_eof(&src),
            Err(MetaRequestDecodeError::Fatal(
                FatalDecodeError::UnexpectedEof
            ))
        );

        let mut decoder = MetaRequestDecoder::new();
        let mut src = BytesMut::from(&b"ms key 3 z\r\nf"[..]);
        assert_eq!(decoder.decode(&mut src), Ok(None));
        assert_eq!(
            decoder.decode_eof(&src),
            Err(MetaRequestDecodeError::Fatal(
                FatalDecodeError::UnexpectedEof
            ))
        );
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

    fn repeated_p_flags(count: usize) -> Vec<u8> {
        (0..count)
            .map(|index| format!("P{index}"))
            .collect::<Vec<_>>()
            .join(" ")
            .into_bytes()
    }

    #[test]
    fn enforces_get_flag_token_budget() {
        // Exactly 19 distinct valid mg flags exist: one over memcached's
        // budget of 18 (20 line tokens minus the command and the key).
        let over = b"mg AAAA b c f h k l q s t u v C1 E1 N30 R30 T30 Otag Pp Lx\r\n";
        assert_eq!(
            decode_error(over),
            recoverable_client_error(OPTIONS_FLAGS_TOO_LONG)
        );

        // The same line minus one flag sits at the budget and must parse.
        let DecodedMetaCommand::Request {
            request: Request::Get(request),
            reply_plan,
        } = decode(b"mg AAAA b c f h k l q s t u v C1 E1 N30 R30 T30 Otag Pp\r\n")
        else {
            panic!("expected get request at the flag budget");
        };
        assert!(request.return_value);
        assert_eq!(reply_plan.opaque.as_deref(), Some(b"tag".as_slice()));
    }

    #[test]
    fn arithmetic_and_debug_have_no_flag_token_budget() {
        // memcached parses `ma` and `me` on a path without the token budget;
        // 30 flag tokens must fail on the duplicate, not on a budget.
        let mut arithmetic = b"ma key ".to_vec();
        arithmetic.extend_from_slice(&repeated_p_flags(30));
        arithmetic.extend_from_slice(b"\r\n");
        assert_eq!(
            decode_error(&arithmetic),
            recoverable_client_error(DUPLICATE_FLAG)
        );

        let mut debug = b"me key ".to_vec();
        debug.extend_from_slice(&repeated_p_flags(30));
        debug.extend_from_slice(b"\r\n");
        assert_eq!(
            decode_error(&debug),
            recoverable_client_error(DUPLICATE_FLAG)
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
    fn leaves_fragmented_lines_untouched_until_complete() {
        let mut decoder = MetaRequestDecoder::new();
        let mut src = BytesMut::from(&b"mg user"[..]);

        assert_eq!(decoder.decode(&mut src), Ok(None));
        assert_eq!(src, b"mg user".as_slice());

        src.extend_from_slice(b":1");
        assert_eq!(decoder.decode(&mut src), Ok(None));
        assert_eq!(src, b"mg user:1".as_slice());

        src.extend_from_slice(b"\r\n");
        let DecodedMetaCommand::Request {
            request: Request::Get(request),
            ..
        } = decoder.decode(&mut src).unwrap().unwrap()
        else {
            panic!("expected get request");
        };
        assert_eq!(request.key.as_bytes(), b"user:1");
        assert!(src.is_empty());
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
