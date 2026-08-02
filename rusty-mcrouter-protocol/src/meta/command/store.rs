//! `ms`: parse/encode for the Meta store command on both proxy hops.

use bytes::{Bytes, BytesMut};

use crate::meta::numbers::{parse_i32, parse_u32, parse_u64};
use crate::meta::reply_decoder::{
    invalid_response, MetaReplyDecodeError, INVALID_RESPONSE, SHAPE_MISMATCH,
};
use crate::meta::reply_encoder::{
    reply_line_too_long, write_field, write_key_token, write_opaque, MetaReplyEncodeError,
};
use crate::meta::request_decoder::{
    bad_command_line, flag_error, parse_opaque, recoverable_client_error, require_hint_argument,
    resolve_key, MetaRequestDecodeError, ParsedStoreHeader, StoreHeader, BAD_COMMAND_LINE,
    INVALID_FLAG, MAX_LINE_TOKENS, OBJECT_TOO_LARGE,
};
use crate::meta::request_encoder::{
    command_line_too_long, write_backend_key, write_i32_flag, write_mode_flag, write_u64_flag,
    MetaRequestEncodeError,
};
use crate::meta::tokens::{flags, require_no_argument, split_tokens, FlagBudget};
use crate::meta::{
    wire, KeyEncoding, MetaOutputToken, MetaQuietPolicy, MetaReplyPlan, MAX_COMMAND_LINE_BYTES,
    MAX_REPLY_LINE_BYTES, MAX_VALUE_BYTES,
};
use crate::reply::{ErrorReply, Reply, StoreReply, StoreResult};
use crate::request::{StoreMode, StoreRequest};

pub fn parse_request(
    line: &[u8],
    key_frame: Option<&Bytes>,
) -> Result<ParsedStoreHeader, MetaRequestDecodeError> {
    let mut tokens = split_tokens(line);
    if tokens.next() != Some(b"ms".as_slice()) {
        return Err(recoverable_client_error(BAD_COMMAND_LINE));
    }
    let raw_key = tokens
        .next()
        .ok_or_else(|| recoverable_client_error(BAD_COMMAND_LINE))?;
    let raw_value_len = tokens
        .next()
        .ok_or_else(|| recoverable_client_error(BAD_COMMAND_LINE))?;
    let value_len_u64 = parse_u64(raw_value_len).map_err(bad_command_line)?;
    if value_len_u64 > (i32::MAX - 2) as u64 {
        return Err(recoverable_client_error(BAD_COMMAND_LINE));
    }
    let value_len = value_len_u64 as usize;
    let remaining = value_len + 2;
    if value_len > MAX_VALUE_BYTES {
        return Ok(ParsedStoreHeader::Swallow {
            remaining,
            reply: ErrorReply::Server(Some(Bytes::from_static(OBJECT_TOO_LARGE))),
        });
    }

    let parsed = parse_request_fields(raw_key, tokens, key_frame, value_len);
    match parsed {
        Ok(header) => Ok(ParsedStoreHeader::Ready(header)),
        Err(MetaRequestDecodeError::Recoverable(reply)) => {
            Ok(ParsedStoreHeader::Swallow { remaining, reply })
        }
        Err(error) => Err(error),
    }
}

fn parse_request_fields<'a>(
    raw_key: &[u8],
    tokens: impl Iterator<Item = &'a [u8]>,
    key_frame: Option<&Bytes>,
    value_len: usize,
) -> Result<StoreHeader, MetaRequestDecodeError> {
    let mut return_cas = false;
    let mut return_size = false;
    let mut mode = StoreMode::Set;
    let mut client_flags = None;
    let mut ttl = None;
    let mut compare_cas = None;
    let mut override_cas = None;
    let mut invalidate = false;
    let mut vivify_ttl = None;
    let mut reply_plan = MetaReplyPlan::default();
    let mut return_key = false;

    // 20 line tokens minus `ms`, the key, and the datalen. Unreachable today
    // (only 16 distinct valid ms flags exist, and duplicates are rejected),
    // but kept so the budget survives future flag leniency. Raised here
    // rather than in `parse_request` so the body is swallowed.
    for flag in flags(tokens, FlagBudget::Tokens(MAX_LINE_TOKENS - 3)) {
        let (flag, argument) = flag.map_err(flag_error)?;
        match flag {
            b'b' => {
                require_no_argument(argument).map_err(bad_command_line)?;
                reply_plan.key_encoding = KeyEncoding::Base64;
            }
            b'c' => {
                require_no_argument(argument).map_err(bad_command_line)?;
                return_cas = true;
                reply_plan
                    .output_order
                    .push(MetaOutputToken::Cas)
                    .map_err(bad_command_line)?;
            }
            b'C' => compare_cas = Some(parse_u64(argument).map_err(bad_command_line)?),
            b'E' => override_cas = Some(parse_u64(argument).map_err(bad_command_line)?),
            b'F' => client_flags = Some(parse_u32(argument).map_err(bad_command_line)?),
            b'I' => {
                require_no_argument(argument).map_err(bad_command_line)?;
                invalidate = true;
            }
            b'k' => {
                require_no_argument(argument).map_err(bad_command_line)?;
                return_key = true;
                reply_plan
                    .output_order
                    .push(MetaOutputToken::Key)
                    .map_err(bad_command_line)?;
            }
            b'M' => {
                mode = match argument {
                    b"S" => StoreMode::Set,
                    b"E" => StoreMode::Add,
                    b"R" => StoreMode::Replace,
                    b"A" => StoreMode::Append,
                    b"P" => StoreMode::Prepend,
                    _ => return Err(recoverable_client_error(BAD_COMMAND_LINE)),
                };
            }
            b'N' => vivify_ttl = Some(parse_i32(argument).map_err(bad_command_line)?),
            b'O' => parse_opaque(argument, key_frame, &mut reply_plan)?,
            b'q' => {
                require_no_argument(argument).map_err(bad_command_line)?;
                reply_plan.quiet = MetaQuietPolicy::SuppressSuccess;
            }
            b's' => {
                require_no_argument(argument).map_err(bad_command_line)?;
                return_size = true;
                reply_plan
                    .output_order
                    .push(MetaOutputToken::Size)
                    .map_err(bad_command_line)?;
            }
            b'T' => ttl = Some(parse_i32(argument).map_err(bad_command_line)?),
            b'P' | b'L' => require_hint_argument(argument)?,
            _ => return Err(recoverable_client_error(INVALID_FLAG)),
        }
    }

    let key = resolve_key(raw_key, key_frame, return_key, &mut reply_plan)?;
    Ok(StoreHeader {
        key,
        value_len,
        return_cas,
        return_size,
        mode,
        client_flags,
        ttl,
        compare_cas,
        override_cas,
        invalidate,
        vivify_ttl,
        reply_plan,
    })
}

pub fn encode_request(
    request: &StoreRequest,
    out: &mut BytesMut,
) -> Result<(), MetaRequestEncodeError> {
    if request.value.len() > MAX_VALUE_BYTES {
        return Err(MetaRequestEncodeError::ValueTooLarge {
            maximum: MAX_VALUE_BYTES,
        });
    }

    let line_start = out.len();
    out.extend_from_slice(b"ms ");
    let key_is_base64 = write_backend_key(out, &request.key)?;
    out.extend_from_slice(b" ");
    wire::write_u64(out, request.value.len() as u64);

    if key_is_base64 {
        wire::write_bare_flag(out, b'b');
    }
    if request.return_cas {
        wire::write_bare_flag(out, b'c');
    }
    if request.return_size {
        wire::write_bare_flag(out, b's');
    }
    if let Some(cas) = request.compare_cas {
        write_u64_flag(out, b'C', cas);
    }
    if let Some(cas) = request.override_cas {
        write_u64_flag(out, b'E', cas);
    }
    if let Some(flags) = request.client_flags {
        write_u64_flag(out, b'F', u64::from(flags));
    }
    if request.invalidate {
        wire::write_bare_flag(out, b'I');
    }
    if let Some(ttl) = request.ttl {
        write_i32_flag(out, b'T', ttl);
    }
    match request.mode {
        StoreMode::Set => {}
        StoreMode::Add => write_mode_flag(out, b'E'),
        StoreMode::Replace => write_mode_flag(out, b'R'),
        StoreMode::Append => write_mode_flag(out, b'A'),
        StoreMode::Prepend => write_mode_flag(out, b'P'),
    }
    if let Some(ttl) = request.vivify_ttl {
        write_i32_flag(out, b'N', ttl);
    }

    wire::finish_line(out, line_start, MAX_COMMAND_LINE_BYTES).map_err(command_line_too_long)?;
    out.extend_from_slice(&request.value);
    out.extend_from_slice(b"\r\n");
    Ok(())
}

pub fn parse_reply(
    expect_cas: bool,
    expect_size: bool,
    line: &[u8],
) -> Result<Reply, MetaReplyDecodeError> {
    let mut tokens = split_tokens(line);
    let code = tokens
        .next()
        .ok_or(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE))?;
    let result = parse_attributes(tokens)?;
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
    Ok(Reply::Store(reply))
}

fn parse_attributes<'a>(
    tokens: impl Iterator<Item = &'a [u8]>,
) -> Result<StoreResult, MetaReplyDecodeError> {
    let mut result = StoreResult::default();

    for flag in flags(tokens, FlagBudget::Unlimited) {
        let (flag, argument) = flag.map_err(invalid_response)?;
        match flag {
            b'c' => result.cas = Some(parse_u64(argument).map_err(invalid_response)?),
            b's' => result.size = Some(parse_u64(argument).map_err(invalid_response)?),
            _ => return Err(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE)),
        }
    }
    Ok(result)
}

pub fn encode_reply(
    reply: &StoreReply,
    plan: &MetaReplyPlan,
    out: &mut BytesMut,
) -> Result<(), MetaReplyEncodeError> {
    let (code, result) = match reply {
        StoreReply::Success(result) => (b"HD".as_slice(), result),
        StoreReply::NotStored(result) => (b"NS".as_slice(), result),
        StoreReply::Exists(result) => (b"EX".as_slice(), result),
        StoreReply::NotFound(result) => (b"NF".as_slice(), result),
    };

    let line_start = out.len();
    out.extend_from_slice(code);
    for token in plan.output_order.iter() {
        match token {
            MetaOutputToken::Cas => write_field(out, b'c', result.cas, "CAS", true)?,
            MetaOutputToken::Size => write_field(out, b's', result.size, "size", true)?,
            MetaOutputToken::Opaque => write_opaque(plan, out)?,
            MetaOutputToken::Key => write_key_token(plan, out)?,
            _ => {
                return Err(MetaReplyEncodeError::InvalidData(
                    "invalid store output token",
                ));
            }
        }
    }
    wire::finish_line(out, line_start, MAX_REPLY_LINE_BYTES).map_err(reply_line_too_long)
}
