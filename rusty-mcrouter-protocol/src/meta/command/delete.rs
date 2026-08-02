//! `md`: parse/encode for the Meta delete command on both proxy hops.

use bytes::BytesMut;

use crate::meta::reply_decoder::{
    MetaReplyDecodeError, INVALID_RESPONSE, MAX_REPLY_LINE_BYTES, SHAPE_MISMATCH,
};
use crate::meta::reply_encoder::{
    reply_line_too_long, write_key_token, write_opaque, MetaReplyEncodeError,
};
use crate::meta::request_decoder::{
    bad_argument, bad_number, capacity_error, flag_error, parse_opaque, recoverable_client_error,
    require_hint_argument, resolve_key, DecodedMetaCommand, MetaRequestDecodeError,
    BAD_COMMAND_LINE, INVALID_FLAG, MAX_COMMAND_LINE_BYTES, MAX_LINE_TOKENS,
};
use crate::meta::request_encoder::{
    command_line_too_long, write_backend_key, write_i32_flag, write_u64_flag,
    MetaRequestEncodeError,
};
use crate::meta::tokens::{
    flags, parse_i32, parse_u32, parse_u64, require_no_argument, split_tokens, FlagBudget,
};
use crate::meta::{
    wire, KeyEncoding, MetaOutputToken, MetaQuietPolicy, MetaReplyExpectation, MetaReplyPlan,
};
use crate::reply::{DeleteReply, Reply};
use crate::request::{DeleteRequest, Request};

pub fn parse_request<'a>(
    mut tokens: impl Iterator<Item = &'a [u8]>,
) -> Result<DecodedMetaCommand, MetaRequestDecodeError> {
    let raw_key = tokens
        .next()
        .ok_or_else(|| recoverable_client_error(BAD_COMMAND_LINE))?;
    let mut compare_cas = None;
    let mut override_cas = None;
    let mut client_flags = None;
    let mut invalidate = false;
    let mut ttl = None;
    let mut remove_value = false;
    let mut reply_plan = MetaReplyPlan::default();
    let mut return_key = false;

    // 20 line tokens minus `md` and the key. Unreachable today (only 12
    // distinct valid md flags exist), kept for the same reason as `ms`.
    for flag in flags(tokens, FlagBudget::Tokens(MAX_LINE_TOKENS - 2)) {
        let (flag, argument) = flag.map_err(flag_error)?;
        match flag {
            b'b' => {
                require_no_argument(argument).map_err(bad_argument)?;
                reply_plan.key_encoding = KeyEncoding::Base64;
            }
            b'C' => compare_cas = Some(parse_u64(argument).map_err(bad_number)?),
            b'E' => override_cas = Some(parse_u64(argument).map_err(bad_number)?),
            b'F' => client_flags = Some(parse_u32(argument).map_err(bad_number)?),
            b'I' => {
                require_no_argument(argument).map_err(bad_argument)?;
                invalidate = true;
            }
            b'k' => {
                require_no_argument(argument).map_err(bad_argument)?;
                return_key = true;
                reply_plan
                    .output_order
                    .push(MetaOutputToken::Key)
                    .map_err(capacity_error)?;
            }
            b'O' => parse_opaque(argument, &mut reply_plan)?,
            b'q' => {
                require_no_argument(argument).map_err(bad_argument)?;
                reply_plan.quiet = MetaQuietPolicy::SuppressSuccess;
            }
            b'T' => ttl = Some(parse_i32(argument).map_err(bad_number)?),
            b'x' => {
                require_no_argument(argument).map_err(bad_argument)?;
                remove_value = true;
            }
            b'P' | b'L' => require_hint_argument(argument)?,
            _ => return Err(recoverable_client_error(INVALID_FLAG)),
        }
    }

    let key = resolve_key(raw_key, return_key, &mut reply_plan)?;
    Ok(DecodedMetaCommand::Request {
        request: Request::Delete(DeleteRequest {
            key,
            compare_cas,
            override_cas,
            client_flags,
            invalidate,
            ttl,
            remove_value,
        }),
        reply_plan,
    })
}

pub fn encode_request(
    request: &DeleteRequest,
    out: &mut BytesMut,
) -> Result<MetaReplyExpectation, MetaRequestEncodeError> {
    let line_start = out.len();
    out.extend_from_slice(b"md ");
    let key_is_base64 = write_backend_key(out, &request.key)?;

    if key_is_base64 {
        wire::write_bare_flag(out, b'b');
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
    if request.remove_value {
        wire::write_bare_flag(out, b'x');
    }

    wire::finish_line(out, line_start, MAX_COMMAND_LINE_BYTES).map_err(command_line_too_long)?;
    Ok(MetaReplyExpectation::Delete)
}

pub fn parse_reply(line: &[u8]) -> Result<Reply, MetaReplyDecodeError> {
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
    Ok(Reply::Delete(reply))
}

pub fn encode_reply(
    reply: &DeleteReply,
    plan: &MetaReplyPlan,
    out: &mut BytesMut,
) -> Result<(), MetaReplyEncodeError> {
    let code = match reply {
        DeleteReply::Success => b"HD".as_slice(),
        DeleteReply::NotStored => b"NS".as_slice(),
        DeleteReply::Exists => b"EX".as_slice(),
        DeleteReply::NotFound => b"NF".as_slice(),
    };

    let line_start = out.len();
    out.extend_from_slice(code);
    for token in plan.output_order.iter() {
        match token {
            MetaOutputToken::Opaque => write_opaque(plan, out)?,
            MetaOutputToken::Key => write_key_token(plan, out)?,
            _ => {
                return Err(MetaReplyEncodeError::InvalidData(
                    "invalid delete output token",
                ));
            }
        }
    }
    wire::finish_line(out, line_start, MAX_REPLY_LINE_BYTES).map_err(reply_line_too_long)
}
