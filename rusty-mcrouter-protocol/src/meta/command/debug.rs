//! `me`: parse/encode for the Meta debug command on both proxy hops.

use base64::{engine::general_purpose::STANDARD, Engine as _};
use bytes::{Bytes, BytesMut};

use crate::meta::reply_decoder::MAX_REPLY_LINE_BYTES;
use crate::meta::reply_decoder::{MetaReplyDecodeError, INVALID_RESPONSE, SHAPE_MISMATCH};
use crate::meta::reply_encoder::{encoded_key_too_long, reply_line_too_long, MetaReplyEncodeError};
use crate::meta::request_decoder::MAX_COMMAND_LINE_BYTES;
use crate::meta::request_decoder::{
    bad_argument, flag_error, parse_key, recoverable_client_error, require_hint_argument,
    DecodedMetaCommand, MetaRequestDecodeError, BAD_COMMAND_LINE, INVALID_FLAG,
};
use crate::meta::request_encoder::{
    command_line_too_long, write_backend_key, MetaRequestEncodeError,
};
use crate::meta::tokens::{flags, require_no_argument, split_tokens, FlagBudget};
use crate::meta::{wire, KeyEncoding, MetaReplyExpectation, MetaReplyPlan};

/// memcached's `me` response carries a small, fixed set of `<name>=<value>`
/// fields; the cap bounds a misbehaving backend.
pub const MAX_DEBUG_FIELDS: usize = 64;
use crate::reply::{DebugField, DebugHit, DebugReply, Reply};
use crate::request::{DebugRequest, Request};

pub fn parse_request<'a>(
    mut tokens: impl Iterator<Item = &'a [u8]>,
) -> Result<DecodedMetaCommand, MetaRequestDecodeError> {
    let raw_key = tokens
        .next()
        .ok_or_else(|| recoverable_client_error(BAD_COMMAND_LINE))?;
    let mut key_encoding = KeyEncoding::Text;

    // `me` has no upstream token budget; see the `ma` note on termination.
    for flag in flags(tokens, FlagBudget::Unlimited) {
        let (flag, argument) = flag.map_err(flag_error)?;
        match flag {
            b'b' => {
                require_no_argument(argument).map_err(bad_argument)?;
                key_encoding = KeyEncoding::Base64;
            }
            b'P' | b'L' => require_hint_argument(argument)?,
            _ => return Err(recoverable_client_error(INVALID_FLAG)),
        }
    }

    let key = parse_key(raw_key, key_encoding)?;
    let reply_plan = MetaReplyPlan {
        external_key: Some(key.clone_bytes()),
        key_encoding,
        ..MetaReplyPlan::default()
    };
    Ok(DecodedMetaCommand::Request {
        request: Request::Debug(DebugRequest { key }),
        reply_plan,
    })
}

pub fn encode_request(
    request: &DebugRequest,
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

pub fn parse_reply(expected_key: &Bytes, line: &[u8]) -> Result<Reply, MetaReplyDecodeError> {
    let mut tokens = split_tokens(line);
    match tokens.next() {
        Some(b"EN") => {
            if tokens.next().is_some() {
                return Err(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE));
            }
            Ok(Reply::Debug(DebugReply::Miss))
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
            Ok(Reply::Debug(DebugReply::Hit(DebugHit { fields })))
        }
        _ => Err(MetaReplyDecodeError::InvalidResponse(SHAPE_MISMATCH)),
    }
}

pub fn encode_reply(
    reply: &DebugReply,
    plan: &MetaReplyPlan,
    out: &mut BytesMut,
) -> Result<(), MetaReplyEncodeError> {
    if plan.output_order.iter().next().is_some() {
        return Err(MetaReplyEncodeError::InvalidData(
            "debug reply has an output-token plan",
        ));
    }
    let line_start = out.len();
    match reply {
        DebugReply::Miss => out.extend_from_slice(b"EN"),
        DebugReply::Hit(hit) => {
            out.extend_from_slice(b"ME ");
            write_key(plan, out)?;
            write_fields(hit, out)?;
        }
    }
    wire::finish_line(out, line_start, MAX_REPLY_LINE_BYTES).map_err(reply_line_too_long)
}

fn write_key(plan: &MetaReplyPlan, out: &mut BytesMut) -> Result<(), MetaReplyEncodeError> {
    let key = plan
        .external_key
        .as_ref()
        .ok_or(MetaReplyEncodeError::MissingField("external key"))?;
    if key.is_empty() {
        return Err(MetaReplyEncodeError::InvalidData("empty external key"));
    }
    match plan.key_encoding {
        KeyEncoding::Text => {
            if key.iter().any(|byte| *byte <= b' ' || *byte == 0x7f) {
                return Err(MetaReplyEncodeError::InvalidData("invalid external key"));
            }
            out.extend_from_slice(key);
        }
        KeyEncoding::Base64 => {
            wire::write_base64_key(out, key).map_err(encoded_key_too_long)?;
        }
    }
    Ok(())
}

fn write_fields(hit: &DebugHit, out: &mut BytesMut) -> Result<(), MetaReplyEncodeError> {
    if hit.fields.len() > MAX_DEBUG_FIELDS {
        return Err(MetaReplyEncodeError::InvalidData("too many debug fields"));
    }
    for field in &hit.fields {
        if field.name.is_empty()
            || field
                .name
                .iter()
                .any(|byte| *byte <= b' ' || *byte == b'=' || *byte == 0x7f)
            || field
                .value
                .iter()
                .any(|byte| *byte <= b' ' || *byte == 0x7f)
        {
            return Err(MetaReplyEncodeError::InvalidData("invalid debug field"));
        }
        out.extend_from_slice(b" ");
        out.extend_from_slice(&field.name);
        out.extend_from_slice(b"=");
        out.extend_from_slice(&field.value);
    }
    Ok(())
}
