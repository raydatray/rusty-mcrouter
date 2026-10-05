use base64::{engine::general_purpose::STANDARD, Engine as _};
use bytes::{Bytes, BytesMut};

use crate::meta::read::{flags, require_no_argument, split_tokens, Flag, FlagBudget};
use crate::meta::reply_decoder::{INVALID_RESPONSE, MAX_REPLY_LINE_BYTES, SHAPE_MISMATCH};
use crate::meta::request_decoder::{
    parse_key, recoverable_client_error, require_hint_argument, BAD_COMMAND_LINE, INVALID_FLAG,
    MAX_COMMAND_LINE_BYTES,
};
use crate::meta::request_encoder::write_backend_key;
use crate::meta::write::{finish_line, write_bare_flag, write_base64_key};
use crate::meta::{
    DecodedMetaCommand, KeyEncoding, MetaReplyDecodeError, MetaReplyEncodeError,
    MetaReplyExpectation, MetaReplyPlan, MetaRequestDecodeError, MetaRequestEncodeError,
};
use crate::reply::{DebugField, DebugHit, DebugReply};
use crate::request::DebugRequest;
use crate::{Reply, Request};

/// `me` response carries a small, fixed set of `<name>=<value>` fields, this
/// cap bounds a misbehaving backend
pub const MAX_DEBUG_FIELDS: usize = 64;

pub fn parse_request<'a>(
    mut tokens: impl Iterator<Item = &'a [u8]>,
) -> Result<DecodedMetaCommand, MetaRequestDecodeError> {
    let raw_key = tokens
        .next()
        .ok_or_else(|| recoverable_client_error(BAD_COMMAND_LINE))?;

    let mut key_encoding = KeyEncoding::Text;

    // `me` has no upstream token budget
    for flag in flags(tokens, FlagBudget::Unlimited) {
        let Flag { letter, argument } = flag?;
        match letter {
            b'b' => {
                require_no_argument(argument)?;

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
        write_bare_flag(out, b'b');
    }

    finish_line(out, line_start, MAX_COMMAND_LINE_BYTES)?;

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

            // memcached echoes the key as stored on the item (either text or
            // base64 upon item creation), so accept either form
            let key_matches = returned_key == expected_key.as_ref()
                || STANDARD
                    .decode(returned_key)
                    .is_ok_and(|decoded| decoded == expected_key.as_ref());
            if !key_matches {
                return Err(MetaReplyDecodeError::InvalidResponse(SHAPE_MISMATCH));
            }

            let fields = tokens
                .enumerate()
                .map(|(index, token)| {
                    if index >= MAX_DEBUG_FIELDS {
                        return Err(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE));
                    }

                    let separator = token
                        .iter()
                        .position(|byte| *byte == b'=')
                        .filter(|&separator| separator != 0)
                        .ok_or(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE))?;

                    Ok(DebugField {
                        name: Bytes::copy_from_slice(&token[..separator]),
                        value: Bytes::copy_from_slice(&token[separator + 1..]),
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;

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

    finish_line(out, line_start, MAX_REPLY_LINE_BYTES)?;

    Ok(())
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
            write_base64_key(out, key)?;
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
