//! `mg`: parse/encode for the Meta get command on both proxy hops.

use bytes::{Bytes, BytesMut};

use crate::meta::numbers::{parse_i32, parse_i64, parse_u32, parse_u64};
use crate::meta::reply_decoder::{
    invalid_response, MetaReplyDecodeError, INVALID_RESPONSE, SHAPE_MISMATCH,
};
use crate::meta::reply_encoder::{
    reply_line_too_long, write_field, write_key_token, write_opaque, MetaReplyEncodeError,
};
use crate::meta::request_decoder::{
    bad_command_line, flag_error, parse_opaque, recoverable_client_error, require_hint_argument,
    resolve_key, DecodedMetaCommand, MetaRequestDecodeError, BAD_COMMAND_LINE, INVALID_FLAG,
    MAX_LINE_TOKENS,
};
use crate::meta::request_encoder::{
    command_line_too_long, write_backend_key, write_i32_flag, write_u64_flag,
    MetaRequestEncodeError,
};
use crate::meta::tokens::{flags, require_no_argument, split_tokens, FlagBudget};
use crate::meta::{
    wire, GetSuccessShape, KeyEncoding, MetaOutputToken, MetaQuietPolicy, MetaReplyPlan,
    MAX_COMMAND_LINE_BYTES, MAX_REPLY_LINE_BYTES, MAX_REPLY_VALUE_BYTES,
};
use crate::reply::{GetHit, GetReply, RecacheState, Reply};
use crate::request::{GetRequest, GetTemporalInstruction, GetTemporalInstructions, Request};

pub fn parse_request<'a>(
    mut tokens: impl Iterator<Item = &'a [u8]>,
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
    let mut return_key = false;

    // 20 line tokens minus `mg` and the key.
    for flag in flags(tokens, FlagBudget::Tokens(MAX_LINE_TOKENS - 2)) {
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
            b'C' => check_cas = Some(parse_u64(argument).map_err(bad_command_line)?),
            b'f' => {
                require_no_argument(argument).map_err(bad_command_line)?;
                return_client_flags = true;
                reply_plan
                    .output_order
                    .push(MetaOutputToken::ClientFlags)
                    .map_err(bad_command_line)?;
            }
            b'h' => {
                require_no_argument(argument).map_err(bad_command_line)?;
                return_hit_state = true;
                reply_plan
                    .output_order
                    .push(MetaOutputToken::HitState)
                    .map_err(bad_command_line)?;
            }
            b'k' => {
                require_no_argument(argument).map_err(bad_command_line)?;
                return_key = true;
                reply_plan
                    .output_order
                    .push(MetaOutputToken::Key)
                    .map_err(bad_command_line)?;
            }
            b'l' => {
                require_no_argument(argument).map_err(bad_command_line)?;
                return_last_access = true;
                reply_plan
                    .output_order
                    .push(MetaOutputToken::LastAccess)
                    .map_err(bad_command_line)?;
            }
            b'O' => parse_opaque(argument, &mut reply_plan)?,
            b'q' => {
                require_no_argument(argument).map_err(bad_command_line)?;
                reply_plan.quiet = MetaQuietPolicy::SuppressMiss;
            }
            b's' => {
                require_no_argument(argument).map_err(bad_command_line)?;
                return_size = true;
                reply_plan
                    .output_order
                    .push(MetaOutputToken::Size)
                    .map_err(bad_command_line)?;
            }
            b't' => {
                require_no_argument(argument).map_err(bad_command_line)?;
                temporal
                    .push(GetTemporalInstruction::ReturnTtl)
                    .map_err(bad_command_line)?;
                reply_plan
                    .output_order
                    .push(MetaOutputToken::Ttl)
                    .map_err(bad_command_line)?;
            }
            b'u' => {
                require_no_argument(argument).map_err(bad_command_line)?;
                no_lru_bump = true;
            }
            b'v' => {
                require_no_argument(argument).map_err(bad_command_line)?;
                return_value = true;
            }
            b'E' => override_cas = Some(parse_u64(argument).map_err(bad_command_line)?),
            b'N' => temporal
                .push(GetTemporalInstruction::Vivify(
                    parse_i32(argument).map_err(bad_command_line)?,
                ))
                .map_err(bad_command_line)?,
            b'R' => temporal
                .push(GetTemporalInstruction::WinForRecache(
                    parse_i32(argument).map_err(bad_command_line)?,
                ))
                .map_err(bad_command_line)?,
            b'T' => temporal
                .push(GetTemporalInstruction::UpdateTtl(
                    parse_i32(argument).map_err(bad_command_line)?,
                ))
                .map_err(bad_command_line)?,
            b'P' | b'L' => require_hint_argument(argument)?,
            _ => return Err(recoverable_client_error(INVALID_FLAG)),
        }
    }

    let key = resolve_key(raw_key, return_key, &mut reply_plan)?;
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

pub fn encode_request(
    request: &GetRequest,
    out: &mut BytesMut,
) -> Result<GetSuccessShape, MetaRequestEncodeError> {
    let line_start = out.len();
    out.extend_from_slice(b"mg ");
    let key_is_base64 = write_backend_key(out, &request.key)?;

    if key_is_base64 {
        wire::write_bare_flag(out, b'b');
    }

    // Direct fields use canonical order. Only the temporal program has
    // request-order semantics in memcached.
    if request.return_value {
        wire::write_bare_flag(out, b'v');
    }
    if request.return_client_flags {
        wire::write_bare_flag(out, b'f');
    }
    if request.return_cas {
        wire::write_bare_flag(out, b'c');
    }
    if request.return_size {
        wire::write_bare_flag(out, b's');
    }
    if request.return_hit_state {
        wire::write_bare_flag(out, b'h');
    }
    if request.return_last_access {
        wire::write_bare_flag(out, b'l');
    }
    if let Some(cas) = request.check_cas {
        write_u64_flag(out, b'C', cas);
    }
    if let Some(cas) = request.override_cas {
        write_u64_flag(out, b'E', cas);
    }
    if request.no_lru_bump {
        wire::write_bare_flag(out, b'u');
    }

    for instruction in request.temporal.iter() {
        match instruction {
            GetTemporalInstruction::Vivify(ttl) => write_i32_flag(out, b'N', *ttl),
            GetTemporalInstruction::UpdateTtl(ttl) => write_i32_flag(out, b'T', *ttl),
            GetTemporalInstruction::ReturnTtl => wire::write_bare_flag(out, b't'),
            GetTemporalInstruction::WinForRecache(ttl) => write_i32_flag(out, b'R', *ttl),
        }
    }

    wire::finish_line(out, line_start, MAX_COMMAND_LINE_BYTES).map_err(command_line_too_long)?;
    Ok(match (request.return_value, request.check_cas.is_some()) {
        (false, _) => GetSuccessShape::Header,
        (true, false) => GetSuccessShape::Value,
        (true, true) => GetSuccessShape::HeaderOrValue,
    })
}

pub fn parse_reply(
    shape: GetSuccessShape,
    line: &[u8],
    value: Option<Bytes>,
) -> Result<Reply, MetaReplyDecodeError> {
    let mut tokens = split_tokens(line);
    match tokens.next() {
        Some(b"EN") => {
            if tokens.next().is_some() {
                return Err(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE));
            }
            Ok(Reply::Get(GetReply::Miss))
        }
        Some(b"HD") => {
            if shape == GetSuccessShape::Value {
                return Err(MetaReplyDecodeError::InvalidResponse(SHAPE_MISMATCH));
            }
            let hit = parse_attributes(tokens)?;
            Ok(Reply::Get(GetReply::Hit(hit)))
        }
        Some(b"VA") => {
            if shape == GetSuccessShape::Header {
                return Err(MetaReplyDecodeError::InvalidResponse(SHAPE_MISMATCH));
            }
            // Framing validated the length token and sized `value` from it.
            if tokens.next().is_none() {
                return Err(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE));
            }
            let Some(value) = value else {
                return Err(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE));
            };
            let mut hit = parse_attributes(tokens)?;
            if hit.size.is_some_and(|size| size != value.len() as u64) {
                return Err(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE));
            }
            hit.value = Some(value);
            Ok(Reply::Get(GetReply::Hit(hit)))
        }
        _ => Err(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE)),
    }
}

fn parse_attributes<'a>(
    tokens: impl Iterator<Item = &'a [u8]>,
) -> Result<GetHit, MetaReplyDecodeError> {
    let mut hit = GetHit::default();

    for flag in flags(tokens, FlagBudget::Unlimited) {
        let (flag, argument) = flag.map_err(invalid_response)?;
        match flag {
            b'c' => hit.cas = Some(parse_u64(argument).map_err(invalid_response)?),
            b'f' => hit.client_flags = Some(parse_u32(argument).map_err(invalid_response)?),
            b's' => hit.size = Some(parse_u64(argument).map_err(invalid_response)?),
            b't' => hit.ttl = Some(parse_i64(argument).map_err(invalid_response)?),
            b'h' => {
                hit.hit_before = Some(match argument {
                    b"0" => false,
                    b"1" => true,
                    _ => return Err(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE)),
                });
            }
            b'l' => hit.last_access_seconds = Some(parse_u64(argument).map_err(invalid_response)?),
            b'W' => {
                require_no_argument(argument).map_err(invalid_response)?;
                if hit.recache != RecacheState::None {
                    return Err(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE));
                }
                hit.recache = RecacheState::Won;
            }
            b'Z' => {
                require_no_argument(argument).map_err(invalid_response)?;
                if hit.recache != RecacheState::None {
                    return Err(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE));
                }
                hit.recache = RecacheState::AlreadyWon;
            }
            b'X' => {
                require_no_argument(argument).map_err(invalid_response)?;
                hit.stale = true;
            }
            _ => return Err(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE)),
        }
    }
    Ok(hit)
}

pub fn encode_reply(
    reply: &GetReply,
    plan: &MetaReplyPlan,
    out: &mut BytesMut,
) -> Result<(), MetaReplyEncodeError> {
    match reply {
        GetReply::Miss => encode_miss(plan, out),
        GetReply::Hit(hit) => encode_hit(hit, plan, out),
    }
}

fn encode_miss(plan: &MetaReplyPlan, out: &mut BytesMut) -> Result<(), MetaReplyEncodeError> {
    let line_start = out.len();
    out.extend_from_slice(b"EN");
    for token in plan.output_order.iter() {
        match token {
            MetaOutputToken::Opaque => write_opaque(plan, out)?,
            MetaOutputToken::Key => write_key_token(plan, out)?,
            _ => {}
        }
    }
    wire::finish_line(out, line_start, MAX_REPLY_LINE_BYTES).map_err(reply_line_too_long)
}

fn encode_hit(
    hit: &GetHit,
    plan: &MetaReplyPlan,
    out: &mut BytesMut,
) -> Result<(), MetaReplyEncodeError> {
    let line_start = out.len();
    if let Some(value) = &hit.value {
        if value.len() > MAX_REPLY_VALUE_BYTES {
            return Err(MetaReplyEncodeError::ValueTooLarge {
                maximum: MAX_REPLY_VALUE_BYTES,
            });
        }
        if hit.size.is_some_and(|size| size != value.len() as u64) {
            return Err(MetaReplyEncodeError::SizeMismatch);
        }
        out.extend_from_slice(b"VA ");
        wire::write_u64(out, value.len() as u64);
    } else {
        out.extend_from_slice(b"HD");
    }

    for token in plan.output_order.iter() {
        match token {
            MetaOutputToken::Cas => {
                write_field(out, b'c', hit.cas, "CAS", true)?;
            }
            MetaOutputToken::ClientFlags => {
                write_field(
                    out,
                    b'f',
                    hit.client_flags.map(u64::from),
                    "client flags",
                    true,
                )?;
            }
            MetaOutputToken::Size => {
                write_field(out, b's', hit.size, "size", true)?;
            }
            MetaOutputToken::Ttl => {
                write_field(out, b't', hit.ttl, "TTL", true)?;
            }
            MetaOutputToken::HitState => {
                let value = hit
                    .hit_before
                    .ok_or(MetaReplyEncodeError::MissingField("hit state"))?;
                wire::write_bare_flag(out, b'h');
                out.extend_from_slice(if value { b"1" } else { b"0" });
            }
            MetaOutputToken::LastAccess => {
                write_field(out, b'l', hit.last_access_seconds, "last access", true)?;
            }
            MetaOutputToken::Opaque => write_opaque(plan, out)?,
            MetaOutputToken::Key => write_key_token(plan, out)?,
        }
    }

    if hit.recache == RecacheState::AlreadyWon {
        wire::write_bare_flag(out, b'Z');
    }
    if hit.stale {
        wire::write_bare_flag(out, b'X');
    }
    if hit.recache == RecacheState::Won {
        wire::write_bare_flag(out, b'W');
    }

    wire::finish_line(out, line_start, MAX_REPLY_LINE_BYTES).map_err(reply_line_too_long)?;
    if let Some(value) = &hit.value {
        out.extend_from_slice(value);
        out.extend_from_slice(b"\r\n");
    }
    Ok(())
}
