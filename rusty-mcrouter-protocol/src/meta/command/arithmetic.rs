//! `ma`: parse/encode for the Meta arithmetic command on both proxy hops.

use bytes::{Bytes, BytesMut};

use crate::meta::numbers::{parse_i32, parse_i64, parse_u64};
use crate::meta::reply_decoder::{
    invalid_response, MetaReplyDecodeError, INVALID_RESPONSE, SHAPE_MISMATCH,
};
use crate::meta::reply_encoder::{
    reply_line_too_long, write_field, write_key_token, write_opaque, MetaReplyEncodeError,
};
use crate::meta::request_decoder::{
    bad_command_line, flag_error, parse_opaque, recoverable_client_error, require_hint_argument,
    resolve_key, DecodedMetaCommand, MetaRequestDecodeError, BAD_COMMAND_LINE, INVALID_FLAG,
};
use crate::meta::request_encoder::{
    command_line_too_long, write_backend_key, write_i32_flag, write_mode_flag, write_u64_flag,
    MetaRequestEncodeError,
};
use crate::meta::tokens::{flags, require_no_argument, split_tokens, FlagBudget};
use crate::meta::{
    wire, KeyEncoding, MetaOutputToken, MetaQuietPolicy, MetaReplyPlan, MAX_COMMAND_LINE_BYTES,
    MAX_REPLY_LINE_BYTES,
};
use crate::reply::{ArithmeticReply, ArithmeticResult, Reply};
use crate::request::{
    ArithmeticMode, ArithmeticRequest, ArithmeticTemporalInstruction,
    ArithmeticTemporalInstructions, Request,
};

pub fn parse_request<'a>(
    mut tokens: impl Iterator<Item = &'a [u8]>,
    key_frame: Option<&Bytes>,
) -> Result<DecodedMetaCommand, MetaRequestDecodeError> {
    let raw_key = tokens
        .next()
        .ok_or_else(|| recoverable_client_error(BAD_COMMAND_LINE))?;
    let mut return_value = false;
    let mut return_cas = false;
    let mut mode = ArithmeticMode::Increment;
    let mut delta = 1;
    let mut initial_value = None;
    let mut compare_cas = None;
    let mut override_cas = None;
    let mut temporal = ArithmeticTemporalInstructions::default();
    let mut reply_plan = MetaReplyPlan::default();
    let mut return_key = false;

    // `ma` has no upstream token budget. The loop still terminates quickly:
    // non-alphabetic or repeated flags error out, so at most 52 distinct
    // letters are ever processed.
    for flag in flags(tokens, FlagBudget::Unlimited) {
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
            b'D' => delta = parse_u64(argument).map_err(bad_command_line)?,
            b'E' => override_cas = Some(parse_u64(argument).map_err(bad_command_line)?),
            b'J' => initial_value = Some(parse_u64(argument).map_err(bad_command_line)?),
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
                    b"I" | b"+" => ArithmeticMode::Increment,
                    b"D" | b"-" => ArithmeticMode::Decrement,
                    _ => return Err(recoverable_client_error(BAD_COMMAND_LINE)),
                };
            }
            b'N' => temporal
                .push(ArithmeticTemporalInstruction::Vivify(
                    parse_i32(argument).map_err(bad_command_line)?,
                ))
                .map_err(bad_command_line)?,
            b'O' => parse_opaque(argument, key_frame, &mut reply_plan)?,
            b'q' => {
                require_no_argument(argument).map_err(bad_command_line)?;
                reply_plan.quiet = MetaQuietPolicy::SuppressSuccess;
            }
            b't' => {
                require_no_argument(argument).map_err(bad_command_line)?;
                temporal
                    .push(ArithmeticTemporalInstruction::ReturnTtl)
                    .map_err(bad_command_line)?;
                reply_plan
                    .output_order
                    .push(MetaOutputToken::Ttl)
                    .map_err(bad_command_line)?;
            }
            b'T' => temporal
                .push(ArithmeticTemporalInstruction::UpdateTtl(
                    parse_i32(argument).map_err(bad_command_line)?,
                ))
                .map_err(bad_command_line)?,
            b'v' => {
                require_no_argument(argument).map_err(bad_command_line)?;
                return_value = true;
            }
            b'P' | b'L' => require_hint_argument(argument)?,
            _ => return Err(recoverable_client_error(INVALID_FLAG)),
        }
    }

    let has_vivify = temporal
        .iter()
        .any(|instruction| matches!(instruction, ArithmeticTemporalInstruction::Vivify(_)));
    if initial_value.is_some() && !has_vivify {
        return Err(recoverable_client_error(BAD_COMMAND_LINE));
    }

    let key = resolve_key(raw_key, key_frame, return_key, &mut reply_plan)?;
    Ok(DecodedMetaCommand::Request {
        request: Request::Arithmetic(ArithmeticRequest {
            key,
            return_value,
            return_cas,
            mode,
            delta,
            initial_value,
            compare_cas,
            override_cas,
            temporal,
        }),
        reply_plan,
    })
}

pub fn encode_request(
    request: &ArithmeticRequest,
    out: &mut BytesMut,
) -> Result<(), MetaRequestEncodeError> {
    let line_start = out.len();
    out.extend_from_slice(b"ma ");
    let key_is_base64 = write_backend_key(out, &request.key)?;

    if key_is_base64 {
        wire::write_bare_flag(out, b'b');
    }
    if request.return_value {
        wire::write_bare_flag(out, b'v');
    }
    if request.return_cas {
        wire::write_bare_flag(out, b'c');
    }
    if let Some(cas) = request.compare_cas {
        write_u64_flag(out, b'C', cas);
    }
    if let Some(cas) = request.override_cas {
        write_u64_flag(out, b'E', cas);
    }
    if let Some(initial) = request.initial_value {
        write_u64_flag(out, b'J', initial);
    }
    if request.delta != 1 {
        write_u64_flag(out, b'D', request.delta);
    }
    if request.mode == ArithmeticMode::Decrement {
        write_mode_flag(out, b'D');
    }
    for instruction in request.temporal.iter() {
        match instruction {
            ArithmeticTemporalInstruction::Vivify(ttl) => write_i32_flag(out, b'N', *ttl),
            ArithmeticTemporalInstruction::UpdateTtl(ttl) => write_i32_flag(out, b'T', *ttl),
            ArithmeticTemporalInstruction::ReturnTtl => wire::write_bare_flag(out, b't'),
        }
    }

    wire::finish_line(out, line_start, MAX_COMMAND_LINE_BYTES).map_err(command_line_too_long)?;
    Ok(())
}

pub fn parse_reply(
    expect_value: bool,
    expect_cas: bool,
    expect_ttl: bool,
    line: &[u8],
    value: Option<Bytes>,
) -> Result<Reply, MetaReplyDecodeError> {
    let mut tokens = split_tokens(line);
    let code = tokens
        .next()
        .ok_or(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE))?;

    match code {
        b"HD" => {
            if expect_value {
                return Err(MetaReplyDecodeError::InvalidResponse(SHAPE_MISMATCH));
            }
            let result = parse_attributes(tokens)?;
            validate_success(&result, expect_cas, expect_ttl)?;
            Ok(Reply::Arithmetic(ArithmeticReply::Success(result)))
        }
        b"VA" => {
            if !expect_value {
                return Err(MetaReplyDecodeError::InvalidResponse(SHAPE_MISMATCH));
            }
            // Framing validated the length token and sized `value` from it.
            if tokens.next().is_none() {
                return Err(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE));
            }
            let Some(value) = value else {
                return Err(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE));
            };
            let mut result = parse_attributes(tokens)?;
            validate_success(&result, expect_cas, expect_ttl)?;
            result.value = Some(parse_u64(&value).map_err(invalid_response)?);
            Ok(Reply::Arithmetic(ArithmeticReply::Success(result)))
        }
        b"NS" => Ok(Reply::Arithmetic(ArithmeticReply::NotStored(
            parse_attributes(tokens)?,
        ))),
        b"EX" => Ok(Reply::Arithmetic(ArithmeticReply::Exists(
            parse_attributes(tokens)?,
        ))),
        b"NF" => Ok(Reply::Arithmetic(ArithmeticReply::NotFound(
            parse_attributes(tokens)?,
        ))),
        _ => Err(MetaReplyDecodeError::InvalidResponse(SHAPE_MISMATCH)),
    }
}

fn parse_attributes<'a>(
    tokens: impl Iterator<Item = &'a [u8]>,
) -> Result<ArithmeticResult, MetaReplyDecodeError> {
    let mut result = ArithmeticResult::default();

    for flag in flags(tokens, FlagBudget::Unlimited) {
        let (flag, argument) = flag.map_err(invalid_response)?;
        match flag {
            b'c' => result.cas = Some(parse_u64(argument).map_err(invalid_response)?),
            b't' => result.ttl = Some(parse_i64(argument).map_err(invalid_response)?),
            _ => return Err(MetaReplyDecodeError::InvalidResponse(INVALID_RESPONSE)),
        }
    }
    Ok(result)
}

fn validate_success(
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

pub fn encode_reply(
    reply: &ArithmeticReply,
    plan: &MetaReplyPlan,
    out: &mut BytesMut,
) -> Result<(), MetaReplyEncodeError> {
    let (code, result, success) = match reply {
        ArithmeticReply::Success(result) => {
            if result.value.is_some() {
                (b"VA".as_slice(), result, true)
            } else {
                (b"HD".as_slice(), result, true)
            }
        }
        ArithmeticReply::NotStored(result) => (b"NS".as_slice(), result, false),
        ArithmeticReply::Exists(result) => (b"EX".as_slice(), result, false),
        ArithmeticReply::NotFound(result) => (b"NF".as_slice(), result, false),
    };
    if !success && result.value.is_some() {
        return Err(MetaReplyEncodeError::InvalidData(
            "arithmetic failure contains a value",
        ));
    }

    let mut value_digits = [0; 20];
    let value_body = result
        .value
        .map(|value| wire::format_u64(value, &mut value_digits));
    let line_start = out.len();
    out.extend_from_slice(code);
    if let Some(value) = value_body {
        out.extend_from_slice(b" ");
        wire::write_u64(out, value.len() as u64);
    }

    for token in plan.output_order.iter() {
        match token {
            MetaOutputToken::Cas => {
                write_field(out, b'c', result.cas, "CAS", success)?;
            }
            MetaOutputToken::Ttl => {
                write_field(out, b't', result.ttl, "TTL", success)?;
            }
            MetaOutputToken::Opaque => write_opaque(plan, out)?,
            MetaOutputToken::Key => write_key_token(plan, out)?,
            _ => {
                return Err(MetaReplyEncodeError::InvalidData(
                    "invalid arithmetic output token",
                ));
            }
        }
    }
    wire::finish_line(out, line_start, MAX_REPLY_LINE_BYTES).map_err(reply_line_too_long)?;
    if let Some(value) = value_body {
        out.extend_from_slice(value);
        out.extend_from_slice(b"\r\n");
    }
    Ok(())
}
