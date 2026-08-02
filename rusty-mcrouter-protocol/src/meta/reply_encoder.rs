use base64::{engine::general_purpose::STANDARD, Engine as _};
use bytes::{Bytes, BytesMut};
use thiserror::Error;

use crate::{
    key::MAX_KEY_BYTES,
    reply::{ErrorReply, GetHit, GetReply, RecacheState, Reply, StoreReply},
};

use super::{
    KeyEncoding, MetaOutputToken, MetaQuietPolicy, MetaReplyPlan, MAX_REPLY_LINE_BYTES,
    MAX_REPLY_VALUE_BYTES,
};

const MAX_BASE64_KEY_BYTES: usize = MAX_KEY_BYTES.div_ceil(3) * 4;

#[derive(Debug, Default)]
pub struct MetaReplyEncoder;

impl MetaReplyEncoder {
    pub const fn new() -> Self {
        Self
    }

    /// Appends one frontend reply. On error, `out` is unchanged.
    pub fn encode(
        &self,
        reply: &Reply,
        plan: &MetaReplyPlan,
        out: &mut BytesMut,
    ) -> Result<ReplyEncodeStatus, MetaReplyEncodeError> {
        if matches!(
            (reply, plan.quiet),
            (Reply::Get(GetReply::Miss), MetaQuietPolicy::SuppressMiss)
                | (
                    Reply::Store(StoreReply::Success(_)),
                    MetaQuietPolicy::SuppressSuccess
                )
        ) {
            return Ok(ReplyEncodeStatus::Suppressed);
        }

        let checkpoint = out.len();
        let result = match reply {
            Reply::Get(reply) => encode_get(reply, plan, out),
            Reply::Store(reply) => encode_store(reply, plan, out),
            Reply::Error(reply) => encode_error(reply, out),
            _ => Err(MetaReplyEncodeError::UnsupportedReply),
        };
        if result.is_err() {
            out.truncate(checkpoint);
        }
        result.map(|()| ReplyEncodeStatus::Written)
    }

    pub fn encode_noop(&self, out: &mut BytesMut) {
        out.extend_from_slice(b"MN\r\n");
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplyEncodeStatus {
    Written,
    Suppressed,
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum MetaReplyEncodeError {
    #[error("reply operation is not implemented by the Meta encoder")]
    UnsupportedReply,

    #[error("get reply is missing requested {0}")]
    MissingField(&'static str),

    #[error("get reply value and size token disagree")]
    SizeMismatch,

    #[error("Meta reply value exceeds the {maximum}-byte limit")]
    ValueTooLarge { maximum: usize },

    #[error("base64-encoded response key exceeds the {maximum}-byte limit")]
    EncodedKeyTooLong { maximum: usize },

    #[error("invalid Meta reply data: {0}")]
    InvalidData(&'static str),

    #[error("Meta reply exceeds the {maximum}-byte line limit")]
    FrameTooLarge { maximum: usize },
}

fn encode_get(
    reply: &GetReply,
    plan: &MetaReplyPlan,
    out: &mut BytesMut,
) -> Result<(), MetaReplyEncodeError> {
    match reply {
        GetReply::Miss => encode_get_miss(plan, out),
        GetReply::Hit(hit) => encode_get_hit(hit, plan, out),
    }
}

fn encode_get_miss(plan: &MetaReplyPlan, out: &mut BytesMut) -> Result<(), MetaReplyEncodeError> {
    let line_start = out.len();
    out.extend_from_slice(b"EN");
    for token in plan.output_order.iter() {
        match token {
            MetaOutputToken::Opaque => write_opaque(plan, out)?,
            MetaOutputToken::Key => write_key(plan, out)?,
            _ => {}
        }
    }
    finish_line(out, line_start)
}

fn encode_get_hit(
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
        write_u64(out, value.len() as u64);
    } else {
        out.extend_from_slice(b"HD");
    }

    for token in plan.output_order.iter() {
        match token {
            MetaOutputToken::Cas => {
                write_required_u64(out, b'c', hit.cas, "CAS")?;
            }
            MetaOutputToken::ClientFlags => {
                write_required_u64(out, b'f', hit.client_flags.map(u64::from), "client flags")?;
            }
            MetaOutputToken::Size => {
                write_required_u64(out, b's', hit.size, "size")?;
            }
            MetaOutputToken::Ttl => {
                write_required_i64(out, b't', hit.ttl, "TTL")?;
            }
            MetaOutputToken::HitState => {
                let value = hit
                    .hit_before
                    .ok_or(MetaReplyEncodeError::MissingField("hit state"))?;
                write_bare_flag(out, b'h');
                out.extend_from_slice(if value { b"1" } else { b"0" });
            }
            MetaOutputToken::LastAccess => {
                write_required_u64(out, b'l', hit.last_access_seconds, "last access")?;
            }
            MetaOutputToken::Opaque => write_opaque(plan, out)?,
            MetaOutputToken::Key => write_key(plan, out)?,
        }
    }

    if hit.recache == RecacheState::AlreadyWon {
        write_bare_flag(out, b'Z');
    }
    if hit.stale {
        write_bare_flag(out, b'X');
    }
    if hit.recache == RecacheState::Won {
        write_bare_flag(out, b'W');
    }

    finish_line(out, line_start)?;
    if let Some(value) = &hit.value {
        out.extend_from_slice(value);
        out.extend_from_slice(b"\r\n");
    }
    Ok(())
}

fn encode_store(
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
            MetaOutputToken::Cas => write_required_u64(out, b'c', result.cas, "CAS")?,
            MetaOutputToken::Size => write_required_u64(out, b's', result.size, "size")?,
            MetaOutputToken::Opaque => write_opaque(plan, out)?,
            MetaOutputToken::Key => write_key(plan, out)?,
            _ => {
                return Err(MetaReplyEncodeError::InvalidData(
                    "invalid store output token",
                ));
            }
        }
    }
    finish_line(out, line_start)
}

fn encode_error(reply: &ErrorReply, out: &mut BytesMut) -> Result<(), MetaReplyEncodeError> {
    let line_start = out.len();
    match reply {
        ErrorReply::Error => out.extend_from_slice(b"ERROR"),
        ErrorReply::Client(message) => {
            out.extend_from_slice(b"CLIENT_ERROR");
            write_error_message(out, message.as_ref())?;
        }
        ErrorReply::Server(message) => {
            out.extend_from_slice(b"SERVER_ERROR");
            write_error_message(out, message.as_ref())?;
        }
    }
    finish_line(out, line_start)
}

fn write_error_message(
    out: &mut BytesMut,
    message: Option<&Bytes>,
) -> Result<(), MetaReplyEncodeError> {
    let Some(message) = message else {
        return Ok(());
    };
    if message.is_empty() || message.iter().any(|byte| matches!(byte, b'\r' | b'\n')) {
        return Err(MetaReplyEncodeError::InvalidData("invalid error message"));
    }
    out.extend_from_slice(b" ");
    out.extend_from_slice(message);
    Ok(())
}

fn write_opaque(plan: &MetaReplyPlan, out: &mut BytesMut) -> Result<(), MetaReplyEncodeError> {
    let opaque = plan
        .opaque
        .as_ref()
        .ok_or(MetaReplyEncodeError::MissingField("opaque token"))?;
    if opaque.is_empty() || opaque.len() > super::MAX_OPAQUE_BYTES {
        return Err(MetaReplyEncodeError::InvalidData("invalid opaque token"));
    }
    write_bare_flag(out, b'O');
    out.extend_from_slice(opaque);
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

    write_bare_flag(out, b'k');
    match plan.key_encoding {
        KeyEncoding::Text => out.extend_from_slice(key),
        KeyEncoding::Base64 => {
            let mut encoded = [0; MAX_BASE64_KEY_BYTES];
            let encoded_len = STANDARD.encode_slice(key, &mut encoded).map_err(|_| {
                MetaReplyEncodeError::EncodedKeyTooLong {
                    maximum: MAX_KEY_BYTES,
                }
            })?;
            if encoded_len > MAX_KEY_BYTES {
                return Err(MetaReplyEncodeError::EncodedKeyTooLong {
                    maximum: MAX_KEY_BYTES,
                });
            }
            out.extend_from_slice(&encoded[..encoded_len]);
            write_bare_flag(out, b'b');
        }
    }
    Ok(())
}

fn write_required_u64(
    out: &mut BytesMut,
    flag: u8,
    value: Option<u64>,
    name: &'static str,
) -> Result<(), MetaReplyEncodeError> {
    let value = value.ok_or(MetaReplyEncodeError::MissingField(name))?;
    write_bare_flag(out, flag);
    write_u64(out, value);
    Ok(())
}

fn write_required_i64(
    out: &mut BytesMut,
    flag: u8,
    value: Option<i64>,
    name: &'static str,
) -> Result<(), MetaReplyEncodeError> {
    let value = value.ok_or(MetaReplyEncodeError::MissingField(name))?;
    write_bare_flag(out, flag);
    if value < 0 {
        out.extend_from_slice(b"-");
    }
    write_u64(out, value.unsigned_abs());
    Ok(())
}

fn write_bare_flag(out: &mut BytesMut, flag: u8) {
    out.extend_from_slice(&[b' ', flag]);
}

fn write_u64(out: &mut BytesMut, mut value: u64) {
    let mut digits = [0; 20];
    let mut start = digits.len();
    loop {
        start -= 1;
        digits[start] = b'0' + (value % 10) as u8;
        value /= 10;
        if value == 0 {
            break;
        }
    }
    out.extend_from_slice(&digits[start..]);
}

fn finish_line(out: &mut BytesMut, line_start: usize) -> Result<(), MetaReplyEncodeError> {
    if out.len() - line_start + 2 > MAX_REPLY_LINE_BYTES {
        return Err(MetaReplyEncodeError::FrameTooLarge {
            maximum: MAX_REPLY_LINE_BYTES,
        });
    }
    out.extend_from_slice(b"\r\n");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        meta::{DecodedMetaCommand, MetaReplyDecoder, MetaRequestDecoder, MetaRequestEncoder},
        reply::{DebugHit, DebugReply, StoreResult},
    };

    fn plan(input: &[u8]) -> MetaReplyPlan {
        let mut decoder = MetaRequestDecoder::new();
        let mut src = BytesMut::from(input);
        let DecodedMetaCommand::Request { reply_plan, .. } =
            decoder.decode(&mut src).unwrap().unwrap()
        else {
            panic!("expected request");
        };
        reply_plan
    }

    fn encode(
        reply: &Reply,
        plan: &MetaReplyPlan,
    ) -> Result<(ReplyEncodeStatus, BytesMut), MetaReplyEncodeError> {
        let mut out = BytesMut::new();
        let status = MetaReplyEncoder::new().encode(reply, plan, &mut out)?;
        Ok((status, out))
    }

    #[test]
    fn encodes_header_hit_in_frontend_order() {
        let plan = plan(b"mg key Otag s t c f h l k\r\n");
        let reply = Reply::Get(GetReply::Hit(GetHit {
            value: None,
            client_flags: Some(7),
            cas: Some(42),
            size: Some(3),
            ttl: Some(-1),
            hit_before: Some(true),
            last_access_seconds: Some(9),
            recache: RecacheState::Won,
            stale: true,
        }));

        assert_eq!(
            encode(&reply, &plan).unwrap(),
            (
                ReplyEncodeStatus::Written,
                BytesMut::from(&b"HD Otag s3 t-1 c42 f7 h1 l9 kkey X W\r\n"[..]),
            )
        );
    }

    #[test]
    fn encodes_value_hit_and_body() {
        let plan = plan(b"mg key v c\r\n");
        let reply = Reply::Get(GetReply::Hit(GetHit {
            value: Some(Bytes::from_static(b"foo")),
            cas: Some(42),
            ..GetHit::default()
        }));

        assert_eq!(
            encode(&reply, &plan).unwrap(),
            (
                ReplyEncodeStatus::Written,
                BytesMut::from(&b"VA 3 c42\r\nfoo\r\n"[..]),
            )
        );
    }

    #[test]
    fn encodes_store_reply_in_frontend_order() {
        let plan = plan(b"ms key 3 Otag s c k\r\nfoo\r\n");
        let reply = Reply::Store(StoreReply::Success(StoreResult {
            cas: Some(42),
            size: Some(3),
        }));

        assert_eq!(
            encode(&reply, &plan).unwrap(),
            (
                ReplyEncodeStatus::Written,
                BytesMut::from(&b"HD Otag s3 c42 kkey\r\n"[..]),
            )
        );
    }

    #[test]
    fn suppresses_only_successful_quiet_store_reply() {
        let plan = plan(b"ms key 3 q Otag\r\nfoo\r\n");
        let success = Reply::Store(StoreReply::Success(StoreResult::default()));
        assert_eq!(
            encode(&success, &plan).unwrap(),
            (ReplyEncodeStatus::Suppressed, BytesMut::new())
        );

        let failure = Reply::Store(StoreReply::NotStored(StoreResult::default()));
        assert_eq!(
            encode(&failure, &plan).unwrap(),
            (
                ReplyEncodeStatus::Written,
                BytesMut::from(&b"NS Otag\r\n"[..]),
            )
        );
    }

    #[test]
    fn rejects_store_reply_missing_projected_fields_atomically() {
        let plan = plan(b"ms key 3 c s\r\nfoo\r\n");
        let reply = Reply::Store(StoreReply::Success(StoreResult::default()));
        let mut out = BytesMut::from(&b"existing"[..]);

        assert_eq!(
            MetaReplyEncoder::new().encode(&reply, &plan, &mut out),
            Err(MetaReplyEncodeError::MissingField("CAS"))
        );
        assert_eq!(out, b"existing".as_slice());
    }

    #[test]
    fn suppresses_quiet_get_miss_without_touching_output() {
        let plan = plan(b"mg key q Otag\r\n");
        let mut out = BytesMut::from(&b"existing"[..]);

        assert_eq!(
            MetaReplyEncoder::new().encode(&Reply::Get(GetReply::Miss), &plan, &mut out),
            Ok(ReplyEncodeStatus::Suppressed)
        );
        assert_eq!(out, b"existing".as_slice());
    }

    #[test]
    fn get_miss_restores_only_local_tokens() {
        let plan = plan(b"mg key c Otag s k\r\n");

        assert_eq!(
            encode(&Reply::Get(GetReply::Miss), &plan).unwrap(),
            (
                ReplyEncodeStatus::Written,
                BytesMut::from(&b"EN Otag kkey\r\n"[..]),
            )
        );
    }

    #[test]
    fn returns_base64_external_key_with_marker() {
        let plan = plan(b"mg a2V5 b k\r\n");

        assert_eq!(
            encode(&Reply::Get(GetReply::Miss), &plan).unwrap(),
            (
                ReplyEncodeStatus::Written,
                BytesMut::from(&b"EN ka2V5 b\r\n"[..]),
            )
        );
    }

    #[test]
    fn encodes_standard_errors_without_frontend_tokens() {
        let plan = plan(b"mg key Otag k\r\n");

        assert_eq!(
            encode(
                &Reply::Error(ErrorReply::Client(Some(Bytes::from_static(b"bad command")))),
                &plan,
            )
            .unwrap(),
            (
                ReplyEncodeStatus::Written,
                BytesMut::from(&b"CLIENT_ERROR bad command\r\n"[..]),
            )
        );
    }

    #[test]
    fn rejects_missing_projected_fields_atomically() {
        let plan = plan(b"mg key c\r\n");
        let reply = Reply::Get(GetReply::Hit(GetHit::default()));
        let mut out = BytesMut::from(&b"existing"[..]);

        assert_eq!(
            MetaReplyEncoder::new().encode(&reply, &plan, &mut out),
            Err(MetaReplyEncodeError::MissingField("CAS"))
        );
        assert_eq!(out, b"existing".as_slice());
    }

    #[test]
    fn rejects_value_size_mismatch_atomically() {
        let plan = plan(b"mg key v s\r\n");
        let reply = Reply::Get(GetReply::Hit(GetHit {
            value: Some(Bytes::from_static(b"foo")),
            size: Some(4),
            ..GetHit::default()
        }));
        let mut out = BytesMut::from(&b"existing"[..]);

        assert_eq!(
            MetaReplyEncoder::new().encode(&reply, &plan, &mut out),
            Err(MetaReplyEncodeError::SizeMismatch)
        );
        assert_eq!(out, b"existing".as_slice());
    }

    #[test]
    fn rejects_unimplemented_reply_operations() {
        let reply = Reply::Debug(DebugReply::Hit(DebugHit { fields: vec![] }));

        assert_eq!(
            encode(&reply, &MetaReplyPlan::default()),
            Err(MetaReplyEncodeError::UnsupportedReply)
        );
    }

    #[test]
    fn encodes_noop() {
        let mut out = BytesMut::new();
        MetaReplyEncoder::new().encode_noop(&mut out);

        assert_eq!(out, b"MN\r\n".as_slice());
    }

    #[test]
    fn completes_get_vertical_slice() {
        let mut request_decoder = MetaRequestDecoder::new();
        let mut frontend_input = BytesMut::from(&b"mg /region/cluster/key Otag c v\r\n"[..]);
        let DecodedMetaCommand::Request {
            request,
            reply_plan,
        } = request_decoder
            .decode(&mut frontend_input)
            .unwrap()
            .unwrap()
        else {
            panic!("expected request");
        };

        let mut backend_output = BytesMut::new();
        let expectation = MetaRequestEncoder::new()
            .encode(&request, &mut backend_output)
            .unwrap();
        assert_eq!(backend_output, b"mg key v c\r\n".as_slice());

        let mut reply_decoder = MetaReplyDecoder::new();
        let mut backend_input = BytesMut::from(&b"VA 3 c42\r\nfoo\r\n"[..]);
        let reply = reply_decoder
            .decode(&expectation, &mut backend_input)
            .unwrap()
            .unwrap();

        let mut frontend_output = BytesMut::new();
        assert_eq!(
            MetaReplyEncoder::new().encode(&reply, &reply_plan, &mut frontend_output),
            Ok(ReplyEncodeStatus::Written)
        );
        assert_eq!(frontend_output, b"VA 3 Otag c42\r\nfoo\r\n".as_slice());
    }

    #[test]
    fn completes_store_vertical_slice() {
        let mut request_decoder = MetaRequestDecoder::new();
        let mut frontend_input =
            BytesMut::from(&b"ms /region/cluster/key 3 Otag s c k\r\nfoo\r\n"[..]);
        let DecodedMetaCommand::Request {
            request,
            reply_plan,
        } = request_decoder
            .decode(&mut frontend_input)
            .unwrap()
            .unwrap()
        else {
            panic!("expected request");
        };

        let mut backend_output = BytesMut::new();
        let expectation = MetaRequestEncoder::new()
            .encode(&request, &mut backend_output)
            .unwrap();
        assert_eq!(backend_output, b"ms key 3 c s\r\nfoo\r\n".as_slice());

        let mut reply_decoder = MetaReplyDecoder::new();
        let mut backend_input = BytesMut::from(&b"HD c42 s3\r\n"[..]);
        let reply = reply_decoder
            .decode(&expectation, &mut backend_input)
            .unwrap()
            .unwrap();

        let mut frontend_output = BytesMut::new();
        assert_eq!(
            MetaReplyEncoder::new().encode(&reply, &reply_plan, &mut frontend_output),
            Ok(ReplyEncodeStatus::Written)
        );
        assert_eq!(
            frontend_output,
            b"HD Otag s3 c42 k/region/cluster/key\r\n".as_slice()
        );
    }
}
