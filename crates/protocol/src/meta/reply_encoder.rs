use bytes::{Bytes, BytesMut};
use thiserror::Error;

use crate::key::MAX_KEY_BYTES;
use crate::meta::reply_decoder::MAX_REPLY_LINE_BYTES;
use crate::meta::request_decoder::MAX_OPAQUE_BYTES;
use crate::meta::{command, wire, KeyEncoding, MetaQuietPolicy, MetaReplyPlan};
use crate::reply::{ArithmeticReply, DeleteReply, ErrorReply, GetReply, StoreReply};
use crate::Reply;

#[derive(Debug, Default)]
pub struct MetaReplyEncoder;

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum MetaReplyEncodeError {
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

impl MetaReplyEncoder {
    pub const fn new() -> Self {
        Self
    }

    pub fn encode(
        &self,
        reply: &Reply,
        plan: &MetaReplyPlan,
        out: &mut BytesMut,
    ) -> Result<(), MetaReplyEncodeError> {
        if matches!(
            (reply, plan.quiet),
            (Reply::Get(GetReply::Miss), MetaQuietPolicy::SuppressMiss)
                | (
                    Reply::Store(StoreReply::Success(_)),
                    MetaQuietPolicy::SuppressSuccess
                )
                | (
                    Reply::Delete(DeleteReply::Success),
                    MetaQuietPolicy::SuppressSuccess
                )
                | (
                    Reply::Arithmetic(ArithmeticReply::Success(_)),
                    MetaQuietPolicy::SuppressSuccess
                )
        ) {
            return Ok(());
        }

        let checkpoint = out.len();
        let result = match reply {
            Reply::Get(reply) => command::get::encode_reply(reply, plan, out),
            Reply::Store(reply) => command::store::encode_reply(reply, plan, out),
            Reply::Delete(reply) => command::delete::encode_reply(reply, plan, out),
            Reply::Arithmetic(reply) => command::arithmetic::encode_reply(reply, plan, out),
            Reply::Debug(reply) => command::debug::encode_reply(reply, plan, out),
            Reply::Error(reply) => encode_error(reply, out),
            Reply::Version(_) => Err(MetaReplyEncodeError::InvalidData(
                "version reply is backend only",
            )),
        };
        if result.is_err() {
            out.truncate(checkpoint);
        }
        result
    }

    pub fn encode_noop(&self, out: &mut BytesMut) {
        out.extend_from_slice(b"MN\r\n");
    }
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
    wire::finish_line(out, line_start, MAX_REPLY_LINE_BYTES).map_err(reply_line_too_long)
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

pub fn write_opaque(plan: &MetaReplyPlan, out: &mut BytesMut) -> Result<(), MetaReplyEncodeError> {
    let opaque = plan
        .opaque
        .as_ref()
        .ok_or(MetaReplyEncodeError::MissingField("opaque token"))?;
    if opaque.is_empty() || opaque.len() > MAX_OPAQUE_BYTES {
        return Err(MetaReplyEncodeError::InvalidData("invalid opaque token"));
    }
    wire::write_bare_flag(out, b'O');
    out.extend_from_slice(opaque);
    Ok(())
}

/// Writes the client-facing ` k<key>` token (plus the `b` marker for a
/// base64-encoded reply key) from the frontend's reply plan.
pub fn write_key_token(
    plan: &MetaReplyPlan,
    out: &mut BytesMut,
) -> Result<(), MetaReplyEncodeError> {
    let key = plan
        .external_key
        .as_ref()
        .ok_or(MetaReplyEncodeError::MissingField("external key"))?;
    if key.is_empty() {
        return Err(MetaReplyEncodeError::InvalidData("empty external key"));
    }

    wire::write_bare_flag(out, b'k');
    match plan.key_encoding {
        KeyEncoding::Text => out.extend_from_slice(key),
        KeyEncoding::Base64 => {
            wire::write_base64_key(out, key).map_err(encoded_key_too_long)?;
            wire::write_bare_flag(out, b'b');
        }
    }
    Ok(())
}

/// Writes one ` <flag><value>` reply token. A `required` projection with no
/// value is the reply/plan mismatch this encoder exists to catch; optional
/// fields (arithmetic failure codes) are simply omitted.
pub fn write_field(
    out: &mut BytesMut,
    flag: u8,
    value: Option<u64>,
    name: &'static str,
    required: bool,
) -> Result<(), MetaReplyEncodeError> {
    match value {
        Some(value) => {
            wire::write_bare_flag(out, flag);
            wire::write_u64(out, value);
            Ok(())
        }
        None if required => Err(MetaReplyEncodeError::MissingField(name)),
        None => Ok(()),
    }
}

/// [`write_field`] for the one signed reply token, `t<ttl>`.
pub fn write_i64_field(
    out: &mut BytesMut,
    flag: u8,
    value: Option<i64>,
    name: &'static str,
    required: bool,
) -> Result<(), MetaReplyEncodeError> {
    match value {
        Some(value) => {
            wire::write_bare_flag(out, flag);
            wire::write_i64(out, value);
            Ok(())
        }
        None if required => Err(MetaReplyEncodeError::MissingField(name)),
        None => Ok(()),
    }
}

pub fn encoded_key_too_long(_: wire::EncodedKeyTooLong) -> MetaReplyEncodeError {
    MetaReplyEncodeError::EncodedKeyTooLong {
        maximum: MAX_KEY_BYTES,
    }
}

pub fn reply_line_too_long(error: wire::LineTooLong) -> MetaReplyEncodeError {
    MetaReplyEncodeError::FrameTooLarge {
        maximum: error.maximum,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reply::{DebugField, DebugHit, DebugReply, GetHit};
    use crate::test_support::{
        backend_request, get_miss, plan, reply, response, store_success, version,
    };

    #[test]
    fn encodes_header_hit_in_frontend_order() {
        let command = b"mg key Otag s t c f h l k\r\n";

        assert_eq!(
            response(command, b"HD s3 t-1 c42 f7 h1 l9 X W\r\n"),
            b"HD Otag s3 t-1 c42 f7 h1 l9 kkey X W\r\n".as_slice()
        );
    }

    #[test]
    fn encodes_value_hit_and_body() {
        let command = b"mg key v c\r\n";

        assert_eq!(
            response(command, b"VA 3 c42\r\nfoo\r\n"),
            b"VA 3 c42\r\nfoo\r\n".as_slice()
        );
    }

    #[test]
    fn encodes_store_reply_in_frontend_order() {
        let command = b"ms key 3 Otag s c k\r\nfoo\r\n";

        assert_eq!(
            response(command, b"HD c42 s3\r\n"),
            b"HD Otag s3 c42 kkey\r\n".as_slice()
        );
    }

    #[test]
    fn suppresses_only_successful_quiet_store_reply() {
        let command = b"ms key 3 q Otag\r\nfoo\r\n";
        assert_eq!(response(command, b"HD\r\n"), b"".as_slice());

        assert_eq!(response(command, b"NS\r\n"), b"NS Otag\r\n".as_slice());
    }

    #[test]
    fn rejects_store_reply_missing_projected_fields_atomically() {
        let plan = plan(b"ms key 3 c s\r\nfoo\r\n");
        let backend_reply = store_success();
        let mut out = BytesMut::from(&b"existing"[..]);

        assert_eq!(
            MetaReplyEncoder::new().encode(&backend_reply, &plan, &mut out),
            Err(MetaReplyEncodeError::MissingField("CAS"))
        );
        assert_eq!(out, b"existing".as_slice());
    }

    #[test]
    fn encodes_all_delete_outcomes_with_local_tokens() {
        let command = b"md key Otag k\r\n";
        for (backend, expected) in [
            (b"HD\r\n".as_slice(), b"HD Otag kkey\r\n".as_slice()),
            (b"NS\r\n".as_slice(), b"NS Otag kkey\r\n".as_slice()),
            (b"EX\r\n".as_slice(), b"EX Otag kkey\r\n".as_slice()),
            (b"NF\r\n".as_slice(), b"NF Otag kkey\r\n".as_slice()),
        ] {
            assert_eq!(response(command, backend), expected);
        }
    }

    #[test]
    fn suppresses_only_successful_quiet_delete_reply() {
        let command = b"md key q Otag\r\n";
        assert_eq!(response(command, b"HD\r\n"), b"".as_slice());
        assert_eq!(response(command, b"NF\r\n"), b"NF Otag\r\n".as_slice());
    }

    #[test]
    fn encodes_arithmetic_header_and_value_success() {
        let header_command = b"ma key Otag t c k\r\n";
        assert_eq!(
            response(header_command, b"HD c42 t-1\r\n"),
            b"HD Otag t-1 c42 kkey\r\n".as_slice()
        );

        let value_command = b"ma key v c\r\n";
        assert_eq!(
            response(value_command, b"VA 20 c43\r\n18446744073709551615\r\n",),
            b"VA 20 c43\r\n18446744073709551615\r\n".as_slice()
        );
    }

    #[test]
    fn arithmetic_failures_restore_local_and_available_tokens() {
        let command = b"ma key Otag t c k\r\n";

        assert_eq!(
            response(command, b"EX c41\r\n"),
            b"EX Otag c41 kkey\r\n".as_slice()
        );
    }

    #[test]
    fn suppresses_only_successful_quiet_arithmetic_reply() {
        let command = b"ma key q Otag\r\n";
        assert_eq!(response(command, b"HD\r\n"), b"".as_slice());

        assert_eq!(response(command, b"NF\r\n"), b"NF Otag\r\n".as_slice());
    }

    #[test]
    fn rejects_arithmetic_success_missing_projected_fields_atomically() {
        let plan = plan(b"ma key c t\r\n");
        let backend_reply = reply(b"ma fixture\r\n", b"HD\r\n");
        let mut out = BytesMut::from(&b"existing"[..]);

        assert_eq!(
            MetaReplyEncoder::new().encode(&backend_reply, &plan, &mut out),
            Err(MetaReplyEncodeError::MissingField("CAS"))
        );
        assert_eq!(out, b"existing".as_slice());
    }

    #[test]
    fn suppresses_quiet_get_miss_without_touching_output() {
        let plan = plan(b"mg key q Otag\r\n");
        let reply = get_miss();
        let mut out = BytesMut::from(&b"existing"[..]);

        assert_eq!(
            MetaReplyEncoder::new().encode(&reply, &plan, &mut out),
            Ok(())
        );
        assert_eq!(out, b"existing".as_slice());
    }

    #[test]
    fn get_miss_restores_only_local_tokens() {
        assert_eq!(
            response(b"mg key c Otag s k\r\n", b"EN\r\n"),
            b"EN Otag kkey\r\n".as_slice()
        );
    }

    #[test]
    fn returns_base64_external_key_with_marker() {
        assert_eq!(
            response(b"mg a2V5 b k\r\n", b"EN\r\n"),
            b"EN ka2V5 b\r\n".as_slice()
        );
    }

    #[test]
    fn encodes_standard_errors_without_frontend_tokens() {
        assert_eq!(
            response(b"mg key Otag k\r\n", b"CLIENT_ERROR bad command\r\n"),
            b"CLIENT_ERROR bad command\r\n".as_slice()
        );
    }

    #[test]
    fn rejects_backend_version_reply_atomically() {
        let plan = plan(b"mg key\r\n");
        let reply = version(b"1.6.39");
        let mut out = BytesMut::from(&b"existing"[..]);

        assert_eq!(
            MetaReplyEncoder::new().encode(&reply, &plan, &mut out),
            Err(MetaReplyEncodeError::InvalidData(
                "version reply is backend only"
            ))
        );
        assert_eq!(out, b"existing".as_slice());
    }

    #[test]
    fn rejects_missing_projected_fields_atomically() {
        let plan = plan(b"mg key c\r\n");
        let backend_reply = reply(b"mg fixture\r\n", b"HD\r\n");
        let mut out = BytesMut::from(&b"existing"[..]);

        assert_eq!(
            MetaReplyEncoder::new().encode(&backend_reply, &plan, &mut out),
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
    fn encodes_debug_hit_and_miss() {
        let command = b"me key\r\n";
        assert_eq!(
            response(command, b"ME key exp=60 fetch=yes\r\n"),
            b"ME key exp=60 fetch=yes\r\n".as_slice()
        );
        assert_eq!(response(command, b"EN\r\n"), b"EN\r\n".as_slice());
    }

    #[test]
    fn encodes_base64_debug_key_without_marker() {
        let command = b"me a2V5 b\r\n";

        assert_eq!(response(command, b"ME a2V5\r\n"), b"ME a2V5\r\n".as_slice());
    }

    #[test]
    fn rejects_invalid_debug_fields_atomically() {
        let plan = plan(b"me key\r\n");
        let reply = Reply::Debug(DebugReply::Hit(DebugHit {
            fields: vec![DebugField {
                name: Bytes::from_static(b"bad name"),
                value: Bytes::from_static(b"value"),
            }],
        }));
        let mut out = BytesMut::from(&b"existing"[..]);

        assert_eq!(
            MetaReplyEncoder::new().encode(&reply, &plan, &mut out),
            Err(MetaReplyEncodeError::InvalidData("invalid debug field"))
        );
        assert_eq!(out, b"existing".as_slice());
    }

    #[test]
    fn encodes_noop() {
        let mut out = BytesMut::new();
        MetaReplyEncoder::new().encode_noop(&mut out);
        assert_eq!(out, b"MN\r\n".as_slice());
    }

    #[test]
    fn completes_get_vertical_slice() {
        let command = b"mg /region/cluster/key Otag c v\r\n";
        assert_eq!(backend_request(command), b"mg key v c\r\n".as_slice());
        assert_eq!(
            response(command, b"VA 3 c42\r\nfoo\r\n"),
            b"VA 3 Otag c42\r\nfoo\r\n".as_slice()
        );
    }

    #[test]
    fn completes_store_vertical_slice() {
        let command = b"ms /region/cluster/key 3 Otag s c k\r\nfoo\r\n";
        assert_eq!(
            backend_request(command),
            b"ms key 3 c s\r\nfoo\r\n".as_slice()
        );
        assert_eq!(
            response(command, b"HD c42 s3\r\n"),
            b"HD Otag s3 c42 k/region/cluster/key\r\n".as_slice()
        )
    }

    #[test]
    fn completes_delete_vertical_slice() {
        let command = b"md /region/cluster/key Otag k C42\r\n";
        assert_eq!(backend_request(command), b"md key C42\r\n".as_slice());
        assert_eq!(
            response(command, b"HD\r\n"),
            b"HD Otag k/region/cluster/key\r\n".as_slice()
        )
    }

    #[test]
    fn completes_arithmetic_vertical_slice() {
        let command = b"ma /region/cluster/key Otag t c v k D2\r\n";
        assert_eq!(backend_request(command), b"ma key v c D2 t\r\n".as_slice());
        assert_eq!(
            response(command, b"VA 2 c42 t60\r\n43\r\n"),
            b"VA 2 Otag t60 c42 k/region/cluster/key\r\n43\r\n".as_slice()
        )
    }

    #[test]
    fn completes_debug_vertical_slice() {
        let command = b"me /region/cluster/key Pproxy\r\n";
        assert_eq!(backend_request(command), b"me key\r\n".as_slice());
        assert_eq!(
            response(command, b"ME key exp=60 fetch=yes\r\n"),
            b"ME /region/cluster/key exp=60 fetch=yes\r\n".as_slice()
        )
    }
}
