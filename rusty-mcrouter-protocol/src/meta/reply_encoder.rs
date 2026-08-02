use bytes::{Bytes, BytesMut};
use thiserror::Error;

use crate::{
    key::MAX_KEY_BYTES,
    reply::{ArithmeticReply, DeleteReply, ErrorReply, GetReply, Reply, StoreReply},
};

use super::{command, wire, KeyEncoding, MetaQuietPolicy, MetaReplyPlan, MAX_REPLY_LINE_BYTES};

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
                | (
                    Reply::Delete(DeleteReply::Success),
                    MetaQuietPolicy::SuppressSuccess
                )
                | (
                    Reply::Arithmetic(ArithmeticReply::Success(_)),
                    MetaQuietPolicy::SuppressSuccess
                )
        ) {
            return Ok(ReplyEncodeStatus::Suppressed);
        }

        let checkpoint = out.len();
        let result = match reply {
            Reply::Get(reply) => command::get::encode_reply(reply, plan, out),
            Reply::Store(reply) => command::store::encode_reply(reply, plan, out),
            Reply::Delete(reply) => command::delete::encode_reply(reply, plan, out),
            Reply::Arithmetic(reply) => command::arithmetic::encode_reply(reply, plan, out),
            Reply::Debug(reply) => command::debug::encode_reply(reply, plan, out),
            Reply::Error(reply) => encode_error(reply, out),
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
    if opaque.is_empty() || opaque.len() > super::MAX_OPAQUE_BYTES {
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
            let mut scratch = [0; wire::MAX_BASE64_KEY_BYTES];
            let encoded = wire::encode_base64_key(key, &mut scratch).map_err(|_| {
                MetaReplyEncodeError::EncodedKeyTooLong {
                    maximum: MAX_KEY_BYTES,
                }
            })?;
            out.extend_from_slice(encoded);
            wire::write_bare_flag(out, b'b');
        }
    }
    Ok(())
}

/// Writes one ` <flag><value>` reply token. A `required` projection with no
/// value is the reply/plan mismatch this encoder exists to catch; optional
/// fields (arithmetic failure codes) are simply omitted.
pub fn write_field<T: wire::WireInt>(
    out: &mut BytesMut,
    flag: u8,
    value: Option<T>,
    name: &'static str,
    required: bool,
) -> Result<(), MetaReplyEncodeError> {
    match value {
        Some(value) => {
            wire::write_bare_flag(out, flag);
            value.write(out);
            Ok(())
        }
        None if required => Err(MetaReplyEncodeError::MissingField(name)),
        None => Ok(()),
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
    use crate::{
        meta::{DecodedMetaCommand, MetaReplyDecoder, MetaRequestDecoder, MetaRequestEncoder},
        reply::{
            ArithmeticResult, DebugField, DebugHit, DebugReply, GetHit, RecacheState, StoreResult,
        },
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
    fn encodes_all_delete_outcomes_with_local_tokens() {
        let plan = plan(b"md key Otag k\r\n");
        for (reply, expected) in [
            (DeleteReply::Success, b"HD Otag kkey\r\n".as_slice()),
            (DeleteReply::NotStored, b"NS Otag kkey\r\n".as_slice()),
            (DeleteReply::Exists, b"EX Otag kkey\r\n".as_slice()),
            (DeleteReply::NotFound, b"NF Otag kkey\r\n".as_slice()),
        ] {
            assert_eq!(
                encode(&Reply::Delete(reply), &plan).unwrap(),
                (ReplyEncodeStatus::Written, BytesMut::from(expected))
            );
        }
    }

    #[test]
    fn suppresses_only_successful_quiet_delete_reply() {
        let plan = plan(b"md key q Otag\r\n");
        assert_eq!(
            encode(&Reply::Delete(DeleteReply::Success), &plan).unwrap(),
            (ReplyEncodeStatus::Suppressed, BytesMut::new())
        );
        assert_eq!(
            encode(&Reply::Delete(DeleteReply::NotFound), &plan).unwrap(),
            (
                ReplyEncodeStatus::Written,
                BytesMut::from(&b"NF Otag\r\n"[..]),
            )
        );
    }

    #[test]
    fn encodes_arithmetic_header_and_value_success() {
        let header_plan = plan(b"ma key Otag t c k\r\n");
        let header = Reply::Arithmetic(ArithmeticReply::Success(ArithmeticResult {
            value: None,
            cas: Some(42),
            ttl: Some(-1),
        }));
        assert_eq!(
            encode(&header, &header_plan).unwrap(),
            (
                ReplyEncodeStatus::Written,
                BytesMut::from(&b"HD Otag t-1 c42 kkey\r\n"[..]),
            )
        );

        let value_plan = plan(b"ma key v c\r\n");
        let value = Reply::Arithmetic(ArithmeticReply::Success(ArithmeticResult {
            value: Some(u64::MAX),
            cas: Some(43),
            ttl: None,
        }));
        assert_eq!(
            encode(&value, &value_plan).unwrap(),
            (
                ReplyEncodeStatus::Written,
                BytesMut::from(&b"VA 20 c43\r\n18446744073709551615\r\n"[..]),
            )
        );
    }

    #[test]
    fn arithmetic_failures_restore_local_and_available_tokens() {
        let plan = plan(b"ma key Otag t c k\r\n");
        let reply = Reply::Arithmetic(ArithmeticReply::Exists(ArithmeticResult {
            value: None,
            cas: Some(41),
            ttl: None,
        }));

        assert_eq!(
            encode(&reply, &plan).unwrap(),
            (
                ReplyEncodeStatus::Written,
                BytesMut::from(&b"EX Otag c41 kkey\r\n"[..]),
            )
        );
    }

    #[test]
    fn suppresses_only_successful_quiet_arithmetic_reply() {
        let plan = plan(b"ma key q Otag\r\n");
        let success = Reply::Arithmetic(ArithmeticReply::Success(ArithmeticResult::default()));
        assert_eq!(
            encode(&success, &plan).unwrap(),
            (ReplyEncodeStatus::Suppressed, BytesMut::new())
        );

        let failure = Reply::Arithmetic(ArithmeticReply::NotFound(ArithmeticResult::default()));
        assert_eq!(
            encode(&failure, &plan).unwrap(),
            (
                ReplyEncodeStatus::Written,
                BytesMut::from(&b"NF Otag\r\n"[..]),
            )
        );
    }

    #[test]
    fn rejects_arithmetic_success_missing_projected_fields_atomically() {
        let plan = plan(b"ma key c t\r\n");
        let reply = Reply::Arithmetic(ArithmeticReply::Success(ArithmeticResult::default()));
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
    fn encodes_debug_hit_and_miss() {
        let plan = plan(b"me key\r\n");
        let hit = Reply::Debug(DebugReply::Hit(DebugHit {
            fields: vec![
                DebugField {
                    name: Bytes::from_static(b"exp"),
                    value: Bytes::from_static(b"60"),
                },
                DebugField {
                    name: Bytes::from_static(b"fetch"),
                    value: Bytes::from_static(b"yes"),
                },
            ],
        }));
        assert_eq!(
            encode(&hit, &plan).unwrap(),
            (
                ReplyEncodeStatus::Written,
                BytesMut::from(&b"ME key exp=60 fetch=yes\r\n"[..]),
            )
        );
        assert_eq!(
            encode(&Reply::Debug(DebugReply::Miss), &plan).unwrap(),
            (ReplyEncodeStatus::Written, BytesMut::from(&b"EN\r\n"[..]),)
        );
    }

    #[test]
    fn encodes_base64_debug_key_without_marker() {
        let plan = plan(b"me a2V5 b\r\n");
        let reply = Reply::Debug(DebugReply::Hit(DebugHit { fields: vec![] }));

        assert_eq!(
            encode(&reply, &plan).unwrap(),
            (
                ReplyEncodeStatus::Written,
                BytesMut::from(&b"ME a2V5\r\n"[..]),
            )
        );
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

    #[test]
    fn completes_delete_vertical_slice() {
        let mut request_decoder = MetaRequestDecoder::new();
        let mut frontend_input = BytesMut::from(&b"md /region/cluster/key Otag k C42\r\n"[..]);
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
        assert_eq!(backend_output, b"md key C42\r\n".as_slice());

        let mut reply_decoder = MetaReplyDecoder::new();
        let mut backend_input = BytesMut::from(&b"HD\r\n"[..]);
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
            b"HD Otag k/region/cluster/key\r\n".as_slice()
        );
    }

    #[test]
    fn completes_arithmetic_vertical_slice() {
        let mut request_decoder = MetaRequestDecoder::new();
        let mut frontend_input = BytesMut::from(&b"ma /region/cluster/key Otag t c v k D2\r\n"[..]);
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
        assert_eq!(backend_output, b"ma key v c D2 t\r\n".as_slice());

        let mut reply_decoder = MetaReplyDecoder::new();
        let mut backend_input = BytesMut::from(&b"VA 2 c42 t60\r\n43\r\n"[..]);
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
            b"VA 2 Otag t60 c42 k/region/cluster/key\r\n43\r\n".as_slice()
        );
    }

    #[test]
    fn completes_debug_vertical_slice() {
        let mut request_decoder = MetaRequestDecoder::new();
        let mut frontend_input = BytesMut::from(&b"me /region/cluster/key Pproxy\r\n"[..]);
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
        assert_eq!(backend_output, b"me key\r\n".as_slice());

        let mut reply_decoder = MetaReplyDecoder::new();
        let mut backend_input = BytesMut::from(&b"ME key exp=60 fetch=yes\r\n"[..]);
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
            b"ME /region/cluster/key exp=60 fetch=yes\r\n".as_slice()
        );
    }
}
