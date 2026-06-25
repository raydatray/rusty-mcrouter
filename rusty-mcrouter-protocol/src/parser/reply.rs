use bytes::BytesMut;

use crate::{
    reply::{Reply, Value},
    ProtocolError, Result,
};

use super::shared::{
    body_terminator_len, parse_u32, parse_u64, parse_usize, read_line, MAX_VALUE_SIZE,
};

pub fn parse_reply(buf: &mut BytesMut) -> Result<Option<Reply>> {
    let Some((line_end, total)) = read_line(buf, 0) else {
        return Ok(None);
    };

    match classify_first_line(&buf[..line_end]) {
        FirstLine::GetReply => parse_get_reply(buf),
        FirstLine::Simple(reply) => {
            let _ = buf.split_to(total);
            Ok(Some(reply))
        }
        FirstLine::ClientErrorMessage => {
            let frozen = buf.split_to(total).freeze();
            let msg = frozen.slice(b"CLIENT_ERROR ".len()..line_end);
            Ok(Some(Reply::ClientError(msg)))
        }
        FirstLine::ServerErrorMessage => {
            let frozen = buf.split_to(total).freeze();
            let msg = frozen.slice(b"SERVER_ERROR ".len()..line_end);
            Ok(Some(Reply::ServerError(msg)))
        }
        FirstLine::NumericLine => {
            let value = match parse_u64(&buf[..line_end]) {
                Ok(value) => value,
                Err(e) => {
                    let _ = buf.split_to(total);
                    return Err(e);
                }
            };
            let _ = buf.split_to(total);
            Ok(Some(Reply::Numeric(value)))
        }
    }
}

enum FirstLine {
    GetReply,
    Simple(Reply),
    ClientErrorMessage,
    ServerErrorMessage,
    NumericLine,
}

fn classify_first_line(line: &[u8]) -> FirstLine {
    match line {
        b"STORED" => FirstLine::Simple(Reply::Stored),
        b"NOT_STORED" => FirstLine::Simple(Reply::NotStored),
        b"EXISTS" => FirstLine::Simple(Reply::Exists),
        b"NOT_FOUND" => FirstLine::Simple(Reply::NotFound),
        b"ERROR" => FirstLine::Simple(Reply::Error),
        b"DELETED" => FirstLine::Simple(Reply::Deleted),
        b"TOUCHED" => FirstLine::Simple(Reply::Touched),
        _ if line.starts_with(b"CLIENT_ERROR ") => FirstLine::ClientErrorMessage,
        _ if line.starts_with(b"SERVER_ERROR ") => FirstLine::ServerErrorMessage,
        _ if !line.is_empty() && line.iter().all(|b| b.is_ascii_digit()) => FirstLine::NumericLine,
        _ => FirstLine::GetReply,
    }
}

struct ValueOffsets {
    key_start: usize,
    key_end: usize,
    flags: u32,
    data_start: usize,
    data_end: usize,
}

fn parse_get_reply(buf: &mut BytesMut) -> Result<Option<Reply>> {
    let mut cursor = 0;
    let mut blocks: Vec<ValueOffsets> = Vec::new();

    loop {
        let Some((line_end, line_total)) = read_line(buf, cursor) else {
            return Ok(None);
        };
        let line = &buf[cursor..line_end];

        if line == b"END" {
            let frozen = buf.split_to(line_total).freeze();
            let hits = blocks
                .into_iter()
                .map(|b| Value {
                    key: frozen.slice(b.key_start..b.key_end),
                    flags: b.flags,
                    data: frozen.slice(b.data_start..b.data_end),
                })
                .collect();
            return Ok(Some(Reply::Get { hits }));
        }

        if !line.starts_with(b"VALUE ") {
            let _ = buf.split_to(line_total);
            return Err(ProtocolError::Malformed("expected VALUE or END"));
        }

        let after_value = &line[6..];
        let mut parts = after_value.split(|&b| b == b' ');
        let Some(key) = parts.next().filter(|k| !k.is_empty()) else {
            let _ = buf.split_to(line_total);
            return Err(ProtocolError::Malformed("missing or empty key in VALUE"));
        };
        let Some(flags_bytes) = parts.next() else {
            let _ = buf.split_to(line_total);
            return Err(ProtocolError::Malformed("missing flags in VALUE"));
        };
        let Some(bytes_str) = parts.next() else {
            let _ = buf.split_to(line_total);
            return Err(ProtocolError::Malformed("missing byte count in VALUE"));
        };
        // Extra fields (e.g. CAS for `gets`) are accepted and silently ignored.

        let flags = match parse_u32(flags_bytes) {
            Ok(flags) => flags,
            Err(e) => {
                let _ = buf.split_to(line_total);
                return Err(e);
            }
        };
        let bytes_count = match parse_usize(bytes_str) {
            Ok(bytes_count) => bytes_count,
            Err(e) => {
                let _ = buf.split_to(line_total);
                return Err(e);
            }
        };
        if bytes_count > MAX_VALUE_SIZE {
            let _ = buf.split_to(line_total);
            return Err(ProtocolError::Malformed("value too large"));
        }

        let key_start = cursor + 6;
        let key_end = key_start + key.len();

        // Data is binary-counted, NOT line-terminated. Consume exactly
        // bytes_count bytes, then a CRLF or LF that does NOT count toward
        // bytes_count. Embedded \r, \n, NULs, or fake protocol keywords in
        // the payload must pass through untouched.
        let data_start = line_total;
        let data_end = match data_start.checked_add(bytes_count) {
            Some(data_end) => data_end,
            None => {
                let _ = buf.split_to(line_total);
                return Err(ProtocolError::Malformed("body length overflow"));
            }
        };
        let terminator_len = match body_terminator_len(buf, data_end) {
            Ok(Some(len)) => len,
            Ok(None) => return Ok(None),
            Err(e) => {
                let _ = buf.split_to(data_end + 1);
                return Err(e);
            }
        };

        blocks.push(ValueOffsets {
            key_start,
            key_end,
            flags,
            data_start,
            data_end,
        });
        cursor = data_end + terminator_len;
    }
}

#[cfg(test)]
mod tests {
    use bytes::{Bytes, BytesMut};

    use crate::{
        fixtures::assert_reply_round_trips,
        reply::{Reply, Value},
        ProtocolError, Result,
    };

    use super::parse_reply;

    fn pr(bytes: &[u8]) -> (Result<Option<Reply>>, BytesMut) {
        let mut buf = BytesMut::from(bytes);
        let result = parse_reply(&mut buf);
        (result, buf)
    }

    #[test]
    fn parse_reply_miss_returns_empty_hits() {
        let (result, buf) = pr(b"END\r\n");
        assert_eq!(result.unwrap().unwrap(), Reply::Get { hits: vec![] });
        assert!(buf.is_empty());
    }

    #[test]
    fn parse_reply_single_hit() {
        let (result, buf) = pr(b"VALUE foo 0 3\r\nbar\r\nEND\r\n");
        assert_eq!(
            result.unwrap().unwrap(),
            Reply::Get {
                hits: vec![Value {
                    key: Bytes::from_static(b"foo"),
                    flags: 0,
                    data: Bytes::from_static(b"bar"),
                }]
            }
        );
        assert!(buf.is_empty());
    }

    #[test]
    fn parse_reply_multiple_hits_with_distinct_flags() {
        let (result, buf) =
            pr(b"VALUE k1 0 1\r\na\r\nVALUE k2 5 2\r\nbc\r\nVALUE k3 99 4\r\nzzzz\r\nEND\r\n");
        let Reply::Get { hits } = result.unwrap().unwrap() else {
            panic!("expected Reply::Get");
        };
        assert_eq!(hits.len(), 3);
        assert_eq!(hits[0].key.as_ref(), b"k1");
        assert_eq!(hits[0].flags, 0);
        assert_eq!(hits[0].data.as_ref(), b"a");
        assert_eq!(hits[1].key.as_ref(), b"k2");
        assert_eq!(hits[1].flags, 5);
        assert_eq!(hits[1].data.as_ref(), b"bc");
        assert_eq!(hits[2].key.as_ref(), b"k3");
        assert_eq!(hits[2].flags, 99);
        assert_eq!(hits[2].data.as_ref(), b"zzzz");
        assert!(buf.is_empty());
    }

    #[test]
    fn parse_reply_empty_value_block() {
        let (result, buf) = pr(b"VALUE k 5 0\r\n\r\nEND\r\n");
        assert_eq!(
            result.unwrap().unwrap(),
            Reply::Get {
                hits: vec![Value {
                    key: Bytes::from_static(b"k"),
                    flags: 5,
                    data: Bytes::from_static(b""),
                }]
            }
        );
        assert!(buf.is_empty());
    }

    #[test]
    fn parse_reply_returns_none_on_partial_header() {
        let (result, buf) = pr(b"VALUE foo 0");
        assert!(matches!(result, Ok(None)));
        assert_eq!(buf.as_ref(), b"VALUE foo 0");
    }

    #[test]
    fn parse_reply_returns_none_on_partial_data_block() {
        let (result, buf) = pr(b"VALUE foo 0 5\r\nfoo");
        assert!(matches!(result, Ok(None)));
        assert_eq!(buf.as_ref(), b"VALUE foo 0 5\r\nfoo");
    }

    #[test]
    fn parse_reply_returns_none_on_partial_trailing_crlf() {
        let (result, buf) = pr(b"VALUE foo 0 3\r\nbar\r");
        assert!(matches!(result, Ok(None)));
        assert_eq!(buf.as_ref(), b"VALUE foo 0 3\r\nbar\r");
    }

    #[test]
    fn parse_reply_returns_none_when_end_missing() {
        let (result, buf) = pr(b"VALUE foo 0 3\r\nbar\r\n");
        assert!(matches!(result, Ok(None)));
        assert_eq!(buf.as_ref(), b"VALUE foo 0 3\r\nbar\r\n");
    }

    #[test]
    fn parse_reply_consumes_only_first_complete_reply() {
        let mut buf = BytesMut::from(&b"END\r\nEND\r\n"[..]);
        let first = parse_reply(&mut buf).unwrap().unwrap();
        assert_eq!(first, Reply::Get { hits: vec![] });
        assert_eq!(buf.as_ref(), b"END\r\n");

        let second = parse_reply(&mut buf).unwrap().unwrap();
        assert_eq!(second, Reply::Get { hits: vec![] });
        assert!(buf.is_empty());
    }

    #[test]
    fn parse_reply_accepts_lf_only_line_terminator() {
        // Match parse_request's lenient behavior: accept LF or CRLF terminators.
        let (result, buf) = pr(b"VALUE foo 0 3\nbar\nEND\n");
        let Reply::Get { hits } = result.unwrap().unwrap() else {
            panic!("expected Reply::Get");
        };
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].key.as_ref(), b"foo");
        assert_eq!(hits[0].data.as_ref(), b"bar");
        assert!(buf.is_empty());
    }

    #[test]
    fn parse_reply_data_block_is_binary_safe() {
        // Payload deliberately contains NULs, embedded CRLFs, and fake
        // VALUE/END keywords. The parser must trust the byte count and pass
        // these bytes through unaltered, never mistaking them for framing.
        let payload: &[u8] = b"\x00\r\nVALUE fake 0 0\r\nEND\r\nbinary\xff";
        let mut wire = BytesMut::new();
        wire.extend_from_slice(b"VALUE k 0 ");
        wire.extend_from_slice(payload.len().to_string().as_bytes());
        wire.extend_from_slice(b"\r\n");
        wire.extend_from_slice(payload);
        wire.extend_from_slice(b"\r\nEND\r\n");

        let mut buf = wire;
        let Reply::Get { hits } = parse_reply(&mut buf).unwrap().unwrap() else {
            panic!("expected Reply::Get");
        };
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].data.as_ref(), payload);
        assert!(buf.is_empty());
    }

    #[test]
    fn parse_reply_rejects_unknown_line() {
        let (result, buf) = pr(b"FOO\r\n");
        assert!(matches!(
            result,
            Err(ProtocolError::Malformed("expected VALUE or END"))
        ));
        assert!(buf.is_empty());
    }

    #[test]
    fn parse_reply_error_consumes_offending_bytes() {
        let mut buf = BytesMut::from(&b"VALUE foo 0 3\r\nbarXX\r\nEND\r\n"[..]);
        assert!(matches!(
            parse_reply(&mut buf),
            Err(ProtocolError::Malformed("missing CRLF after body"))
        ));
        assert_eq!(buf.as_ref(), b"X\r\nEND\r\n");
    }

    #[test]
    fn parse_reply_returns_error_replies_as_first_class_variants() {
        let cases: &[(&[u8], Reply)] = &[
            (b"ERROR\r\n", Reply::Error),
            (
                b"CLIENT_ERROR oops\r\n",
                Reply::ClientError(Bytes::from_static(b"oops")),
            ),
            (
                b"SERVER_ERROR boom\r\n",
                Reply::ServerError(Bytes::from_static(b"boom")),
            ),
        ];
        cases.iter().for_each(|(input, expected)| {
            let mut buf = BytesMut::from(*input);
            assert_eq!(parse_reply(&mut buf).unwrap().unwrap(), *expected);
            assert!(buf.is_empty(), "input fully consumed for {expected:?}");
        });
    }

    #[test]
    fn parse_reply_storage_acks_parse_to_correct_variants() {
        let cases: &[(&[u8], Reply)] = &[
            (b"STORED\r\n", Reply::Stored),
            (b"NOT_STORED\r\n", Reply::NotStored),
            (b"EXISTS\r\n", Reply::Exists),
            (b"NOT_FOUND\r\n", Reply::NotFound),
            (b"DELETED\r\n", Reply::Deleted),
            (b"TOUCHED\r\n", Reply::Touched),
        ];
        cases.iter().for_each(|(input, expected)| {
            let mut buf = BytesMut::from(*input);
            assert_eq!(parse_reply(&mut buf).unwrap().unwrap(), *expected);
            assert!(buf.is_empty(), "input fully consumed for {expected:?}");
        });
    }

    #[test]
    fn parse_reply_storage_acks_accept_lf_only_terminator() {
        let mut buf = BytesMut::from(&b"STORED\n"[..]);
        assert_eq!(parse_reply(&mut buf).unwrap().unwrap(), Reply::Stored);
        assert!(buf.is_empty());
    }

    #[test]
    fn parse_reply_error_message_with_empty_body() {
        let mut buf = BytesMut::from(&b"CLIENT_ERROR \r\n"[..]);
        assert_eq!(
            parse_reply(&mut buf).unwrap().unwrap(),
            Reply::ClientError(Bytes::from_static(b""))
        );
        assert!(buf.is_empty());
    }

    #[test]
    fn parse_reply_storage_acks_consume_only_first_complete_reply() {
        let mut buf = BytesMut::from(&b"STORED\r\nNOT_STORED\r\n"[..]);
        assert_eq!(parse_reply(&mut buf).unwrap().unwrap(), Reply::Stored);
        assert_eq!(buf.as_ref(), b"NOT_STORED\r\n");
        assert_eq!(parse_reply(&mut buf).unwrap().unwrap(), Reply::NotStored);
        assert!(buf.is_empty());
    }

    #[test]
    fn parse_reply_storage_ack_returns_none_on_partial_line() {
        let mut buf = BytesMut::from(&b"STOR"[..]);
        assert!(matches!(parse_reply(&mut buf), Ok(None)));
        assert_eq!(buf.as_ref(), b"STOR");
    }

    #[test]
    fn parse_reply_round_trips_storage_and_error_replies() {
        let replies = [
            Reply::Stored,
            Reply::NotStored,
            Reply::Exists,
            Reply::NotFound,
            Reply::Error,
            Reply::ClientError(Bytes::from_static(b"bad command")),
            Reply::ServerError(Bytes::from_static(b"out of memory")),
        ];
        replies.iter().for_each(|original| {
            let mut buf = BytesMut::new();
            original.serialize_into(&mut buf);
            let parsed = parse_reply(&mut buf).unwrap().unwrap();
            assert_eq!(parsed, *original);
            assert!(buf.is_empty());
        });
    }

    #[test]
    fn parse_reply_rejects_non_numeric_byte_count() {
        let (result, buf) = pr(b"VALUE foo 0 abc\r\nbar\r\nEND\r\n");
        assert!(matches!(
            result,
            Err(ProtocolError::Malformed("invalid usize"))
        ));
        assert_eq!(buf.as_ref(), b"bar\r\nEND\r\n");
    }

    #[test]
    fn parse_reply_rejects_too_large_declared_value_without_buffering() {
        let (result, buf) = pr(b"VALUE foo 0 1048577\r\n");
        assert!(matches!(
            result,
            Err(ProtocolError::Malformed("value too large"))
        ));
        assert!(buf.is_empty());
    }

    #[test]
    fn parse_reply_rejects_missing_crlf_after_data() {
        let (result, buf) = pr(b"VALUE foo 0 3\r\nbarXX\r\nEND\r\n");
        assert!(matches!(
            result,
            Err(ProtocolError::Malformed("missing CRLF after body"))
        ));
        assert_eq!(buf.as_ref(), b"X\r\nEND\r\n");
    }

    #[test]
    fn parse_reply_numeric_value() {
        let cases: &[(&[u8], u64)] = &[
            (b"0\r\n", 0),
            (b"1\r\n", 1),
            (b"12345\r\n", 12345),
            (b"18446744073709551615\r\n", u64::MAX),
        ];
        cases.iter().for_each(|(input, expected)| {
            let mut buf = BytesMut::from(*input);
            assert_eq!(
                parse_reply(&mut buf).unwrap().unwrap(),
                Reply::Numeric(*expected),
                "input={input:?}"
            );
            assert!(buf.is_empty(), "input fully consumed for {input:?}");
        });
    }

    #[test]
    fn parse_reply_numeric_accepts_lf_only_terminator() {
        let mut buf = BytesMut::from(&b"42\n"[..]);
        assert_eq!(parse_reply(&mut buf).unwrap().unwrap(), Reply::Numeric(42));
        assert!(buf.is_empty());
    }

    #[test]
    fn parse_reply_numeric_rejects_overflow() {
        let mut buf = BytesMut::from(&b"99999999999999999999999\r\n"[..]);
        assert!(matches!(
            parse_reply(&mut buf),
            Err(ProtocolError::Malformed("invalid u64"))
        ));
    }

    #[test]
    fn parse_reply_numeric_round_trips_with_serializer() {
        let values = [0u64, 1, 12345, u64::MAX];
        values.iter().for_each(|v| {
            assert_reply_round_trips(Reply::Numeric(*v));
        });
    }

    #[test]
    fn parse_reply_round_trips_with_serializer() {
        let original = Reply::Get {
            hits: vec![
                Value {
                    key: Bytes::from_static(b"alpha"),
                    flags: 0,
                    data: Bytes::from_static(b"hello"),
                },
                Value {
                    key: Bytes::from_static(b"beta"),
                    flags: 99,
                    data: Bytes::from_static(b""),
                },
                Value {
                    key: Bytes::from_static(b"gamma"),
                    flags: u32::MAX,
                    data: Bytes::from_static(b"\x00\x01\xff binary \r\n"),
                },
            ],
        };

        assert_reply_round_trips(original);
    }
}
