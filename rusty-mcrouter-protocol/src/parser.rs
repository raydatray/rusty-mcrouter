use bytes::{Bytes, BytesMut};

use crate::{
    error::ProtocolError,
    reply::{Reply, Value},
    request::Request,
};

// TODO: parser is stateless and re-parses headers on every partial-read call.
// mcrouter's McServerAsciiParser holds in-progress state across calls. Make
// this stateful via a RequestParser struct holding ParseState — low priority,
// costs ~µs + 1 small alloc per partial read on multi-fragment set bodies.
const MAX_KEY_LEN: usize = 250;
const SET_HEADER_HELP: &str = "set requires <key> <flags> <exptime> <bytes>";

pub fn parse_request(buf: &mut BytesMut) -> Result<Option<Request>, ProtocolError> {
    let eol_idx = match buf.iter().position(|&b| b == b'\n') {
        Some(i) => i,
        None => return Ok(None),
    };
    let line_text_end = if eol_idx > 0 && buf[eol_idx - 1] == b'\r' {
        eol_idx - 1
    } else {
        eol_idx
    };

    let cmd = command_name(&buf[..line_text_end]);

    match cmd {
        b"get" => parse_get_request(buf, eol_idx),
        b"set" => parse_set_request(buf, eol_idx, line_text_end),
        _ => {
            let _ = buf.split_to(eol_idx + 1);
            Err(ProtocolError::Malformed("unknown command"))
        }
    }
}

fn command_name(header: &[u8]) -> &[u8] {
    let end = header
        .iter()
        .position(|&b| b == b' ')
        .unwrap_or(header.len());
    &header[..end]
}

fn parse_get_request(buf: &mut BytesMut, eol_idx: usize) -> Result<Option<Request>, ProtocolError> {
    let mut line = buf.split_to(eol_idx + 1).freeze();
    if line.ends_with(b"\r\n") {
        line.truncate(line.len() - 2);
    } else {
        line.truncate(line.len() - 1);
    }

    let rest = match line.strip_prefix(b"get ") {
        Some(_) => line.slice(b"get ".len()..),
        None => return Err(ProtocolError::Malformed("missing arguments")),
    };
    parse_get(rest).map(Some)
}

fn parse_set_request(
    buf: &mut BytesMut,
    eol_idx: usize,
    line_text_end: usize,
) -> Result<Option<Request>, ProtocolError> {
    // Header parse is pure over a slice; the wrapper handles buf mutation
    // so we keep partial-frame reads idempotent.
    let (key, flags, exptime, bytes_count) = match parse_set_header(&buf[..line_text_end]) {
        Ok(p) => p,
        Err(e) => {
            let _ = buf.split_to(eol_idx + 1);
            return Err(e);
        }
    };

    // Body framing mirrors VALUE block parsing: bytes_count payload, then a
    // CRLF or LF that does NOT count toward bytes_count.
    let data_start = eol_idx + 1;
    let data_end = data_start + bytes_count;
    if buf.len() < data_end + 1 {
        return Ok(None);
    }
    let terminator_len = match buf[data_end] {
        b'\n' => 1,
        b'\r' => {
            if buf.len() < data_end + 2 {
                return Ok(None);
            }
            if buf[data_end + 1] != b'\n' {
                let _ = buf.split_to(data_end + 1);
                return Err(ProtocolError::Malformed(
                    "missing LF after CR in set body terminator",
                ));
            }
            2
        }
        _ => {
            let _ = buf.split_to(data_end + 1);
            return Err(ProtocolError::Malformed("missing CRLF after set body"));
        }
    };

    let total = data_end + terminator_len;
    let frozen = buf.split_to(total).freeze();
    let data = frozen.slice(data_start..data_end);
    Ok(Some(Request::Set {
        key,
        flags,
        exptime,
        data,
    }))
}

fn parse_set_header(header: &[u8]) -> Result<(Bytes, u32, i32, usize), ProtocolError> {
    let after_cmd = header
        .strip_prefix(b"set ")
        .ok_or(ProtocolError::Malformed("missing arguments"))?;

    let mut parts = after_cmd.split(|&b| b == b' ').filter(|s| !s.is_empty());
    let key = parts
        .next()
        .ok_or(ProtocolError::Malformed(SET_HEADER_HELP))?;
    let flags_bytes = parts
        .next()
        .ok_or(ProtocolError::Malformed(SET_HEADER_HELP))?;
    let exptime_bytes = parts
        .next()
        .ok_or(ProtocolError::Malformed(SET_HEADER_HELP))?;
    let bytes_bytes = parts
        .next()
        .ok_or(ProtocolError::Malformed(SET_HEADER_HELP))?;

    if let Some(extra) = parts.next() {
        return Err(if extra == b"noreply" {
            ProtocolError::Malformed("noreply not yet supported")
        } else {
            ProtocolError::Malformed("set: unexpected extra token in header")
        });
    }

    validate_key(key)?;
    let flags = parse_u32(flags_bytes)?;
    let exptime = parse_i32(exptime_bytes)?;
    let bytes_count = parse_usize(bytes_bytes)?;

    Ok((Bytes::copy_from_slice(key), flags, exptime, bytes_count))
}

pub fn parse_reply(buf: &mut BytesMut) -> Result<Option<Reply>, ProtocolError> {
    let line_end = match buf.iter().position(|&b| b == b'\n') {
        Some(i) => i,
        None => return Ok(None),
    };
    let line_text_end = if line_end > 0 && buf[line_end - 1] == b'\r' {
        line_end - 1
    } else {
        line_end
    };
    let total = line_end + 1;

    match classify_first_line(&buf[..line_text_end]) {
        FirstLine::GetReply => parse_get_reply(buf),
        FirstLine::Simple(reply) => {
            let _ = buf.split_to(total);
            Ok(Some(reply))
        }
        FirstLine::ClientErrorMessage => {
            let frozen = buf.split_to(total).freeze();
            let msg = frozen.slice(b"CLIENT_ERROR ".len()..line_text_end);
            Ok(Some(Reply::ClientError(msg)))
        }
        FirstLine::ServerErrorMessage => {
            let frozen = buf.split_to(total).freeze();
            let msg = frozen.slice(b"SERVER_ERROR ".len()..line_text_end);
            Ok(Some(Reply::ServerError(msg)))
        }
    }
}

enum FirstLine {
    GetReply,
    Simple(Reply),
    ClientErrorMessage,
    ServerErrorMessage,
}

fn classify_first_line(line: &[u8]) -> FirstLine {
    match line {
        b"STORED" => FirstLine::Simple(Reply::Stored),
        b"NOT_STORED" => FirstLine::Simple(Reply::NotStored),
        b"EXISTS" => FirstLine::Simple(Reply::Exists),
        b"NOT_FOUND" => FirstLine::Simple(Reply::NotFound),
        b"ERROR" => FirstLine::Simple(Reply::Error),
        _ if line.starts_with(b"CLIENT_ERROR ") => FirstLine::ClientErrorMessage,
        _ if line.starts_with(b"SERVER_ERROR ") => FirstLine::ServerErrorMessage,
        _ => FirstLine::GetReply,
    }
}

fn parse_get_reply(buf: &mut BytesMut) -> Result<Option<Reply>, ProtocolError> {
    let mut cursor = 0;
    let mut blocks: Vec<(usize, usize, u32, usize, usize)> = Vec::new();

    loop {
        let line_end = match buf[cursor..].iter().position(|&b| b == b'\n') {
            Some(i) => cursor + i,
            None => return Ok(None),
        };

        let line_text_end = if line_end > cursor && buf[line_end - 1] == b'\r' {
            line_end - 1
        } else {
            line_end
        };
        let line = &buf[cursor..line_text_end];

        if line == b"END" {
            let total = line_end + 1;
            let frozen = buf.split_to(total).freeze();
            let hits = blocks
                .into_iter()
                .map(|(ks, ke, flags, ds, de)| Value {
                    key: frozen.slice(ks..ke),
                    flags,
                    data: frozen.slice(ds..de),
                })
                .collect();
            return Ok(Some(Reply::Get { hits }));
        }

        if !line.starts_with(b"VALUE ") {
            return Err(ProtocolError::Malformed("expected VALUE or END"));
        }

        let after_value = &line[6..];
        let mut parts = after_value.split(|&b| b == b' ');
        let key = parts
            .next()
            .filter(|k| !k.is_empty())
            .ok_or(ProtocolError::Malformed("missing or empty key in VALUE"))?;
        let flags_bytes = parts
            .next()
            .ok_or(ProtocolError::Malformed("missing flags in VALUE"))?;
        let bytes_str = parts
            .next()
            .ok_or(ProtocolError::Malformed("missing byte count in VALUE"))?;
        // Extra fields (e.g. CAS for `gets`) are accepted and silently ignored.

        let flags = parse_u32(flags_bytes)?;
        let bytes_count = parse_usize(bytes_str)?;

        let key_start = cursor + 6;
        let key_end = key_start + key.len();

        // Data is binary-counted, NOT line-terminated. Consume exactly
        // bytes_count bytes, then a CRLF or LF that does NOT count toward
        // bytes_count. Embedded \r, \n, NULs, or fake protocol keywords in
        // the payload must pass through untouched.
        let data_start = line_end + 1;
        let data_end = data_start + bytes_count;
        if buf.len() < data_end + 1 {
            return Ok(None);
        }
        let terminator_len = match buf[data_end] {
            b'\n' => 1,
            b'\r' => {
                if buf.len() < data_end + 2 {
                    return Ok(None);
                }
                if buf[data_end + 1] != b'\n' {
                    return Err(ProtocolError::Malformed(
                        "missing LF after CR in value terminator",
                    ));
                }
                2
            }
            _ => {
                return Err(ProtocolError::Malformed(
                    "missing CRLF after value data block",
                ))
            }
        };

        blocks.push((key_start, key_end, flags, data_start, data_end));
        cursor = data_end + terminator_len;
    }
}

fn parse_get(rest: Bytes) -> Result<Request, ProtocolError> {
    let keys = rest
        .split(|&b| b == b' ')
        .filter(|seg| !seg.is_empty())
        .map(|seg| validate_key(seg).map(|()| rest.slice_ref(seg)))
        .collect::<Result<Vec<_>, _>>()?;

    if keys.is_empty() {
        return Err(ProtocolError::Malformed("get requires at least one key"));
    }

    Ok(Request::Get { keys })
}

fn validate_key(key: &[u8]) -> Result<(), ProtocolError> {
    if key.is_empty() {
        return Err(ProtocolError::InvalidKey);
    }

    if key.len() > MAX_KEY_LEN {
        return Err(ProtocolError::KeyTooLong(key.len()));
    }

    if key
        .iter()
        .any(|&b| b.is_ascii_whitespace() || b.is_ascii_control())
    {
        return Err(ProtocolError::InvalidKey);
    }

    Ok(())
}

fn parse_u32(s: &[u8]) -> Result<u32, ProtocolError> {
    std::str::from_utf8(s)
        .ok()
        .and_then(|s| s.parse::<u32>().ok())
        .ok_or(ProtocolError::Malformed("invalid u32"))
}

fn parse_i32(s: &[u8]) -> Result<i32, ProtocolError> {
    std::str::from_utf8(s)
        .ok()
        .and_then(|s| s.parse::<i32>().ok())
        .ok_or(ProtocolError::Malformed("invalid i32"))
}

fn parse_usize(s: &[u8]) -> Result<usize, ProtocolError> {
    std::str::from_utf8(s)
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .ok_or(ProtocolError::Malformed("invalid usize"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_request_returns_none_when_no_newline() {
        let mut empty = BytesMut::new();
        assert!(matches!(parse_request(&mut empty), Ok(None)));
        assert!(empty.is_empty());

        let mut partial = BytesMut::from(&b"get fo"[..]);
        assert!(matches!(parse_request(&mut partial), Ok(None)));
        assert_eq!(partial.as_ref(), b"get fo");
    }

    #[test]
    fn parse_request_strips_lf_and_crlf() {
        let cases: &[&[u8]] = &[b"get foo\n", b"get foo\r\n"];

        cases.iter().for_each(|input| {
            let mut buf = BytesMut::from(*input);
            let req = parse_request(&mut buf).unwrap().unwrap();
            assert_eq!(
                req,
                Request::Get {
                    keys: vec![Bytes::from_static(b"foo")]
                }
            );
            assert!(buf.is_empty());
        });
    }

    #[test]
    fn parse_request_consumes_one_frame_at_a_time() {
        let mut buf = BytesMut::from(&b"get foo\nget bar\n"[..]);

        let first = parse_request(&mut buf).unwrap().unwrap();
        assert_eq!(
            first,
            Request::Get {
                keys: vec![Bytes::from_static(b"foo")]
            }
        );
        assert_eq!(buf.as_ref(), b"get bar\n");

        let second = parse_request(&mut buf).unwrap().unwrap();
        assert_eq!(
            second,
            Request::Get {
                keys: vec![Bytes::from_static(b"bar")]
            }
        );
        assert!(buf.is_empty());

        assert!(matches!(parse_request(&mut buf), Ok(None)));
    }

    #[test]
    fn parse_request_propagates_errors_and_consumes_malformed_lines() {
        let mut unknown = BytesMut::from(&b"WAT foo\n"[..]);
        assert!(matches!(
            parse_request(&mut unknown),
            Err(ProtocolError::Malformed("unknown command"))
        ));
        assert!(unknown.is_empty());

        for terminator in [&b"\n"[..], &b"\r\n"[..]] {
            let mut buf = BytesMut::from(terminator);
            assert!(matches!(
                parse_request(&mut buf),
                Err(ProtocolError::Malformed("unknown command"))
            ));
            assert!(buf.is_empty());
        }
    }

    #[test]
    fn parse_request_rejects_get_without_args() {
        for input in [&b"get\n"[..], &b"get\r\n"[..]] {
            let mut buf = BytesMut::from(input);
            assert!(matches!(
                parse_request(&mut buf),
                Err(ProtocolError::Malformed("missing arguments"))
            ));
            assert!(buf.is_empty());
        }
    }

    #[test]
    fn parse_request_get_multiple_keys() {
        let mut buf = BytesMut::from(&b"get foo bar baz\r\n"[..]);
        let Request::Get { keys } = parse_request(&mut buf).unwrap().unwrap() else {
            panic!("expected Request::Get");
        };
        assert_eq!(
            keys,
            vec![
                Bytes::from_static(b"foo"),
                Bytes::from_static(b"bar"),
                Bytes::from_static(b"baz"),
            ]
        );
    }

    #[test]
    fn parse_request_rejects_uppercase_command() {
        // Commands are case-sensitive per the memcached text protocol.
        let mut buf = BytesMut::from(&b"GET foo\n"[..]);
        assert!(matches!(
            parse_request(&mut buf),
            Err(ProtocolError::Malformed("unknown command"))
        ));
    }

    #[test]
    fn parse_request_rejects_leading_whitespace() {
        let mut buf = BytesMut::from(&b" foo\n"[..]);
        assert!(matches!(
            parse_request(&mut buf),
            Err(ProtocolError::Malformed("unknown command"))
        ));
    }

    #[test]
    fn parse_request_propagates_get_invalid_key() {
        let mut buf = BytesMut::from(&b"get \x01bad\n"[..]);
        assert!(matches!(
            parse_request(&mut buf),
            Err(ProtocolError::InvalidKey)
        ));
    }

    #[test]
    fn parse_get_basic() {
        let single = parse_get(Bytes::from_static(b"foo")).unwrap();
        assert_eq!(
            single,
            Request::Get {
                keys: vec![Bytes::from_static(b"foo")]
            }
        );

        let Request::Get { keys } = parse_get(Bytes::from_static(b"foo bar baz")).unwrap() else {
            panic!("expected Request::Get");
        };
        assert_eq!(
            keys,
            vec![
                Bytes::from_static(b"foo"),
                Bytes::from_static(b"bar"),
                Bytes::from_static(b"baz"),
            ]
        );
    }
    #[test]
    fn parse_get_whitespace() {
        let cases: &[&[u8]] = &[
            b"foo bar",
            b"foo  bar",
            b"  foo bar",
            b"foo bar  ",
            b"  foo   bar  ",
        ];

        cases.iter().for_each(|input| {
            let Request::Get { keys } = parse_get(Bytes::copy_from_slice(input)).unwrap() else {
                panic!("expected Request::Get");
            };
            assert_eq!(keys.len(), 2);
            assert_eq!(keys[0].as_ref(), b"foo");
            assert_eq!(keys[1].as_ref(), b"bar");
        });
    }
    #[test]
    fn parse_get_rejects_empty() {
        assert!(matches!(
            parse_get(Bytes::new()),
            Err(ProtocolError::Malformed(_))
        ));

        assert!(matches!(
            parse_get(Bytes::from_static(b"   ")),
            Err(ProtocolError::Malformed(_))
        ));
    }
    #[test]
    fn parse_get_rejects_invalid_keys() {
        // validate_key errors should bubble through the iterator's collect.
        assert!(matches!(
            parse_get(Bytes::from_static(b"foo \x01bar")),
            Err(ProtocolError::InvalidKey)
        ));

        let mut huge = b"foo ".to_vec();
        huge.extend(std::iter::repeat_n(b'x', 251));
        assert!(matches!(
            parse_get(Bytes::from(huge)),
            Err(ProtocolError::KeyTooLong(251))
        ));
    }

    #[test]
    fn validate_key_basic_ascii() {
        assert!(validate_key(b"foo").is_ok());
        assert!(validate_key(b"a").is_ok());
    }

    #[test]
    fn validate_key_length() {
        assert!(matches!(validate_key(b""), Err(ProtocolError::InvalidKey)));

        let key_250 = vec![b'x'; 250];
        assert!(validate_key(&key_250).is_ok());

        let key_251 = vec![b'x'; 251];
        assert!(matches!(
            validate_key(&key_251),
            Err(ProtocolError::KeyTooLong(251))
        ))
    }

    #[test]
    fn validate_key_rejects_whitespace() {
        let cases: &[&[u8]] = &[
            b" foo",
            b"foo ",
            b"foo bar",
            b"foo\tbar",
            b"foo\nbar",
            b"foo\rbar",
            b"\x0Bfoo", // vertical tab
            b"foo\x0C", // form feed
        ];

        cases
            .iter()
            .for_each(|c| assert!(matches!(validate_key(c), Err(ProtocolError::InvalidKey))));
    }

    #[test]
    fn validate_key_rejects_control_chars() {
        let cases: &[u8] = &[0x00u8, 0x01, 0x07, 0x1B, 0x1F, 0x7F];

        cases.iter().for_each(|c| {
            let key = [b'a', *c, b'b'];
            assert!(matches!(validate_key(&key), Err(ProtocolError::InvalidKey)));
        });
    }

    fn pr(bytes: &[u8]) -> (Result<Option<Reply>, ProtocolError>, BytesMut) {
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
        let (result, _buf) = pr(b"FOO\r\n");
        assert!(matches!(
            result,
            Err(ProtocolError::Malformed("expected VALUE or END"))
        ));
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
        let (result, _buf) = pr(b"VALUE foo 0 abc\r\nbar\r\nEND\r\n");
        assert!(matches!(
            result,
            Err(ProtocolError::Malformed("invalid usize"))
        ));
    }

    #[test]
    fn parse_reply_rejects_missing_crlf_after_data() {
        let (result, _buf) = pr(b"VALUE foo 0 3\r\nbarXX\r\nEND\r\n");
        assert!(matches!(
            result,
            Err(ProtocolError::Malformed(
                "missing CRLF after value data block"
            ))
        ));
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

        let mut buf = BytesMut::new();
        original.serialize_into(&mut buf);
        let parsed = parse_reply(&mut buf).unwrap().unwrap();
        assert_eq!(parsed, original);
        assert!(buf.is_empty());
    }

    fn set(key: &'static [u8], flags: u32, exptime: i32, data: &'static [u8]) -> Request {
        Request::Set {
            key: Bytes::from_static(key),
            flags,
            exptime,
            data: Bytes::from_static(data),
        }
    }

    #[test]
    fn parse_request_set_basic() {
        let mut buf = BytesMut::from(&b"set foo 0 0 3\r\nbar\r\n"[..]);
        let req = parse_request(&mut buf).unwrap().unwrap();
        assert_eq!(req, set(b"foo", 0, 0, b"bar"));
        assert!(buf.is_empty());
    }

    #[test]
    fn parse_request_set_with_flags_and_exptime() {
        let mut buf = BytesMut::from(&b"set k 42 3600 1\r\nv\r\n"[..]);
        let req = parse_request(&mut buf).unwrap().unwrap();
        assert_eq!(req, set(b"k", 42, 3600, b"v"));
        assert!(buf.is_empty());
    }

    #[test]
    fn parse_request_set_negative_exptime() {
        let mut buf = BytesMut::from(&b"set k 0 -1 1\r\nv\r\n"[..]);
        let req = parse_request(&mut buf).unwrap().unwrap();
        assert_eq!(req, set(b"k", 0, -1, b"v"));
        assert!(buf.is_empty());
    }

    #[test]
    fn parse_request_set_empty_data() {
        let mut buf = BytesMut::from(&b"set k 0 0 0\r\n\r\n"[..]);
        let req = parse_request(&mut buf).unwrap().unwrap();
        assert_eq!(req, set(b"k", 0, 0, b""));
        assert!(buf.is_empty());
    }

    #[test]
    fn parse_request_set_accepts_lf_only_terminators() {
        let mut buf = BytesMut::from(&b"set foo 0 0 3\nbar\n"[..]);
        let req = parse_request(&mut buf).unwrap().unwrap();
        assert_eq!(req, set(b"foo", 0, 0, b"bar"));
        assert!(buf.is_empty());
    }

    #[test]
    fn parse_request_set_data_block_is_binary_safe() {
        // Body deliberately contains NULs, embedded CRLFs, and fake protocol
        // keywords. Trust the byte count; never re-frame from payload bytes.
        let payload: &[u8] = b"\x00\r\nset fake 0 0 0\r\n\xff";
        let mut wire = BytesMut::new();
        wire.extend_from_slice(b"set k 0 0 ");
        wire.extend_from_slice(payload.len().to_string().as_bytes());
        wire.extend_from_slice(b"\r\n");
        wire.extend_from_slice(payload);
        wire.extend_from_slice(b"\r\n");

        let req = parse_request(&mut wire).unwrap().unwrap();
        let Request::Set { data, .. } = req else {
            panic!("expected Request::Set");
        };
        assert_eq!(data.as_ref(), payload);
        assert!(wire.is_empty());
    }

    #[test]
    fn parse_request_set_returns_none_on_partial_frames_without_consuming() {
        // Each prefix of a valid set frame must return Ok(None) and leave
        // the buffer untouched, so the next read can complete the frame.
        let prefixes: &[&[u8]] = &[
            b"set foo 0 0 3",
            b"set foo 0 0 3\r\n",
            b"set foo 0 0 3\r\nb",
            b"set foo 0 0 3\r\nba",
            b"set foo 0 0 3\r\nbar",
            b"set foo 0 0 3\r\nbar\r",
        ];
        prefixes.iter().for_each(|prefix| {
            let mut buf = BytesMut::from(*prefix);
            assert!(
                matches!(parse_request(&mut buf), Ok(None)),
                "prefix={prefix:?}"
            );
            assert_eq!(buf.as_ref(), *prefix, "buf consumed for prefix {prefix:?}");
        });
    }

    #[test]
    fn parse_request_set_completes_after_buffering_more_bytes() {
        // Simulate fragmented arrival: the first call sees an incomplete
        // frame and returns Ok(None), the second call (after more data) succeeds.
        let mut buf = BytesMut::from(&b"set foo 0 0 3\r\nba"[..]);
        assert!(matches!(parse_request(&mut buf), Ok(None)));
        buf.extend_from_slice(b"r\r\n");
        let req = parse_request(&mut buf).unwrap().unwrap();
        assert_eq!(req, set(b"foo", 0, 0, b"bar"));
        assert!(buf.is_empty());
    }

    #[test]
    fn parse_request_pipelined_set_then_get() {
        let mut buf = BytesMut::from(&b"set foo 0 0 3\r\nbar\r\nget foo\r\n"[..]);
        let first = parse_request(&mut buf).unwrap().unwrap();
        assert_eq!(first, set(b"foo", 0, 0, b"bar"));
        assert_eq!(buf.as_ref(), b"get foo\r\n");

        let second = parse_request(&mut buf).unwrap().unwrap();
        assert_eq!(
            second,
            Request::Get {
                keys: vec![Bytes::from_static(b"foo")]
            }
        );
        assert!(buf.is_empty());
    }

    #[test]
    fn parse_request_set_rejects_missing_header_fields() {
        let cases: &[&[u8]] = &[b"set foo\r\n", b"set foo 0\r\n", b"set foo 0 0\r\n"];
        cases.iter().for_each(|input| {
            let mut buf = BytesMut::from(*input);
            assert!(matches!(
                parse_request(&mut buf),
                Err(ProtocolError::Malformed(SET_HEADER_HELP))
            ));
            assert!(buf.is_empty(), "header line consumed for {input:?}");
        });
    }

    #[test]
    fn parse_request_set_rejects_non_numeric_fields() {
        let cases: &[(&[u8], &str)] = &[
            (b"set foo abc 0 3\r\nbar\r\n", "invalid u32"),
            (b"set foo 0 abc 3\r\nbar\r\n", "invalid i32"),
            (b"set foo 0 0 abc\r\nbar\r\n", "invalid usize"),
        ];
        cases.iter().for_each(|(input, expected_msg)| {
            let mut buf = BytesMut::from(*input);
            match parse_request(&mut buf) {
                Err(ProtocolError::Malformed(m)) => assert_eq!(m, *expected_msg),
                other => panic!("expected Malformed({expected_msg:?}), got {other:?}"),
            }
        });
    }

    #[test]
    fn parse_request_set_rejects_invalid_key() {
        let mut buf = BytesMut::from(&b"set \x01bad 0 0 3\r\nbar\r\n"[..]);
        assert!(matches!(
            parse_request(&mut buf),
            Err(ProtocolError::InvalidKey)
        ));
    }

    #[test]
    fn parse_request_set_rejects_oversized_key() {
        let mut wire = BytesMut::from(&b"set "[..]);
        wire.extend(std::iter::repeat_n(b'x', 251));
        wire.extend_from_slice(b" 0 0 3\r\nbar\r\n");
        assert!(matches!(
            parse_request(&mut wire),
            Err(ProtocolError::KeyTooLong(251))
        ));
    }

    #[test]
    fn parse_request_set_rejects_noreply() {
        let mut buf = BytesMut::from(&b"set foo 0 0 3 noreply\r\nbar\r\n"[..]);
        assert!(matches!(
            parse_request(&mut buf),
            Err(ProtocolError::Malformed("noreply not yet supported"))
        ));
    }

    #[test]
    fn parse_request_set_rejects_extra_token() {
        let mut buf = BytesMut::from(&b"set foo 0 0 3 garbage\r\nbar\r\n"[..]);
        assert!(matches!(
            parse_request(&mut buf),
            Err(ProtocolError::Malformed(
                "set: unexpected extra token in header"
            ))
        ));
    }

    #[test]
    fn parse_request_set_rejects_missing_crlf_after_body() {
        let mut buf = BytesMut::from(&b"set foo 0 0 3\r\nbarXX\r\nEND\r\n"[..]);
        assert!(matches!(
            parse_request(&mut buf),
            Err(ProtocolError::Malformed("missing CRLF after set body"))
        ));
    }

    #[test]
    fn parse_request_set_rejects_lone_cr_in_body_terminator() {
        let mut buf = BytesMut::from(&b"set foo 0 0 3\r\nbar\rX"[..]);
        assert!(matches!(
            parse_request(&mut buf),
            Err(ProtocolError::Malformed(
                "missing LF after CR in set body terminator"
            ))
        ));
    }

    #[test]
    fn parse_request_set_round_trips_with_serializer() {
        let original = Request::Set {
            key: Bytes::from_static(b"alpha"),
            flags: u32::MAX,
            exptime: -42,
            data: Bytes::from_static(b"hello \x00 world\r\n"),
        };

        let mut buf = BytesMut::new();
        original.serialize_into(&mut buf);
        let parsed = parse_request(&mut buf).unwrap().unwrap();
        assert_eq!(parsed, original);
        assert!(buf.is_empty());
    }
}
