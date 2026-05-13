use bytes::{Bytes, BytesMut};

use crate::{
    error::ProtocolError,
    reply::{Reply, Value},
    request::Request,
};

const MAX_KEY_LEN: usize = 250;

pub fn parse_request(buf: &mut BytesMut) -> Result<Option<Request>, ProtocolError> {
    let eol_idx = match buf.iter().position(|&b| b == b'\n') {
        Some(i) => i,
        None => return Ok(None),
    };

    let mut line = buf.split_to(eol_idx + 1).freeze();

    if line.ends_with(b"\r\n") {
        line.truncate(line.len() - 2);
    } else {
        line.truncate(line.len() - 1);
    }

    parse_command(line).map(Some)
}

// only works for get
pub fn parse_reply(buf: &mut BytesMut) -> Result<Option<Reply>, ProtocolError> {
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

        if line == b"ERROR"
            || line.starts_with(b"CLIENT_ERROR")
            || line.starts_with(b"SERVER_ERROR")
        {
            return Err(ProtocolError::Malformed("backend returned error reply"));
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

fn parse_command(line: Bytes) -> Result<Request, ProtocolError> {
    let space = line
        .iter()
        .position(|&b| b == b' ')
        .ok_or(ProtocolError::Malformed("missing arguments"))?;

    let cmd = &line[..space];
    let rest = line.slice(space + 1..);

    match cmd {
        b"get" => parse_get(rest),
        _ => Err(ProtocolError::Malformed("unknown command")),
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
        let mut unknown = BytesMut::from(&b"set foo\n"[..]);
        assert!(matches!(
            parse_request(&mut unknown),
            Err(ProtocolError::Malformed("unknown command"))
        ));
        assert!(unknown.is_empty());

        for terminator in [&b"\n"[..], &b"\r\n"[..]] {
            let mut buf = BytesMut::from(terminator);
            assert!(matches!(
                parse_request(&mut buf),
                Err(ProtocolError::Malformed("missing arguments"))
            ));
            assert!(buf.is_empty());
        }
    }

    #[test]
    fn parse_command_get_single_key() {
        let req = parse_command(Bytes::from_static(b"get foo")).unwrap();
        assert_eq!(
            req,
            Request::Get {
                keys: vec![Bytes::from_static(b"foo")]
            }
        );
    }

    #[test]
    fn parse_command_get_multiple_keys() {
        let Request::Get { keys } = parse_command(Bytes::from_static(b"get foo bar baz")).unwrap();
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
    fn parse_command_rejects_missing_space() {
        assert!(matches!(
            parse_command(Bytes::from_static(b"get")),
            Err(ProtocolError::Malformed("missing arguments"))
        ));

        assert!(matches!(
            parse_command(Bytes::new()),
            Err(ProtocolError::Malformed("missing arguments"))
        ));
    }

    #[test]
    fn parse_command_rejects_unknown_command() {
        let cases: &[&[u8]] = &[b"set foo", b"GET foo", b" foo"];

        cases.iter().for_each(|input| {
            assert!(matches!(
                parse_command(Bytes::copy_from_slice(input)),
                Err(ProtocolError::Malformed("unknown command"))
            ));
        });
    }

    #[test]
    fn parse_command_get_propagates_parse_get_errors() {
        assert!(matches!(
            parse_command(Bytes::from_static(b"get ")),
            Err(ProtocolError::Malformed("get requires at least one key"))
        ));

        assert!(matches!(
            parse_command(Bytes::from_static(b"get \x01bad")),
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

        let Request::Get { keys } = parse_get(Bytes::from_static(b"foo bar baz")).unwrap();
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
            let Request::Get { keys } = parse_get(Bytes::copy_from_slice(input)).unwrap();
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
        huge.extend(std::iter::repeat(b'x').take(251));
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
    fn parse_reply_rejects_error_replies() {
        let inputs: &[&[u8]] = &[
            b"ERROR\r\n",
            b"CLIENT_ERROR oops\r\n",
            b"SERVER_ERROR boom\r\n",
        ];
        inputs.iter().for_each(|input| {
            let mut buf = BytesMut::from(*input);
            assert!(matches!(
                parse_reply(&mut buf),
                Err(ProtocolError::Malformed("backend returned error reply"))
            ));
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
}
