use bytes::{Bytes, BytesMut};

use crate::{error::ProtocolError, request::Request};

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
}
