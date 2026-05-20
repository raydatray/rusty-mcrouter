use bytes::{Bytes, BytesMut};

use crate::{error::ProtocolError, request::Request};

use super::shared::{body_terminator_len, parse_i32, parse_u32, parse_usize, validate_key};

const SET_HEADER_HELP: &str = "set requires <key> <flags> <exptime> <bytes>";

pub(super) fn parse_request(
    buf: &mut BytesMut,
    eol_idx: usize,
    line_text_end: usize,
) -> Result<Option<Request>, ProtocolError> {
    // Header parse is pure over a slice; the wrapper handles buf mutation
    // so we keep partial-frame reads idempotent.
    let SetHeader {
        key,
        flags,
        exptime,
        bytes_count,
    } = match parse_set_header(&buf[..line_text_end]) {
        Ok(h) => h,
        Err(e) => {
            let _ = buf.split_to(eol_idx + 1);
            return Err(e);
        }
    };

    // Body framing mirrors VALUE block parsing: bytes_count payload, then a
    // CRLF or LF that does NOT count toward bytes_count.
    let data_start = eol_idx + 1;
    let data_end = data_start
        .checked_add(bytes_count)
        .ok_or(ProtocolError::Malformed("body length overflow"))?;
    let terminator_len = match body_terminator_len(buf, data_end) {
        Ok(Some(len)) => len,
        Ok(None) => return Ok(None),
        Err(e) => {
            let _ = buf.split_to(data_end + 1);
            return Err(e);
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

struct SetHeader {
    key: Bytes,
    flags: u32,
    exptime: i32,
    bytes_count: usize,
}

fn parse_set_header(header: &[u8]) -> Result<SetHeader, ProtocolError> {
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
    Ok(SetHeader {
        key: Bytes::copy_from_slice(key),
        flags: parse_u32(flags_bytes)?,
        exptime: parse_i32(exptime_bytes)?,
        bytes_count: parse_usize(bytes_bytes)?,
    })
}

#[cfg(test)]
mod tests {
    use bytes::{Bytes, BytesMut};

    use crate::{error::ProtocolError, parser::parse_request, request::Request};

    use super::SET_HEADER_HELP;

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
            Err(ProtocolError::Malformed("missing CRLF after body"))
        ));
    }

    #[test]
    fn parse_request_set_rejects_lone_cr_in_body_terminator() {
        let mut buf = BytesMut::from(&b"set foo 0 0 3\r\nbar\rX"[..]);
        assert!(matches!(
            parse_request(&mut buf),
            Err(ProtocolError::Malformed(
                "missing LF after CR in body terminator"
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
