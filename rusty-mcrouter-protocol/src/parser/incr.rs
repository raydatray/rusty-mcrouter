use bytes::BytesMut;

use crate::{request::Request, ProtocolError, Result};

use super::shared::{extra_token_error, extract_command_args, parse_u64, validate_key};

pub(super) fn parse_request(
    buf: &mut BytesMut,
    eol_idx: usize,
) -> Result<Option<Request>> {
    let rest = extract_command_args(buf, eol_idx, b"incr ")?;

    let mut parts = rest.split(|&b| b == b' ').filter(|s| !s.is_empty());
    let key = parts
        .next()
        .ok_or(ProtocolError::Malformed("incr requires <key> <delta>"))?;
    let delta_bytes = parts
        .next()
        .ok_or(ProtocolError::Malformed("incr requires <key> <delta>"))?;
    if let Some(extra) = parts.next() {
        return Err(extra_token_error(extra, "incr: unexpected extra token"));
    }

    validate_key(key)?;
    let delta = parse_u64(delta_bytes)?;
    Ok(Some(Request::Incr {
        key: rest.slice_ref(key),
        delta,
    }))
}

#[cfg(test)]
mod tests {
    use bytes::{Bytes, BytesMut};

    use crate::{parser::parse_request, request::Request, ProtocolError};

    #[test]
    fn parse_request_incr_basic() {
        let mut buf = BytesMut::from(&b"incr foo 1\r\n"[..]);
        assert_eq!(
            parse_request(&mut buf).unwrap().unwrap(),
            Request::Incr {
                key: Bytes::from_static(b"foo"),
                delta: 1,
            }
        );
        assert!(buf.is_empty());
    }

    #[test]
    fn parse_request_incr_max_delta() {
        let mut buf = BytesMut::from(&b"incr k 18446744073709551615\r\n"[..]);
        assert_eq!(
            parse_request(&mut buf).unwrap().unwrap(),
            Request::Incr {
                key: Bytes::from_static(b"k"),
                delta: u64::MAX,
            }
        );
    }

    #[test]
    fn parse_request_incr_rejects_missing_delta() {
        let mut buf = BytesMut::from(&b"incr foo\r\n"[..]);
        assert!(matches!(
            parse_request(&mut buf),
            Err(ProtocolError::Malformed("incr requires <key> <delta>"))
        ));
    }

    #[test]
    fn parse_request_incr_rejects_missing_key() {
        let mut buf = BytesMut::from(&b"incr\r\n"[..]);
        assert!(matches!(
            parse_request(&mut buf),
            Err(ProtocolError::Malformed("missing arguments"))
        ));
    }

    #[test]
    fn parse_request_incr_rejects_non_numeric_delta() {
        let mut buf = BytesMut::from(&b"incr foo abc\r\n"[..]);
        assert!(matches!(
            parse_request(&mut buf),
            Err(ProtocolError::Malformed("invalid u64"))
        ));
    }

    #[test]
    fn parse_request_incr_rejects_negative_delta() {
        let mut buf = BytesMut::from(&b"incr foo -5\r\n"[..]);
        assert!(matches!(
            parse_request(&mut buf),
            Err(ProtocolError::Malformed("invalid u64"))
        ));
    }

    #[test]
    fn parse_request_incr_rejects_invalid_key() {
        let mut buf = BytesMut::from(&b"incr \x01bad 1\r\n"[..]);
        assert!(matches!(
            parse_request(&mut buf),
            Err(ProtocolError::InvalidKey)
        ));
    }

    #[test]
    fn parse_request_incr_rejects_noreply() {
        let mut buf = BytesMut::from(&b"incr foo 1 noreply\r\n"[..]);
        assert!(matches!(
            parse_request(&mut buf),
            Err(ProtocolError::Malformed("noreply not yet supported"))
        ));
    }

    #[test]
    fn parse_request_incr_rejects_extra_token() {
        let mut buf = BytesMut::from(&b"incr foo 1 garbage\r\n"[..]);
        assert!(matches!(
            parse_request(&mut buf),
            Err(ProtocolError::Malformed("incr: unexpected extra token"))
        ));
    }

    #[test]
    fn parse_request_incr_round_trips_with_serializer() {
        let original = Request::Incr {
            key: Bytes::from_static(b"counter"),
            delta: 12345,
        };

        let mut buf = BytesMut::new();
        original.serialize_into(&mut buf);
        let parsed = parse_request(&mut buf).unwrap().unwrap();
        assert_eq!(parsed, original);
        assert!(buf.is_empty());
    }
}
