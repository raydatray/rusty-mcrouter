use bytes::BytesMut;

use crate::{request::Request, ProtocolError, Result};

use super::shared::{extra_token_error, extract_command_args, parse_u64, validate_key};

pub(super) fn parse_request(
    buf: &mut BytesMut,
    eol_idx: usize,
) -> Result<Option<Request>> {
    let rest = extract_command_args(buf, eol_idx, b"decr ")?;

    let mut parts = rest.split(|&b| b == b' ').filter(|s| !s.is_empty());
    let key = parts
        .next()
        .ok_or(ProtocolError::Malformed("decr requires <key> <delta>"))?;
    let delta_bytes = parts
        .next()
        .ok_or(ProtocolError::Malformed("decr requires <key> <delta>"))?;
    if let Some(extra) = parts.next() {
        return Err(extra_token_error(extra, "decr: unexpected extra token"));
    }

    validate_key(key)?;
    let delta = parse_u64(delta_bytes)?;
    Ok(Some(Request::Decr {
        key: rest.slice_ref(key),
        delta,
    }))
}

#[cfg(test)]
mod tests {
    use bytes::{Bytes, BytesMut};

    use crate::{parser::parse_request, request::Request, ProtocolError};

    #[test]
    fn parse_request_decr_basic() {
        let mut buf = BytesMut::from(&b"decr foo 1\r\n"[..]);
        assert_eq!(
            parse_request(&mut buf).unwrap().unwrap(),
            Request::Decr {
                key: Bytes::from_static(b"foo"),
                delta: 1,
            }
        );
        assert!(buf.is_empty());
    }

    #[test]
    fn parse_request_decr_rejects_extra_token() {
        let mut buf = BytesMut::from(&b"decr foo 1 garbage\r\n"[..]);
        assert!(matches!(
            parse_request(&mut buf),
            Err(ProtocolError::Malformed("decr: unexpected extra token"))
        ));
    }

    #[test]
    fn parse_request_decr_round_trips_with_serializer() {
        let original = Request::Decr {
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
