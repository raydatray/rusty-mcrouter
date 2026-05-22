use bytes::BytesMut;

use crate::{error::ProtocolError, request::Request};

use super::shared::{extra_token_error, extract_command_args, parse_i32, validate_key};

pub(super) fn parse_request(
    buf: &mut BytesMut,
    eol_idx: usize,
) -> Result<Option<Request>, ProtocolError> {
    let rest = extract_command_args(buf, eol_idx, b"touch ")?;

    let mut parts = rest.split(|&b| b == b' ').filter(|s| !s.is_empty());
    let key = parts
        .next()
        .ok_or(ProtocolError::Malformed("touch requires <key> <exptime>"))?;
    let exptime_bytes = parts
        .next()
        .ok_or(ProtocolError::Malformed("touch requires <key> <exptime>"))?;
    if let Some(extra) = parts.next() {
        return Err(extra_token_error(extra, "touch: unexpected extra token"));
    }

    validate_key(key)?;
    let exptime = parse_i32(exptime_bytes)?;
    Ok(Some(Request::Touch {
        key: rest.slice_ref(key),
        exptime,
    }))
}

#[cfg(test)]
mod tests {
    use bytes::{Bytes, BytesMut};

    use crate::{error::ProtocolError, parser::parse_request, request::Request};

    #[test]
    fn parse_request_touch_basic() {
        let mut buf = BytesMut::from(&b"touch foo 60\r\n"[..]);
        assert_eq!(
            parse_request(&mut buf).unwrap().unwrap(),
            Request::Touch {
                key: Bytes::from_static(b"foo"),
                exptime: 60,
            }
        );
        assert!(buf.is_empty());
    }

    #[test]
    fn parse_request_touch_rejects_extra_token() {
        let mut buf = BytesMut::from(&b"touch foo 60 garbage\r\n"[..]);
        assert!(matches!(
            parse_request(&mut buf),
            Err(ProtocolError::Malformed("touch: unexpected extra token"))
        ));
    }

    #[test]
    fn parse_request_touch_round_trips_with_serializer() {
        let original = Request::Touch {
            key: Bytes::from_static(b"alpha"),
            exptime: i32::MIN,
        };

        let mut buf = BytesMut::new();
        original.serialize_into(&mut buf);
        let parsed = parse_request(&mut buf).unwrap().unwrap();
        assert_eq!(parsed, original);
        assert!(buf.is_empty());
    }
}
