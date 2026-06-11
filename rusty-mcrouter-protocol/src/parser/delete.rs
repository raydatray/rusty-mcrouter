use bytes::BytesMut;

use crate::{request::Request, ProtocolError, Result};

use super::shared::{extra_token_error, extract_command_args, validate_key};

pub(super) fn parse_request(buf: &mut BytesMut, eol_idx: usize) -> Result<Option<Request>> {
    let rest = extract_command_args(buf, eol_idx, b"delete ")?;

    let mut parts = rest.split(|&b| b == b' ').filter(|s| !s.is_empty());
    let key = parts
        .next()
        .ok_or(ProtocolError::Malformed("delete requires <key>"))?;
    if let Some(extra) = parts.next() {
        return Err(extra_token_error(extra, "delete: unexpected extra token"));
    }
    validate_key(key)?;
    Ok(Some(Request::Delete {
        key: rest.slice_ref(key),
    }))
}

#[cfg(test)]
mod tests {
    use bytes::{Bytes, BytesMut};

    use crate::{
        parser::parse_request,
        request::{Parsed, Request},
        ProtocolError,
    };

    #[test]
    fn parse_request_delete_basic() {
        let cases: &[&[u8]] = &[b"delete foo\n", b"delete foo\r\n"];

        cases.iter().for_each(|input| {
            let mut buf = BytesMut::from(*input);
            let req = parse_request(&mut buf).unwrap().unwrap();
            assert_eq!(
                req,
                Parsed::One(Request::Delete {
                    key: Bytes::from_static(b"foo")
                })
            );
            assert!(buf.is_empty());
        });
    }

    #[test]
    fn parse_request_rejects_delete_without_args() {
        for input in [&b"delete\n"[..], &b"delete\r\n"[..]] {
            let mut buf = BytesMut::from(input);
            assert!(matches!(
                parse_request(&mut buf),
                Err(ProtocolError::Malformed("missing arguments"))
            ));
            assert!(buf.is_empty());
        }
    }

    #[test]
    fn parse_request_rejects_delete_with_no_key() {
        for input in [
            &b"delete \n"[..],
            &b"delete \r\n"[..],
            &b"delete   \r\n"[..],
        ] {
            let mut buf = BytesMut::from(input);
            assert!(matches!(
                parse_request(&mut buf),
                Err(ProtocolError::Malformed("delete requires <key>"))
            ));
            assert!(buf.is_empty(), "frame consumed for {input:?}");
        }
    }

    #[test]
    fn parse_request_rejects_delete_with_extra_tokens() {
        let cases: &[&[u8]] = &[b"delete foo bar\r\n", b"delete foo 0\r\n"];
        cases.iter().for_each(|input| {
            let mut buf = BytesMut::from(*input);
            assert!(
                matches!(
                    parse_request(&mut buf),
                    Err(ProtocolError::Malformed("delete: unexpected extra token"))
                ),
                "expected extra-token error for {input:?}"
            );
            assert!(buf.is_empty(), "frame consumed for {input:?}");
        });
    }

    #[test]
    fn parse_request_rejects_delete_with_noreply() {
        let mut buf = BytesMut::from(&b"delete foo noreply\r\n"[..]);
        assert!(matches!(
            parse_request(&mut buf),
            Err(ProtocolError::Malformed("noreply not yet supported"))
        ));
    }

    #[test]
    fn parse_request_propagates_delete_invalid_key() {
        let mut buf = BytesMut::from(&b"delete \x01bad\n"[..]);
        assert!(matches!(
            parse_request(&mut buf),
            Err(ProtocolError::InvalidKey)
        ));
    }

    #[test]
    fn parse_request_delete_round_trips_with_serializer() {
        let original = Request::Delete {
            key: Bytes::from_static(b"some-key-with-dashes"),
        };

        let mut buf = BytesMut::new();
        original.serialize_into(&mut buf);
        let parsed = parse_request(&mut buf).unwrap().unwrap();
        assert_eq!(parsed, Parsed::One(original));
        assert!(buf.is_empty());
    }
}
