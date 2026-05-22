use bytes::BytesMut;

use crate::{error::ProtocolError, request::Request};

use super::shared::{parse_storage_request, StorageRequest};

const PREPEND_HEADER_HELP: &str = "prepend requires <key> <flags> <exptime> <bytes>";

pub(super) fn parse_request(
    buf: &mut BytesMut,
    eol_idx: usize,
    line_text_end: usize,
) -> Result<Option<Request>, ProtocolError> {
    let Some(StorageRequest {
        key,
        flags,
        exptime,
        data,
    }) = parse_storage_request(
        buf,
        eol_idx,
        line_text_end,
        b"prepend ",
        PREPEND_HEADER_HELP,
        "prepend: unexpected extra token in header",
    )?
    else {
        return Ok(None);
    };
    Ok(Some(Request::Prepend {
        key,
        flags,
        exptime,
        data,
    }))
}

#[cfg(test)]
mod tests {
    use bytes::{Bytes, BytesMut};

    use crate::{error::ProtocolError, parser::parse_request, request::Request};

    fn prepend(key: &'static [u8], flags: u32, exptime: i32, data: &'static [u8]) -> Request {
        Request::Prepend {
            key: Bytes::from_static(key),
            flags,
            exptime,
            data: Bytes::from_static(data),
        }
    }

    #[test]
    fn parse_request_prepend_basic() {
        let mut buf = BytesMut::from(&b"prepend foo 0 0 3\r\nbar\r\n"[..]);
        assert_eq!(
            parse_request(&mut buf).unwrap().unwrap(),
            prepend(b"foo", 0, 0, b"bar")
        );
        assert!(buf.is_empty());
    }

    #[test]
    fn parse_request_prepend_preserves_flags_and_exptime_in_request_struct() {
        let mut buf = BytesMut::from(&b"prepend k 42 3600 1\r\nv\r\n"[..]);
        assert_eq!(
            parse_request(&mut buf).unwrap().unwrap(),
            prepend(b"k", 42, 3600, b"v")
        );
        assert!(buf.is_empty());
    }

    #[test]
    fn parse_request_prepend_rejects_invalid_key() {
        let mut buf = BytesMut::from(&b"prepend \x01bad 0 0 3\r\nbar\r\n"[..]);
        assert!(matches!(
            parse_request(&mut buf),
            Err(ProtocolError::InvalidKey)
        ));
    }

    #[test]
    fn parse_request_prepend_rejects_noreply() {
        let mut buf = BytesMut::from(&b"prepend foo 0 0 3 noreply\r\nbar\r\n"[..]);
        assert!(matches!(
            parse_request(&mut buf),
            Err(ProtocolError::Malformed("noreply not yet supported"))
        ));
    }

    #[test]
    fn parse_request_prepend_rejects_extra_token() {
        let mut buf = BytesMut::from(&b"prepend foo 0 0 3 garbage\r\nbar\r\n"[..]);
        assert!(matches!(
            parse_request(&mut buf),
            Err(ProtocolError::Malformed(
                "prepend: unexpected extra token in header"
            ))
        ));
    }

    #[test]
    fn parse_request_prepend_round_trips_with_serializer() {
        let original = Request::Prepend {
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
