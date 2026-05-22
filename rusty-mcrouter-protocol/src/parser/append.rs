use bytes::BytesMut;

use crate::{error::ProtocolError, request::Request};

use super::shared::{parse_storage_request, StorageRequest};

const APPEND_HEADER_HELP: &str = "append requires <key> <flags> <exptime> <bytes>";

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
        b"append ",
        APPEND_HEADER_HELP,
        "append: unexpected extra token in header",
    )?
    else {
        return Ok(None);
    };
    Ok(Some(Request::Append {
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

    fn append(key: &'static [u8], flags: u32, exptime: i32, data: &'static [u8]) -> Request {
        Request::Append {
            key: Bytes::from_static(key),
            flags,
            exptime,
            data: Bytes::from_static(data),
        }
    }

    #[test]
    fn parse_request_append_basic() {
        let mut buf = BytesMut::from(&b"append foo 0 0 3\r\nbar\r\n"[..]);
        assert_eq!(
            parse_request(&mut buf).unwrap().unwrap(),
            append(b"foo", 0, 0, b"bar")
        );
        assert!(buf.is_empty());
    }

    #[test]
    fn parse_request_append_rejects_extra_token() {
        let mut buf = BytesMut::from(&b"append foo 0 0 3 garbage\r\nbar\r\n"[..]);
        assert!(matches!(
            parse_request(&mut buf),
            Err(ProtocolError::Malformed(
                "append: unexpected extra token in header"
            ))
        ));
    }

    #[test]
    fn parse_request_append_round_trips_with_serializer() {
        let original = Request::Append {
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
