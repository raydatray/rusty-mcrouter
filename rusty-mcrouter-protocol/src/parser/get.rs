use bytes::BytesMut;

use crate::{
    request::{Parsed, Request},
    ProtocolError, Result,
};

use super::shared::{extract_command_args, validate_key};

pub(super) fn parse_request(buf: &mut BytesMut, eol_idx: usize) -> Result<Option<Parsed>> {
    let rest = extract_command_args(buf, eol_idx, b"get ")?;

    let mut segments = rest
        .split(|&b| b == b' ')
        .filter(|seg| !seg.is_empty())
        .map(|seg| validate_key(seg).map(|()| rest.slice_ref(seg)));

    let Some(first) = segments.next().transpose()? else {
        return Err(ProtocolError::Malformed("get requires at least one key"));
    };

    let Some(second) = segments.next().transpose()? else {
        return Ok(Some(Parsed::One(Request::Get { key: first })));
    };

    let mut keys = vec![first, second];
    for seg in segments {
        keys.push(seg?);
    }

    Ok(Some(Parsed::MultiGet(keys)))
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
    fn parse_request_get_single_key_is_parsed_one() {
        let mut buf = BytesMut::from(&b"get foo\r\n"[..]);
        assert_eq!(
            parse_request(&mut buf).unwrap().unwrap(),
            Parsed::One(Request::Get {
                key: Bytes::from_static(b"foo")
            })
        );
        assert!(buf.is_empty());
    }

    #[test]
    fn parse_request_get_multiple_keys_is_multiget() {
        let mut buf = BytesMut::from(&b"get foo bar baz\r\n"[..]);
        let Parsed::MultiGet(keys) = parse_request(&mut buf).unwrap().unwrap() else {
            panic!("expected Parsed::MultiGet");
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
    fn parse_request_get_handles_internal_whitespace() {
        let cases: &[&[u8]] = &[
            b"get foo bar\r\n",
            b"get foo  bar\r\n",
            b"get   foo bar\r\n",
            b"get foo bar  \r\n",
            b"get   foo   bar  \r\n",
        ];

        cases.iter().for_each(|input| {
            let mut buf = BytesMut::from(*input);
            let Parsed::MultiGet(keys) = parse_request(&mut buf).unwrap().unwrap() else {
                panic!("expected Parsed::MultiGet for {input:?}");
            };
            assert_eq!(keys.len(), 2, "input={input:?}");
            assert_eq!(keys[0].as_ref(), b"foo");
            assert_eq!(keys[1].as_ref(), b"bar");
        });
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
    fn parse_request_rejects_get_with_only_whitespace_args() {
        for input in [&b"get \r\n"[..], &b"get    \r\n"[..]] {
            let mut buf = BytesMut::from(input);
            assert!(
                matches!(
                    parse_request(&mut buf),
                    Err(ProtocolError::Malformed("get requires at least one key"))
                ),
                "input={input:?}"
            );
        }
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
    fn parse_request_propagates_get_oversized_key() {
        let mut wire = Vec::from(&b"get foo "[..]);
        wire.extend(std::iter::repeat_n(b'x', 251));
        wire.extend_from_slice(b"\r\n");
        let mut buf = BytesMut::from(&wire[..]);
        assert!(matches!(
            parse_request(&mut buf),
            Err(ProtocolError::KeyTooLong(251))
        ));
    }
}
