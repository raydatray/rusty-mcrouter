use bytes::Bytes;

use crate::{error::ProtocolError, request::Request};

const MAX_KEY_LEN: usize = 250;

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
