pub(super) fn parse_u64(raw: &[u8]) -> Option<u64> {
    let raw = raw.strip_prefix(b"+").unwrap_or(raw);
    parse_unsigned(raw)
}

pub(super) fn parse_u32(raw: &[u8]) -> Option<u32> {
    u32::try_from(parse_u64(raw)?).ok()
}

pub(super) fn parse_usize(raw: &[u8]) -> Option<usize> {
    usize::try_from(parse_u64(raw)?).ok()
}

pub(super) fn parse_i32(raw: &[u8]) -> Option<i32> {
    let (negative, magnitude) = parse_signed_magnitude(raw)?;
    if negative {
        if magnitude == 1_u64 << 31 {
            Some(i32::MIN)
        } else {
            i32::try_from(magnitude).ok().map(|value| -value)
        }
    } else {
        i32::try_from(magnitude).ok()
    }
}

pub(super) fn parse_i64(raw: &[u8]) -> Option<i64> {
    let (negative, magnitude) = parse_signed_magnitude(raw)?;
    if negative {
        if magnitude == 1_u64 << 63 {
            Some(i64::MIN)
        } else {
            i64::try_from(magnitude).ok().map(|value| -value)
        }
    } else {
        i64::try_from(magnitude).ok()
    }
}

fn parse_signed_magnitude(raw: &[u8]) -> Option<(bool, u64)> {
    match raw.first() {
        Some(b'-') => Some((true, parse_unsigned(&raw[1..])?)),
        Some(b'+') => Some((false, parse_unsigned(&raw[1..])?)),
        Some(_) => Some((false, parse_unsigned(raw)?)),
        None => None,
    }
}

fn parse_unsigned(raw: &[u8]) -> Option<u64> {
    if raw.is_empty() {
        return None;
    }
    let mut value = 0_u64;
    for byte in raw {
        if !byte.is_ascii_digit() {
            return None;
        }
        value = value.checked_mul(10)?.checked_add(u64::from(byte - b'0'))?;
    }
    Some(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_boundaries() {
        assert_eq!(parse_u64(b"18446744073709551615"), Some(u64::MAX));
        assert_eq!(parse_u64(b"18446744073709551616"), None);
        assert_eq!(parse_i32(b"-2147483648"), Some(i32::MIN));
        assert_eq!(parse_i32(b"2147483647"), Some(i32::MAX));
        assert_eq!(parse_i64(b"-9223372036854775808"), Some(i64::MIN));
        assert_eq!(parse_i64(b"9223372036854775807"), Some(i64::MAX));
    }

    #[test]
    fn rejects_empty_signs_and_non_digits() {
        for raw in [b"".as_slice(), b"+", b"-", b"1x", b" 1"] {
            assert_eq!(parse_u64(raw), None);
            assert_eq!(parse_i64(raw), None);
        }
    }
}
