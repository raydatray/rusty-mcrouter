/// The token is not a decimal number that fits the requested width.
#[derive(Debug, Eq, PartialEq)]
pub(super) struct BadNumber;

pub(super) fn parse_u64(raw: &[u8]) -> Result<u64, BadNumber> {
    let raw = raw.strip_prefix(b"+").unwrap_or(raw);
    parse_unsigned(raw)
}

pub(super) fn parse_u32(raw: &[u8]) -> Result<u32, BadNumber> {
    u32::try_from(parse_u64(raw)?).map_err(|_| BadNumber)
}

pub(super) fn parse_usize(raw: &[u8]) -> Result<usize, BadNumber> {
    usize::try_from(parse_u64(raw)?).map_err(|_| BadNumber)
}

pub(super) fn parse_i32(raw: &[u8]) -> Result<i32, BadNumber> {
    let (negative, magnitude) = parse_signed_magnitude(raw)?;
    if negative {
        if magnitude == 1_u64 << 31 {
            Ok(i32::MIN)
        } else {
            i32::try_from(magnitude)
                .map(|value| -value)
                .map_err(|_| BadNumber)
        }
    } else {
        i32::try_from(magnitude).map_err(|_| BadNumber)
    }
}

pub(super) fn parse_i64(raw: &[u8]) -> Result<i64, BadNumber> {
    let (negative, magnitude) = parse_signed_magnitude(raw)?;
    if negative {
        if magnitude == 1_u64 << 63 {
            Ok(i64::MIN)
        } else {
            i64::try_from(magnitude)
                .map(|value| -value)
                .map_err(|_| BadNumber)
        }
    } else {
        i64::try_from(magnitude).map_err(|_| BadNumber)
    }
}

fn parse_signed_magnitude(raw: &[u8]) -> Result<(bool, u64), BadNumber> {
    match raw.first() {
        Some(b'-') => Ok((true, parse_unsigned(&raw[1..])?)),
        Some(b'+') => Ok((false, parse_unsigned(&raw[1..])?)),
        Some(_) => Ok((false, parse_unsigned(raw)?)),
        None => Err(BadNumber),
    }
}

fn parse_unsigned(raw: &[u8]) -> Result<u64, BadNumber> {
    if raw.is_empty() {
        return Err(BadNumber);
    }
    let mut value = 0_u64;
    for byte in raw {
        if !byte.is_ascii_digit() {
            return Err(BadNumber);
        }
        value = value
            .checked_mul(10)
            .and_then(|value| value.checked_add(u64::from(byte - b'0')))
            .ok_or(BadNumber)?;
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_boundaries() {
        assert_eq!(parse_u64(b"18446744073709551615"), Ok(u64::MAX));
        assert_eq!(parse_u64(b"18446744073709551616"), Err(BadNumber));
        assert_eq!(parse_i32(b"-2147483648"), Ok(i32::MIN));
        assert_eq!(parse_i32(b"2147483647"), Ok(i32::MAX));
        assert_eq!(parse_i64(b"-9223372036854775808"), Ok(i64::MIN));
        assert_eq!(parse_i64(b"9223372036854775807"), Ok(i64::MAX));
    }

    #[test]
    fn rejects_empty_signs_and_non_digits() {
        for raw in [b"".as_slice(), b"+", b"-", b"1x", b" 1"] {
            assert_eq!(parse_u64(raw), Err(BadNumber));
            assert_eq!(parse_i64(raw), Err(BadNumber));
        }
    }
}
