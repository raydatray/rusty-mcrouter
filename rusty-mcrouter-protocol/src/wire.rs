use bytes::{BufMut, BytesMut};

// TODO: extension trait on BufMut so callers write `out.put_decimal(n)`
//       — matches the bytes crate's own `put_u64` shape; no perf change
pub(crate) fn write_decimal(out: &mut BytesMut, n: u64) {
    if n == 0 {
        out.put_u8(b'0');
        return;
    }

    let digits = n.ilog10() + 1;
    out.extend(
        (0..digits)
            .rev()
            .map(|p| b'0' + ((n / 10u64.pow(p)) % 10) as u8),
    );
}

pub(crate) fn write_signed_decimal(out: &mut BytesMut, n: i64) {
    if n < 0 {
        out.put_u8(b'-');
    }
    write_decimal(out, n.unsigned_abs());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_decimal_zero() {
        let mut out = BytesMut::new();
        write_decimal(&mut out, 0);
        assert_eq!(out.as_ref(), b"0");
    }

    #[test]
    fn write_decimal_single_and_multi_digit() {
        let cases: &[(u64, &[u8])] = &[
            (1, b"1"),
            (9, b"9"),
            (10, b"10"),
            (99, b"99"),
            (100, b"100"),
        ];

        cases.iter().for_each(|&(n, expected)| {
            let mut out = BytesMut::new();
            write_decimal(&mut out, n);
            assert_eq!(out.as_ref(), expected, "n={n}");
        });
    }

    #[test]
    fn write_decimal_u32_max() {
        let mut out = BytesMut::new();
        write_decimal(&mut out, u32::MAX as u64);
        assert_eq!(out.as_ref(), b"4294967295");
    }

    #[test]
    fn write_decimal_u64_max() {
        let mut out = BytesMut::new();
        write_decimal(&mut out, u64::MAX);
        assert_eq!(out.as_ref(), b"18446744073709551615");
    }

    #[test]
    fn write_decimal_appends_without_clobbering() {
        let mut out = BytesMut::from(&b"n="[..]);
        write_decimal(&mut out, 42);
        assert_eq!(out.as_ref(), b"n=42");
    }

    #[test]
    fn write_signed_decimal_handles_zero_and_positives() {
        let cases: &[(i64, &[u8])] = &[(0, b"0"), (1, b"1"), (1234, b"1234")];
        cases.iter().for_each(|&(n, expected)| {
            let mut out = BytesMut::new();
            write_signed_decimal(&mut out, n);
            assert_eq!(out.as_ref(), expected, "n={n}");
        });
    }

    #[test]
    fn write_signed_decimal_handles_negatives() {
        let cases: &[(i64, &[u8])] = &[(-1, b"-1"), (-1234, b"-1234")];
        cases.iter().for_each(|&(n, expected)| {
            let mut out = BytesMut::new();
            write_signed_decimal(&mut out, n);
            assert_eq!(out.as_ref(), expected, "n={n}");
        });
    }

    #[test]
    fn write_signed_decimal_handles_extremes() {
        let mut out = BytesMut::new();
        write_signed_decimal(&mut out, i32::MIN as i64);
        assert_eq!(out.as_ref(), b"-2147483648");

        let mut out = BytesMut::new();
        write_signed_decimal(&mut out, i32::MAX as i64);
        assert_eq!(out.as_ref(), b"2147483647");
    }
}
