//! Shared write-side helpers for the Meta encoders.
//!
//! Both encoders emit the same primitive shapes — decimal integers, ` <flag>`
//! separators, base64 keys bounded by the wire's key cap — so the formatting
//! lives here once. Framing decisions (line limits, error types) stay with
//! each encoder.

use base64::{engine::general_purpose::STANDARD, Engine as _};
use bytes::BytesMut;

use crate::key::MAX_KEY_BYTES;

/// A base64-encoded key may expand up to 4/3ths of the raw key cap.
pub(super) const MAX_BASE64_KEY_BYTES: usize = MAX_KEY_BYTES.div_ceil(3) * 4;

/// The encoded key exceeds the wire's key-length cap.
pub(super) struct EncodedKeyTooLong;

/// Base64-encodes `key` into `scratch`, enforcing that the encoded form still
/// fits a wire key token.
pub(super) fn encode_base64_key<'a>(
    key: &[u8],
    scratch: &'a mut [u8; MAX_BASE64_KEY_BYTES],
) -> Result<&'a [u8], EncodedKeyTooLong> {
    let encoded_len = STANDARD
        .encode_slice(key, scratch)
        .map_err(|_| EncodedKeyTooLong)?;
    if encoded_len > MAX_KEY_BYTES {
        return Err(EncodedKeyTooLong);
    }
    Ok(&scratch[..encoded_len])
}

/// The line, including its `\r\n` terminator, exceeds the frame limit.
#[derive(Debug, Eq, PartialEq)]
pub(super) struct LineTooLong {
    pub(super) maximum: usize,
}

/// Enforces `max_frame` over the line started at `line_start`, then
/// terminates the line.
pub(super) fn finish_line(
    out: &mut BytesMut,
    line_start: usize,
    max_frame: usize,
) -> Result<(), LineTooLong> {
    if out.len() - line_start + 2 > max_frame {
        return Err(LineTooLong { maximum: max_frame });
    }
    out.extend_from_slice(b"\r\n");
    Ok(())
}

/// Appends ` <flag>`: every Meta flag is space-separated from its
/// predecessor.
pub(super) fn write_bare_flag(out: &mut BytesMut, flag: u8) {
    out.extend_from_slice(&[b' ', flag]);
}

pub(super) fn write_u64(out: &mut BytesMut, value: u64) {
    let mut digits = [0; 20];
    out.extend_from_slice(format_u64(value, &mut digits));
}

pub(super) fn write_i64(out: &mut BytesMut, value: i64) {
    if value < 0 {
        out.extend_from_slice(b"-");
    }
    write_u64(out, value.unsigned_abs());
}

/// Formats `value` into the tail of `digits`, returning the used suffix.
/// 20 bytes hold any u64.
pub(super) fn format_u64(mut value: u64, digits: &mut [u8; 20]) -> &[u8] {
    let mut start = digits.len();
    loop {
        start -= 1;
        digits[start] = b'0' + (value % 10) as u8;
        value /= 10;
        if value == 0 {
            break;
        }
    }
    &digits[start..]
}

/// An integer a reply field can carry; lets one write path serve the
/// unsigned and signed token forms.
pub(super) trait WireInt: Copy {
    fn write(self, out: &mut BytesMut);
}

impl WireInt for u64 {
    fn write(self, out: &mut BytesMut) {
        write_u64(out, self);
    }
}

impl WireInt for i64 {
    fn write(self, out: &mut BytesMut) {
        write_i64(out, self);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_decimal_boundaries() {
        let mut out = BytesMut::new();
        write_u64(&mut out, 0);
        write_bare_flag(&mut out, b'C');
        write_u64(&mut out, u64::MAX);
        write_bare_flag(&mut out, b'T');
        write_i64(&mut out, i64::MIN);
        assert_eq!(
            out,
            b"0 C18446744073709551615 T-9223372036854775808".as_slice()
        );
    }

    #[test]
    fn bounds_encoded_keys() {
        let mut scratch = [0; MAX_BASE64_KEY_BYTES];
        // 186 raw bytes encode to 248 <= 250: accepted.
        assert!(encode_base64_key(&[0; 186], &mut scratch).is_ok());
        // 188 raw bytes encode to 252 > 250: rejected.
        assert!(encode_base64_key(&[0; 188], &mut scratch).is_err());
    }
}
