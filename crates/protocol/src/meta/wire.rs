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
const MAX_BASE64_KEY_BYTES: usize = MAX_KEY_BYTES.div_ceil(3) * 4;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EncodedKeyTooLong;

/// Base64-encodes `key` and appends it to `out`, enforcing that the encoded
/// form still fits a wire key token.
pub fn write_base64_key(out: &mut BytesMut, key: &[u8]) -> Result<(), EncodedKeyTooLong> {
    let mut scratch = [0; MAX_BASE64_KEY_BYTES];
    let encoded_len = STANDARD
        .encode_slice(key, &mut scratch)
        .map_err(|_| EncodedKeyTooLong)?;
    if encoded_len > MAX_KEY_BYTES {
        return Err(EncodedKeyTooLong);
    }
    out.extend_from_slice(&scratch[..encoded_len]);
    Ok(())
}

/// The line, including its `\r\n` terminator, exceeds the frame limit.
#[derive(Debug, Eq, PartialEq)]
pub struct LineTooLong {
    pub maximum: usize,
}

/// Enforces `max_frame` over the line started at `line_start`, then
/// terminates the line.
pub fn finish_line(
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
pub fn write_bare_flag(out: &mut BytesMut, flag: u8) {
    out.extend_from_slice(&[b' ', flag]);
}

pub fn write_u64(out: &mut BytesMut, value: u64) {
    let mut digits = [0; 20];
    out.extend_from_slice(format_u64(value, &mut digits));
}

pub fn write_i64(out: &mut BytesMut, value: i64) {
    if value < 0 {
        out.extend_from_slice(b"-");
    }
    write_u64(out, value.unsigned_abs());
}

/// Formats `value` into the tail of `digits`, returning the used suffix.
/// 20 bytes hold any u64.
pub fn format_u64(mut value: u64, digits: &mut [u8; 20]) -> &[u8] {
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
        let mut out = BytesMut::new();
        // 186 raw bytes encode to 248 <= 250: accepted.
        assert!(write_base64_key(&mut out, &[0; 186]).is_ok());
        assert_eq!(out.len(), 248);
        // 188 raw bytes encode to 252 > 250: rejected, output untouched.
        assert!(write_base64_key(&mut out, &[0; 188]).is_err());
        assert_eq!(out.len(), 248);
    }
}
