use base64::{engine::general_purpose::STANDARD, Engine as _};
use bytes::BytesMut;
use std::fmt::Write as _;

use crate::key::MAX_KEY_BYTES;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct EncodedKeyTooLongError;

/// base64 encodes `key` and appends it to `out`, enforcing `MAX_KEY_BYTES` len
/// leaves `out` unchanged on failure
pub(super) fn write_base64_key(
    out: &mut BytesMut,
    key: &[u8],
) -> Result<(), EncodedKeyTooLongError> {
    let mut scratch = [0; MAX_KEY_BYTES];
    let encoded_len = STANDARD
        .encode_slice(key, &mut scratch)
        .map_err(|_| EncodedKeyTooLongError)?;

    out.extend_from_slice(&scratch[..encoded_len]);

    Ok(())
}

/// the current line including its `\r\n` terminator, exceeds the frame limit
#[derive(Debug, Eq, PartialEq)]
pub(super) struct LineTooLongError;

/// finish the line with `\r\n`, checking that it is at most `max_frame`
/// from `line_start`
pub(super) fn finish_line(
    out: &mut BytesMut,
    line_start: usize,
    max_frame: usize,
) -> Result<(), LineTooLongError> {
    if out.len() - line_start + 2 > max_frame {
        return Err(LineTooLongError);
    }

    out.extend_from_slice(b"\r\n");

    Ok(())
}

/// appends `< flag>` to `out`, space seperating the Meta flag from its
/// predecessor
pub(super) fn write_bare_flag(out: &mut BytesMut, flag: u8) {
    out.extend_from_slice(&[b' ', flag]);
}

pub(super) fn write_u64(out: &mut BytesMut, value: u64) {
    write!(out, "{value}").expect("integer formatting into BytesMut should succeed")
}

pub(super) fn write_i64(out: &mut BytesMut, value: i64) {
    write!(out, "{value}").expect("integer formatting into BytesMut should succeed")
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
        // 186 raw bytes encode to 248 <= 250
        assert!(write_base64_key(&mut out, &[0; 186]).is_ok());
        assert_eq!(out.len(), 248);

        // 188 raw bytes encode to 252 > 250
        assert!(write_base64_key(&mut out, &[0; 188]).is_err());
        assert_eq!(out.len(), 248);
    }
}
