use std::str::from_utf8;

use bytes::{Bytes, BytesMut};

use crate::error::ProtocolError;

const MAX_KEY_LEN: usize = 250;

pub(super) fn read_line(buf: &[u8], offset: usize) -> Option<(usize, usize)> {
    let lf = offset + buf[offset..].iter().position(|&b| b == b'\n')?;
    let text_end = if lf > offset && buf[lf - 1] == b'\r' {
        lf - 1
    } else {
        lf
    };
    Some((text_end, lf + 1))
}

pub(super) fn extract_command_args(
    buf: &mut BytesMut,
    eol_idx: usize,
    command_with_space: &[u8],
) -> Result<Bytes, ProtocolError> {
    let mut line = buf.split_to(eol_idx + 1).freeze();
    if line.ends_with(b"\r\n") {
        line.truncate(line.len() - 2);
    } else {
        line.truncate(line.len() - 1);
    }
    if line.starts_with(command_with_space) {
        Ok(line.slice(command_with_space.len()..))
    } else {
        Err(ProtocolError::Malformed("missing arguments"))
    }
}

pub(super) struct StorageRequest {
    pub key: Bytes,
    pub flags: u32,
    pub exptime: i32,
    pub data: Bytes,
}

pub(super) fn parse_storage_request(
    buf: &mut BytesMut,
    eol_idx: usize,
    line_text_end: usize,
    command_with_space: &[u8],
    header_help: &'static str,
    extra_token_msg: &'static str,
) -> Result<Option<StorageRequest>, ProtocolError> {
    let header = match parse_storage_header(
        &buf[..line_text_end],
        command_with_space,
        header_help,
        extra_token_msg,
    ) {
        Ok(h) => h,
        Err(e) => {
            let _ = buf.split_to(eol_idx + 1);
            return Err(e);
        }
    };

    let data_start = eol_idx + 1;
    let data_end = data_start
        .checked_add(header.bytes_count)
        .ok_or(ProtocolError::Malformed("body length overflow"))?;
    let terminator_len = match body_terminator_len(buf, data_end) {
        Ok(Some(len)) => len,
        Ok(None) => return Ok(None),
        Err(e) => {
            let _ = buf.split_to(data_end + 1);
            return Err(e);
        }
    };

    let total = data_end + terminator_len;
    let frozen = buf.split_to(total).freeze();
    let data = frozen.slice(data_start..data_end);
    Ok(Some(StorageRequest {
        key: header.key,
        flags: header.flags,
        exptime: header.exptime,
        data,
    }))
}

struct StorageHeader {
    key: Bytes,
    flags: u32,
    exptime: i32,
    bytes_count: usize,
}

fn parse_storage_header(
    header: &[u8],
    command_with_space: &[u8],
    header_help: &'static str,
    extra_token_msg: &'static str,
) -> Result<StorageHeader, ProtocolError> {
    let after_cmd = header
        .strip_prefix(command_with_space)
        .ok_or(ProtocolError::Malformed("missing arguments"))?;

    let mut parts = after_cmd.split(|&b| b == b' ').filter(|s| !s.is_empty());
    let key = parts.next().ok_or(ProtocolError::Malformed(header_help))?;
    let flags_bytes = parts.next().ok_or(ProtocolError::Malformed(header_help))?;
    let exptime_bytes = parts.next().ok_or(ProtocolError::Malformed(header_help))?;
    let bytes_bytes = parts.next().ok_or(ProtocolError::Malformed(header_help))?;

    if let Some(extra) = parts.next() {
        return Err(if extra == b"noreply" {
            ProtocolError::Malformed("noreply not yet supported")
        } else {
            ProtocolError::Malformed(extra_token_msg)
        });
    }

    validate_key(key)?;
    Ok(StorageHeader {
        key: Bytes::copy_from_slice(key),
        flags: parse_u32(flags_bytes)?,
        exptime: parse_i32(exptime_bytes)?,
        bytes_count: parse_usize(bytes_bytes)?,
    })
}

pub(super) fn body_terminator_len(
    buf: &[u8],
    data_end: usize,
) -> Result<Option<usize>, ProtocolError> {
    if buf.len() <= data_end {
        return Ok(None);
    }
    match buf[data_end] {
        b'\n' => Ok(Some(1)),
        b'\r' => {
            if buf.len() < data_end + 2 {
                Ok(None)
            } else if buf[data_end + 1] != b'\n' {
                Err(ProtocolError::Malformed("missing LF after CR in body terminator"))
            } else {
                Ok(Some(2))
            }
        }
        _ => Err(ProtocolError::Malformed("missing CRLF after body")),
    }
}

pub(super) fn validate_key(key: &[u8]) -> Result<(), ProtocolError> {
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

pub(super) fn parse_u32(s: &[u8]) -> Result<u32, ProtocolError> {
    from_utf8(s)
        .ok()
        .and_then(|s| s.parse::<u32>().ok())
        .ok_or(ProtocolError::Malformed("invalid u32"))
}

pub(super) fn parse_i32(s: &[u8]) -> Result<i32, ProtocolError> {
    from_utf8(s)
        .ok()
        .and_then(|s| s.parse::<i32>().ok())
        .ok_or(ProtocolError::Malformed("invalid i32"))
}

pub(super) fn parse_usize(s: &[u8]) -> Result<usize, ProtocolError> {
    from_utf8(s)
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .ok_or(ProtocolError::Malformed("invalid usize"))
}

pub(super) fn parse_u64(s: &[u8]) -> Result<u64, ProtocolError> {
    from_utf8(s)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .ok_or(ProtocolError::Malformed("invalid u64"))
}

#[cfg(test)]
mod tests {
    use super::*;

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
            b"\x0Bfoo",
            b"foo\x0C",
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
