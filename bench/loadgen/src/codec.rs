use bytes::{Buf, BytesMut};
use thiserror::Error;

pub const MAX_REPLY_FRAME_BYTES: usize = 1024 * 1024 + 32 * 1024;
const MAX_REPLY_LINE_BYTES: usize = 32 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplyCode {
    Value,
    Hit,
    End,
    NotFound,
    NotStored,
    Exists,
    Error,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum DecodeError {
    #[error("Meta reply line exceeds the {maximum}-byte limit")]
    LineTooLarge { maximum: usize },
    #[error("Meta reply frame exceeds the {maximum}-byte limit")]
    FrameTooLarge { maximum: usize },
    #[error("Meta reply contains a bare newline")]
    BareNewline,
    #[error("Meta VA reply is missing a decimal body length")]
    MissingValueLength,
    #[error("Meta VA reply has an invalid body length")]
    InvalidValueLength,
    #[error("Meta VA reply body is not followed by CRLF")]
    InvalidValueTerminator,
    #[error("malformed Meta reply line")]
    MalformedLine,
    #[error("unknown Meta reply status `{0}`")]
    UnknownStatus(String),
}

#[derive(Debug, Default)]
pub struct Decoder;

impl Decoder {
    pub const fn new() -> Self {
        Self
    }

    pub fn decode(&self, src: &mut BytesMut) -> Result<Option<ReplyCode>, DecodeError> {
        let line_end = match find_line(src)? {
            Some(end) => end,
            None => return Ok(None),
        };
        let line_frame_len = line_end + 2;
        let line = &src[..line_end];
        validate_line(line)?;

        if line.starts_with(b"VA") {
            if !line.starts_with(b"VA ") {
                return Err(DecodeError::MissingValueLength);
            }
            let length = parse_value_length(line)?;
            let frame_len = line_frame_len
                .checked_add(length)
                .and_then(|len| len.checked_add(2))
                .ok_or(DecodeError::FrameTooLarge {
                    maximum: MAX_REPLY_FRAME_BYTES,
                })?;
            if frame_len > MAX_REPLY_FRAME_BYTES {
                return Err(DecodeError::FrameTooLarge {
                    maximum: MAX_REPLY_FRAME_BYTES,
                });
            }
            if src.len() < frame_len {
                return Ok(None);
            }
            if &src[frame_len - 2..frame_len] != b"\r\n" {
                return Err(DecodeError::InvalidValueTerminator);
            }
            src.advance(frame_len);
            return Ok(Some(ReplyCode::Value));
        }

        let status_end = line
            .iter()
            .position(|byte| *byte == b' ')
            .unwrap_or(line.len());
        let status = &line[..status_end];
        let code = match status {
            b"HD" => ReplyCode::Hit,
            b"EN" => ReplyCode::End,
            b"NF" => ReplyCode::NotFound,
            b"NS" => ReplyCode::NotStored,
            b"EX" => ReplyCode::Exists,
            b"ERROR" if line == b"ERROR" => ReplyCode::Error,
            b"CLIENT_ERROR" | b"SERVER_ERROR" => ReplyCode::Error,
            _ => {
                return Err(DecodeError::UnknownStatus(
                    String::from_utf8_lossy(status).into_owned(),
                ));
            }
        };
        src.advance(line_frame_len);
        Ok(Some(code))
    }
}

fn find_line(src: &[u8]) -> Result<Option<usize>, DecodeError> {
    if let Some(newline) = src.iter().position(|byte| *byte == b'\n') {
        if newline == 0 || src[newline - 1] != b'\r' {
            return Err(DecodeError::BareNewline);
        }
        let line_end = newline - 1;
        if newline + 1 > MAX_REPLY_LINE_BYTES {
            return Err(DecodeError::LineTooLarge {
                maximum: MAX_REPLY_LINE_BYTES,
            });
        }
        return Ok(Some(line_end));
    }
    if src.len() >= MAX_REPLY_LINE_BYTES {
        return Err(DecodeError::LineTooLarge {
            maximum: MAX_REPLY_LINE_BYTES,
        });
    }
    Ok(None)
}

fn validate_line(line: &[u8]) -> Result<(), DecodeError> {
    if line.is_empty()
        || line.first() == Some(&b' ')
        || line.last() == Some(&b' ')
        || line.iter().any(|byte| !matches!(byte, b' '..=b'~'))
    {
        return Err(DecodeError::MalformedLine);
    }
    Ok(())
}

fn parse_value_length(line: &[u8]) -> Result<usize, DecodeError> {
    let rest = &line[3..];
    let end = rest
        .iter()
        .position(|byte| *byte == b' ')
        .unwrap_or(rest.len());
    let raw = &rest[..end];
    if raw.is_empty() {
        return Err(DecodeError::MissingValueLength);
    }
    if raw.iter().any(|byte| !byte.is_ascii_digit()) {
        return Err(DecodeError::InvalidValueLength);
    }
    let mut value = 0usize;
    for digit in raw {
        value = value
            .checked_mul(10)
            .and_then(|current| current.checked_add(usize::from(*digit - b'0')))
            .ok_or(DecodeError::InvalidValueLength)?;
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_incremental_value_and_following_reply() {
        let input = b"VA 5 f7\r\na\0b\nc\r\nEN\r\n";
        for split in 0..=input.len() {
            let mut src = BytesMut::from(&input[..split]);
            let decoder = Decoder::new();
            let first = decoder.decode(&mut src).unwrap();
            if split < 16 {
                assert_eq!(first, None, "split={split}");
                src.extend_from_slice(&input[split..]);
                assert_eq!(decoder.decode(&mut src).unwrap(), Some(ReplyCode::Value));
            } else {
                assert_eq!(first, Some(ReplyCode::Value), "split={split}");
                src.extend_from_slice(&input[split..]);
            }
            assert_eq!(decoder.decode(&mut src).unwrap(), Some(ReplyCode::End));
            assert!(src.is_empty());
        }
    }

    #[test]
    fn rejects_bad_value_terminator_without_consuming() {
        let mut src = BytesMut::from(&b"VA 3\r\nabc\nX"[..]);
        assert_eq!(
            Decoder::new().decode(&mut src),
            Err(DecodeError::InvalidValueTerminator)
        );
        assert_eq!(src, b"VA 3\r\nabc\nX".as_slice());
    }

    #[test]
    fn rejects_malformed_lengths_and_lines() {
        for (input, expected) in [
            (b"VA nope\r\n".as_slice(), DecodeError::InvalidValueLength),
            (b"VA\r\n".as_slice(), DecodeError::MissingValueLength),
            (b"HD\n".as_slice(), DecodeError::BareNewline),
            (b" HD\r\n".as_slice(), DecodeError::MalformedLine),
        ] {
            let mut src = BytesMut::from(input);
            assert_eq!(Decoder::new().decode(&mut src), Err(expected));
        }
    }

    #[test]
    fn enforces_frame_cap_before_waiting_for_body() {
        let input = format!("VA {}\r\n", MAX_REPLY_FRAME_BYTES);
        let mut src = BytesMut::from(input.as_bytes());
        assert_eq!(
            Decoder::new().decode(&mut src),
            Err(DecodeError::FrameTooLarge {
                maximum: MAX_REPLY_FRAME_BYTES
            })
        );
    }
}
