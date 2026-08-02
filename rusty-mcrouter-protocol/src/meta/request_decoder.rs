use bytes::{Buf, Bytes, BytesMut};
use thiserror::Error;

use crate::reply::ErrorReply;
use crate::{meta::MetaReplyPlan, request::Request};

pub const MAX_COMMAND_LINE_BYTES: usize = 32 * 1024;
pub const MAX_VALUE_BYTES: usize = 1024 * 1024;
pub const MAX_FLAGS: usize = 64;
pub const MAX_OPAQUE_BYTES: usize = 31;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DecodedMetaCommand {
    Request {
        request: Request,
        reply_plan: MetaReplyPlan,
    },
    NoOp, // mn
}

#[derive(Debug, Default)]
pub struct MetaRequestDecoder;

impl MetaRequestDecoder {
    pub const fn new() -> Self {
        Self
    }

    /// decodes at most one complete Meta command.
    ///
    /// `Ok(None)` leaves an incomplete frame untouched. a recoverable error
    /// consumes exactly one complete command, while a fatal error requires the
    /// session to close the connection.
    pub fn decode(
        &mut self,
        src: &mut BytesMut,
    ) -> Result<Option<DecodedMetaCommand>, MetaRequestDecodeError> {
        let Some(newline) = src.iter().position(|byte| *byte == b'\n') else {
            if src.len() > MAX_COMMAND_LINE_BYTES {
                return Err(FatalDecodeError::FrameTooLarge {
                    maximum: MAX_COMMAND_LINE_BYTES,
                }
                .into());
            }
            return Ok(None);
        };

        let frame_len = newline + 1;
        if frame_len > MAX_COMMAND_LINE_BYTES {
            return Err(FatalDecodeError::FrameTooLarge {
                maximum: MAX_COMMAND_LINE_BYTES,
            }
            .into());
        }

        let line_end = if newline > 0 && src[newline - 1] == b'\r' {
            newline - 1
        } else {
            newline
        };
        let line = &src[..line_end];

        let result = if line == b"mn" {
            Ok(Some(DecodedMetaCommand::NoOp))
        } else if line.starts_with(b"mn ") {
            Err(recoverable_client_error(b"bad command line format"))
        } else {
            Err(MetaRequestDecodeError::Recoverable(ErrorReply::Error))
        };

        src.advance(frame_len);
        result
    }

    pub fn decode_eof(&self, src: &BytesMut) -> Result<(), MetaRequestDecodeError> {
        if src.is_empty() {
            Ok(())
        } else {
            Err(FatalDecodeError::UnexpectedEof.into())
        }
    }
}

fn recoverable_client_error(message: &'static [u8]) -> MetaRequestDecodeError {
    MetaRequestDecodeError::Recoverable(ErrorReply::Client(Some(Bytes::from_static(message))))
}

/// an error produced while incrementally decoding a frontend Meta command
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum MetaRequestDecodeError {
    /// one complete malformed command was consumed. the session should encode
    /// this reply and may continue decoding the connection.
    #[error("recoverable Meta request error")]
    Recoverable(ErrorReply),

    /// frame alignment is not trustworthy. the session must close the
    /// connection rather than attempt to decode another command.
    #[error(transparent)]
    Fatal(#[from] FatalDecodeError),
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum FatalDecodeError {
    #[error("Meta frame exceeds the {maximum}-byte limit")]
    FrameTooLarge { maximum: usize },

    #[error("connection ended with a partial Meta frame")]
    UnexpectedEof,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incomplete_line_is_left_untouched() {
        let mut decoder = MetaRequestDecoder::new();
        let mut src = BytesMut::from(&b"mn"[..]);

        assert_eq!(decoder.decode(&mut src), Ok(None));
        assert_eq!(src, b"mn".as_slice());
    }

    #[test]
    fn decodes_noop_with_lf_or_crlf() {
        for input in [b"mn\n".as_slice(), b"mn\r\n".as_slice()] {
            let mut decoder = MetaRequestDecoder::new();
            let mut src = BytesMut::from(input);

            assert_eq!(decoder.decode(&mut src), Ok(Some(DecodedMetaCommand::NoOp)));
            assert!(src.is_empty());
        }
    }

    #[test]
    fn consumes_one_pipelined_command_at_a_time() {
        let mut decoder = MetaRequestDecoder::new();
        let mut src = BytesMut::from(&b"mn\r\nmn\n"[..]);

        assert_eq!(decoder.decode(&mut src), Ok(Some(DecodedMetaCommand::NoOp)));
        assert_eq!(src, b"mn\n".as_slice());
        assert_eq!(decoder.decode(&mut src), Ok(Some(DecodedMetaCommand::NoOp)));
        assert!(src.is_empty());
    }

    #[test]
    fn malformed_noop_is_recoverable_and_consumed() {
        let mut decoder = MetaRequestDecoder::new();
        let mut src = BytesMut::from(&b"mn unexpected\r\nmn\r\n"[..]);

        assert_eq!(
            decoder.decode(&mut src),
            Err(recoverable_client_error(b"bad command line format"))
        );
        assert_eq!(src, b"mn\r\n".as_slice());
    }

    #[test]
    fn unknown_command_is_recoverable_and_consumed() {
        let mut decoder = MetaRequestDecoder::new();
        let mut src = BytesMut::from(&b"get key\r\nmn\r\n"[..]);

        assert_eq!(
            decoder.decode(&mut src),
            Err(MetaRequestDecodeError::Recoverable(ErrorReply::Error))
        );
        assert_eq!(src, b"mn\r\n".as_slice());
    }

    #[test]
    fn oversized_partial_line_is_fatal_and_untouched() {
        let mut decoder = MetaRequestDecoder::new();
        let mut src = BytesMut::from(vec![b'x'; MAX_COMMAND_LINE_BYTES + 1].as_slice());
        let original = src.clone();

        assert_eq!(
            decoder.decode(&mut src),
            Err(MetaRequestDecodeError::Fatal(
                FatalDecodeError::FrameTooLarge {
                    maximum: MAX_COMMAND_LINE_BYTES,
                }
            ))
        );
        assert_eq!(src, original);
    }

    #[test]
    fn oversized_complete_line_is_fatal_and_untouched() {
        let mut input = vec![b'x'; MAX_COMMAND_LINE_BYTES];
        input.push(b'\n');
        let mut decoder = MetaRequestDecoder::new();
        let mut src = BytesMut::from(input.as_slice());
        let original = src.clone();

        assert_eq!(
            decoder.decode(&mut src),
            Err(MetaRequestDecodeError::Fatal(
                FatalDecodeError::FrameTooLarge {
                    maximum: MAX_COMMAND_LINE_BYTES,
                }
            ))
        );
        assert_eq!(src, original);
    }

    #[test]
    fn eof_requires_an_empty_buffer() {
        let decoder = MetaRequestDecoder::new();

        assert_eq!(decoder.decode_eof(&BytesMut::new()), Ok(()));
        assert_eq!(
            decoder.decode_eof(&BytesMut::from(&b"mn"[..])),
            Err(MetaRequestDecodeError::Fatal(
                FatalDecodeError::UnexpectedEof
            ))
        );
    }
}
