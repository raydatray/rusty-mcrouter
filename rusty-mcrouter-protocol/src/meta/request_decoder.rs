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
