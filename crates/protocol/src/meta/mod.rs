//! The Meta protocol codec, split into the four roles a proxy hop needs:
//!
//! - [`MetaRequestDecoder`]: client bytes -> semantic [`Request`] + hop-local
//!   [`MetaReplyPlan`] (or a session-local no-op / recoverable error);
//! - [`MetaRequestEncoder`]: [`Request`] -> canonical backend bytes, plus the
//!   [`MetaReplyExpectation`] that disambiguates the eventual reply;
//! - [`MetaReplyDecoder`]: backend bytes + expectation -> typed [`Reply`];
//! - [`MetaReplyEncoder`]: [`Reply`] + [`MetaReplyPlan`] -> client bytes, in
//!   the client's requested token order.
//!
//! [`Request`]: crate::Request
//! [`Reply`]: crate::Reply

mod command;
mod reply_decoder;
mod reply_encoder;
mod reply_expectation;
mod reply_plan;
mod request_decoder;
mod request_encoder;
mod tokens;
mod wire;

pub use reply_decoder::{MetaReplyDecodeError, MetaReplyDecoder};
pub use reply_encoder::{MetaReplyEncodeError, MetaReplyEncoder};
pub use reply_expectation::{GetSuccessShape, MetaReplyExpectation};
pub use reply_plan::{
    KeyEncoding, MetaOutputOrder, MetaOutputToken, MetaQuietPolicy, MetaReplyPlan,
};
pub use request_decoder::{
    DecodedMetaCommand, FatalDecodeError, MetaRequestDecodeError, MetaRequestDecoder,
};
pub use request_encoder::{MetaRequestEncodeError, MetaRequestEncoder};
