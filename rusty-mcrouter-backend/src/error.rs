use std::io;

use rusty_mcrouter_protocol::meta::{MetaReplyDecodeError, MetaRequestEncodeError};
use thiserror::Error;

use crate::classify::ResultCode;

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum SendError {
    /// request never left this process - we messed up so no TKO effect
    #[error("local error: {0}")]
    Local(#[from] LocalError),

    /// could not establish a connection - the box is unreachable so HARD TKO
    #[error("connect error: {0}")]
    Connect(#[from] ConnectError),

    /// this request got no usable answer
    #[error("request error: {0}")]
    Request(#[from] RequestError),

    /// the reply stream is not trustworthy - kill it
    ///
    /// NOTE: classifies as ResultCode::LocalError, NOT RemoteError, even
    /// though the server sent the garbage. The request surface says "no
    /// trustworthy verdict on YOUR request" (still failover-eligible); the
    /// server-is-broken verdict travels separately via the connection
    /// teardown -> ConnEvent::Down -> hard TKO. Reclassifying to RemoteError
    /// would double-count the failure. Do not "fix" this.
    #[error("protocol error: {0}")]
    Protocol(#[from] ProtocolError),

    /// synthetic fast-fail - we never attempted send as the destination is
    /// marked TKO
    #[error("destination marked down (reason: {reason:?})")]
    Tko { reason: ResultCode },
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum LocalError {
    /// request is unrepresentable on wire. Fail only this request, the encoder
    /// will truncate its buffer back to the checkpoint
    #[error("encode: {0}")]
    Encode(#[from] MetaRequestEncodeError),

    /// bounded pending queue is full - fail fast and direct request to another
    /// backend if eligible. DELIBERATE: classifies as LocalError, which is in
    /// the default failover set - a saturated-but-healthy primary sheds
    /// overflow to failover children (faithful to mcrouter). If this fires in
    /// production, look at capacity, not this enum.
    #[error("pending queue full")]
    QueueFull,

    /// the destination is shutting down
    #[error("destination shut down")]
    Shutdown,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ConnectError {
    /// connect(2) failed (refused/reset/unreachable/whatever)
    #[error("connect failed: {0:?}")]
    Failed(io::ErrorKind),

    /// connect() did not complete within connect_timeout (after retries)
    #[error("connect timed out")]
    Timeout,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum RequestError {
    /// no reply within reply_timeout, sent is for idempotency
    #[error("timed out (sent: {sent})")]
    Timeout { sent: bool },

    /// the connection died with this request outstanding. Only inflight slots
    /// are failed as dropped
    #[error("connection dropped: {kind:?}")]
    Dropped { kind: io::ErrorKind },
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ProtocolError {
    /// the reply failed to parse - fatal to the connection
    #[error("decode: {0}")]
    Decode(#[from] MetaReplyDecodeError),

    /// reply stream out of step with request FIFO (such as reply bytes with no
    /// inflight request) - fatal to the connection
    #[error("desync: {0}")]
    Desync(&'static str),
}
