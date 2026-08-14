use rusty_mcrouter_protocol::{reply::ErrorReply, Reply};

use crate::error::{ConnectError, RequestError, SendError};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum ResultCode {
    /// hit, miss, stored, deleted, etc. the box answered authoritatively -
    /// only transport/health failures are errors
    Success = 0,
    /// ERROR / CLIENT_ERROR - we messed up, any box would fail
    BadRequest,
    /// SERVER_ERROR - box answered that its broken
    RemoteError,
    /// mid-stream connection drop with this request in-flight - the request
    /// was sent but MAY OR MAY NOT have executed (contrast RemoteError, where
    /// the server definitively answered). mcrouter folds this into
    /// REMOTE_ERROR (a carbon-vocabulary limitation); we keep it distinct so
    /// failover config and stats can tell "server rejected" from "maybe
    /// executed" - the policy predicates below still treat both like mcrouter
    ConnectionDropped,
    /// never reached the server or the reply stream is busted
    LocalError,
    /// connect(2) failed - hard TKO
    ConnectError,
    /// connect() timed out - hard TKO
    ConnectTimeout,
    /// no reply in time - soft TKO
    Timeout,
    /// synthetic fail-fast, the destination was already marked TKO
    Tko,
}

pub const RESULT_CODE_COUNT: usize = 9;

pub fn code_of(result: &Result<Reply, SendError>) -> ResultCode {
    match result {
        Ok(reply) => reply_code(reply),
        Err(err) => err.code(),
    }
}

pub fn reply_code(reply: &Reply) -> ResultCode {
    match reply {
        Reply::Error(ErrorReply::Server(_)) => ResultCode::RemoteError,
        Reply::Error(ErrorReply::Client(_)) | Reply::Error(ErrorReply::Error) => {
            ResultCode::BadRequest
        }
        _ => ResultCode::Success,
    }
}

impl SendError {
    pub fn code(&self) -> ResultCode {
        match self {
            SendError::Local(_) | SendError::Protocol(_) => ResultCode::LocalError,
            SendError::Connect(ConnectError::Timeout) => ResultCode::ConnectTimeout,
            SendError::Connect(ConnectError::Failed(_)) => ResultCode::ConnectError,
            SendError::Request(RequestError::Timeout { .. }) => ResultCode::Timeout,
            SendError::Request(RequestError::Dropped { .. }) => ResultCode::ConnectionDropped,
            SendError::Tko { .. } => ResultCode::Tko,
        }
    }
}

impl ResultCode {
    pub fn is_error(self) -> bool {
        self != ResultCode::Success
    }

    pub fn is_soft_tko_error(self) -> bool {
        self == ResultCode::Timeout
    }

    pub fn is_hard_tko_error(self) -> bool {
        matches!(self, ResultCode::ConnectError | ResultCode::ConnectTimeout)
    }

    pub fn is_tko_or_hard_tko(self) -> bool {
        self == ResultCode::Tko || self.is_hard_tko_error()
    }

    pub fn is_failover_error(self) -> bool {
        matches!(
            self,
            ResultCode::Tko
                | ResultCode::ConnectError
                | ResultCode::ConnectTimeout
                | ResultCode::Timeout
                | ResultCode::RemoteError
                | ResultCode::ConnectionDropped
                | ResultCode::LocalError
        )
    }

    pub fn from_config_name(name: &str) -> Option<ResultCode> {
        Some(match name {
            "timeout" => ResultCode::Timeout,
            "connect_timeout" => ResultCode::ConnectTimeout,
            "connect_error" => ResultCode::ConnectError,
            "remote_error" | "server_error" => ResultCode::RemoteError,
            "connection_dropped" => ResultCode::ConnectionDropped,
            "local_error" => ResultCode::LocalError,
            "tko" => ResultCode::Tko,
            _ => return None,
        })
    }

    /// Inverse of `as u8`; the TkoTracker stores the marking reason in an
    /// AtomicU8 and round-trips it through here.
    pub(crate) fn from_u8(v: u8) -> ResultCode {
        match v {
            1 => ResultCode::BadRequest,
            2 => ResultCode::RemoteError,
            3 => ResultCode::ConnectionDropped,
            4 => ResultCode::LocalError,
            5 => ResultCode::ConnectError,
            6 => ResultCode::ConnectTimeout,
            7 => ResultCode::Timeout,
            8 => ResultCode::Tko,
            _ => ResultCode::Success,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use bytes::Bytes;
    use rusty_mcrouter_protocol::meta::{MetaReplyDecodeError, MetaRequestEncodeError};
    use rusty_mcrouter_protocol::reply::{DeleteReply, ErrorReply};
    use rusty_mcrouter_protocol::Reply;

    use super::*;
    use crate::error::{LocalError, ProtocolError};

    const ALL: [ResultCode; RESULT_CODE_COUNT] = [
        ResultCode::Success,
        ResultCode::BadRequest,
        ResultCode::RemoteError,
        ResultCode::ConnectionDropped,
        ResultCode::LocalError,
        ResultCode::ConnectError,
        ResultCode::ConnectTimeout,
        ResultCode::Timeout,
        ResultCode::Tko,
    ];

    /// The full SendError -> ResultCode classification table. Every
    /// constructible error shape appears exactly once; if a variant is added
    /// this test won't cover it, but the exhaustive match in `code()` will
    /// refuse to compile until it is classified.
    #[test]
    fn send_error_classification_table() {
        let table: &[(SendError, ResultCode)] = &[
            // local phase: never left this process
            (
                SendError::Local(LocalError::Encode(MetaRequestEncodeError::EmptyBackendKey)),
                ResultCode::LocalError,
            ),
            (
                SendError::Local(LocalError::QueueFull),
                ResultCode::LocalError,
            ),
            (
                SendError::Local(LocalError::Shutdown),
                ResultCode::LocalError,
            ),
            // connect phase: hard TKO evidence
            (
                SendError::Connect(ConnectError::Failed(io::ErrorKind::ConnectionRefused)),
                ResultCode::ConnectError,
            ),
            (
                SendError::Connect(ConnectError::Timeout),
                ResultCode::ConnectTimeout,
            ),
            // request phase: connection was up
            (
                SendError::Request(RequestError::Timeout { sent: false }),
                ResultCode::Timeout,
            ),
            (
                SendError::Request(RequestError::Timeout { sent: true }),
                ResultCode::Timeout,
            ),
            (
                SendError::Request(RequestError::Dropped {
                    kind: io::ErrorKind::ConnectionReset,
                }),
                ResultCode::ConnectionDropped,
            ),
            // protocol phase: poisoned stream (LocalError by design; the TKO
            // verdict travels via the connection-down event, not this code)
            (
                SendError::Protocol(ProtocolError::Decode(MetaReplyDecodeError::UnexpectedEof)),
                ResultCode::LocalError,
            ),
            (
                SendError::Protocol(ProtocolError::Desync("reply bytes with no pending request")),
                ResultCode::LocalError,
            ),
            // synthetic fast-fail
            (
                SendError::Tko {
                    reason: ResultCode::Timeout,
                },
                ResultCode::Tko,
            ),
        ];
        for (err, expected) in table {
            assert_eq!(err.code(), *expected, "for {err:?}");
        }
    }

    #[test]
    fn reply_classification_table() {
        let table: &[(Reply, ResultCode)] = &[
            (
                Reply::Error(ErrorReply::Server(None)),
                ResultCode::RemoteError,
            ),
            (
                Reply::Error(ErrorReply::Server(Some(Bytes::from_static(b"oom")))),
                ResultCode::RemoteError,
            ),
            (
                Reply::Error(ErrorReply::Client(None)),
                ResultCode::BadRequest,
            ),
            (Reply::Error(ErrorReply::Error), ResultCode::BadRequest),
            // authoritative answers are success - including NotFound
            (Reply::Delete(DeleteReply::Success), ResultCode::Success),
            (Reply::Delete(DeleteReply::NotFound), ResultCode::Success),
            (
                Reply::Version(Bytes::from_static(b"1.6.39")),
                ResultCode::Success,
            ),
        ];
        for (reply, expected) in table {
            assert_eq!(reply_code(reply), *expected, "for {reply:?}");
            assert_eq!(code_of(&Ok(reply.clone())), *expected);
        }
    }

    /// from_u8 is the inverse of `as u8` for every variant (the TkoTracker
    /// reason slot round-trips through an AtomicU8).
    #[test]
    fn from_u8_round_trips() {
        for code in ALL {
            assert_eq!(ResultCode::from_u8(code as u8), code);
        }
        // out-of-range collapses to Success (never a stale TKO reason)
        assert_eq!(ResultCode::from_u8(200), ResultCode::Success);
    }

    /// RESULT_CODE_COUNT indexes a stats array; every discriminant must fit.
    #[test]
    fn result_code_count_covers_all_discriminants() {
        for code in ALL {
            assert!((code as usize) < RESULT_CODE_COUNT, "for {code:?}");
        }
    }

    /// The mcrouter McResUtil.h policy sets, as tables.
    #[test]
    fn tko_and_failover_predicate_sets() {
        for code in ALL {
            // soft == { Timeout } (McResUtil.h:96)
            assert_eq!(
                code.is_soft_tko_error(),
                code == ResultCode::Timeout,
                "for {code:?}"
            );
            // hard == { ConnectError, ConnectTimeout } (McResUtil.h:107)
            assert_eq!(
                code.is_hard_tko_error(),
                matches!(code, ResultCode::ConnectError | ResultCode::ConnectTimeout),
                "for {code:?}"
            );
            // a code is never both soft and hard
            assert!(
                !(code.is_soft_tko_error() && code.is_hard_tko_error()),
                "for {code:?}"
            );
            // free failover tries == hard TKO + synthetic Tko (McResUtil.h:136)
            assert_eq!(
                code.is_tko_or_hard_tko(),
                code == ResultCode::Tko || code.is_hard_tko_error(),
                "for {code:?}"
            );
            // default failover set == everything except Success (server
            // verdict stands) and BadRequest (our fault on every box)
            assert_eq!(
                code.is_failover_error(),
                !matches!(code, ResultCode::Success | ResultCode::BadRequest),
                "for {code:?}"
            );
            assert_eq!(code.is_error(), code != ResultCode::Success, "for {code:?}");
        }
    }

    #[test]
    fn config_names_parse() {
        let table: &[(&str, ResultCode)] = &[
            ("timeout", ResultCode::Timeout),
            ("connect_timeout", ResultCode::ConnectTimeout),
            ("connect_error", ResultCode::ConnectError),
            ("remote_error", ResultCode::RemoteError),
            ("server_error", ResultCode::RemoteError),
            ("connection_dropped", ResultCode::ConnectionDropped),
            ("local_error", ResultCode::LocalError),
            ("tko", ResultCode::Tko),
        ];
        for (name, expected) in table {
            assert_eq!(
                ResultCode::from_config_name(name),
                Some(*expected),
                "for {name:?}"
            );
        }
        for bad in ["", "Timeout", "success", "bad_request", "busy", "shutdown"] {
            assert_eq!(ResultCode::from_config_name(bad), None, "for {bad:?}");
        }
    }
}
