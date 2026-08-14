use rusty_mcrouter_config::FailoverErrorKind;
use rusty_mcrouter_net::classify::{reply_code, ResultCode};
use rusty_mcrouter_protocol::{Reply, Request};

use crate::routes::{Result, RouteError};

/// Projects a route-level result onto the canonical semantic code — the
/// route layer's ONE surface-spanning match. `None` means an internal bug
/// (e.g. selector out of range): surface it, never retry it.
pub(crate) fn route_code(result: &Result<Reply>) -> Option<ResultCode> {
    match result {
        Ok(reply) => Some(reply_code(reply)),
        Err(RouteError::Backend(err)) => Some(err.code()),
        Err(RouteError::SelectorOutOfRange { .. }) => None,
    }
}

/// Config vocabulary -> canonical code. The config crate stays
/// net-independent, so its enum mirrors ResultCode names and the mapping
/// lives here, at route-build time.
pub(crate) fn code_of_kind(kind: FailoverErrorKind) -> ResultCode {
    match kind {
        FailoverErrorKind::Timeout => ResultCode::Timeout,
        FailoverErrorKind::ConnectTimeout => ResultCode::ConnectTimeout,
        FailoverErrorKind::ConnectError => ResultCode::ConnectError,
        FailoverErrorKind::RemoteError => ResultCode::RemoteError,
        FailoverErrorKind::ConnectionDropped => ResultCode::ConnectionDropped,
        FailoverErrorKind::LocalError => ResultCode::LocalError,
        FailoverErrorKind::Tko => ResultCode::Tko,
    }
}

/// Per-op failover eligibility. `None` = the default failover set
/// (mcrouter McResUtil.h:78, via ResultCode::is_failover_error);
/// `Some(vec![])` is the idempotency lever that blocks failover entirely.
#[derive(Debug, Default)]
pub struct FailoverErrors {
    gets: Option<Vec<ResultCode>>,
    updates: Option<Vec<ResultCode>>,
    deletes: Option<Vec<ResultCode>>,
}

impl FailoverErrors {
    pub(crate) fn new(
        gets: Option<Vec<ResultCode>>,
        updates: Option<Vec<ResultCode>>,
        deletes: Option<Vec<ResultCode>>,
    ) -> Self {
        Self {
            gets,
            updates,
            deletes,
        }
    }

    pub(crate) fn should_failover(&self, req: &Request, result: &Result<Reply>) -> bool {
        let Some(code) = route_code(result) else {
            return false;
        };
        let custom = match req {
            Request::Get(_) => self.gets.as_deref(),
            Request::Store(_) => self.updates.as_deref(),
            Request::Delete(_) => self.deletes.as_deref(),
            // Arithmetic is not idempotent and debug is diagnostic; neither
            // takes a per-op override, so both use the default set.
            Request::Arithmetic(_) | Request::Debug(_) => None,
        };
        match custom {
            None => code.is_failover_error(),
            Some(codes) => codes.contains(&code),
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use rusty_mcrouter_net::error::{ConnectError, LocalError, RequestError, SendError};
    use rusty_mcrouter_protocol::reply::{
        ArithmeticReply, ArithmeticResult, DeleteReply, ErrorReply, GetReply, StoreReply,
        StoreResult,
    };
    use rusty_mcrouter_protocol::test_support::{delete, get, store};

    use super::*;

    fn backend(err: SendError) -> Result<Reply> {
        Err(RouteError::Backend(err))
    }

    fn timeout() -> Result<Reply> {
        backend(SendError::Request(RequestError::Timeout { sent: true }))
    }

    fn server_error() -> Result<Reply> {
        Ok(Reply::Error(ErrorReply::Server(Some(Bytes::from_static(
            b"boom",
        )))))
    }

    fn miss() -> Result<Reply> {
        Ok(Reply::Get(GetReply::Miss))
    }

    #[test]
    fn both_surfaces_are_failover_errors_by_default() {
        let cases = [
            timeout(),
            backend(SendError::Connect(ConnectError::Timeout)),
            backend(SendError::Connect(ConnectError::Failed(
                std::io::ErrorKind::ConnectionRefused,
            ))),
            backend(SendError::Request(RequestError::Dropped {
                kind: std::io::ErrorKind::ConnectionReset,
            })),
            backend(SendError::Local(LocalError::QueueFull)),
            backend(SendError::Tko {
                reason: ResultCode::Timeout,
            }),
            server_error(),
        ];
        let errors = FailoverErrors::default();
        for case in &cases {
            assert!(
                errors.should_failover(&get(b"k"), case),
                "expected failover for {case:?}"
            );
        }
    }

    #[test]
    fn valid_replies_and_internal_conditions_are_not_failover_errors() {
        let cases = [
            miss(),
            Ok(Reply::Delete(DeleteReply::NotFound)),
            Ok(Reply::Store(StoreReply::Success(StoreResult::default()))),
            Ok(Reply::Arithmetic(ArithmeticReply::Success(
                ArithmeticResult {
                    value: Some(1),
                    ..ArithmeticResult::default()
                },
            ))),
            // our-fault replies would fail identically on every box
            Ok(Reply::Error(ErrorReply::Error)),
            Ok(Reply::Error(ErrorReply::Client(Some(Bytes::from_static(
                b"bad",
            ))))),
            Err(RouteError::SelectorOutOfRange { idx: 3, len: 2 }),
        ];
        let errors = FailoverErrors::default();
        for case in &cases {
            assert!(
                !errors.should_failover(&get(b"k"), case),
                "expected no failover for {case:?}"
            );
        }
    }

    #[test]
    fn route_code_projects_both_surfaces() {
        assert_eq!(route_code(&timeout()), Some(ResultCode::Timeout));
        assert_eq!(route_code(&server_error()), Some(ResultCode::RemoteError));
        assert_eq!(route_code(&miss()), Some(ResultCode::Success));
        assert_eq!(
            route_code(&Err(RouteError::SelectorOutOfRange { idx: 1, len: 1 })),
            None
        );
    }

    #[test]
    fn config_kinds_map_one_to_one() {
        let table = [
            (FailoverErrorKind::Timeout, ResultCode::Timeout),
            (FailoverErrorKind::ConnectTimeout, ResultCode::ConnectTimeout),
            (FailoverErrorKind::ConnectError, ResultCode::ConnectError),
            (FailoverErrorKind::RemoteError, ResultCode::RemoteError),
            (
                FailoverErrorKind::ConnectionDropped,
                ResultCode::ConnectionDropped,
            ),
            (FailoverErrorKind::LocalError, ResultCode::LocalError),
            (FailoverErrorKind::Tko, ResultCode::Tko),
        ];
        for (kind, code) in table {
            assert_eq!(code_of_kind(kind), code);
        }
    }

    #[test]
    fn default_uses_the_failover_set_for_every_op() {
        let errors = FailoverErrors::default();
        assert!(errors.should_failover(&get(b"k"), &timeout()));
        assert!(errors.should_failover(&store(b"k", b"v"), &timeout()));
        assert!(errors.should_failover(&delete(b"k"), &timeout()));
        assert!(!errors.should_failover(&get(b"k"), &miss()));
    }

    #[test]
    fn empty_updates_list_blocks_write_failover_but_not_reads() {
        let errors = FailoverErrors::new(None, Some(vec![]), None);
        assert!(
            !errors.should_failover(&store(b"k", b"v"), &timeout()),
            "a set timeout must not fail over"
        );
        assert!(
            errors.should_failover(&get(b"k"), &timeout()),
            "a get timeout still fails over under the default"
        );
    }

    #[test]
    fn explicit_list_matches_only_named_codes() {
        let errors = FailoverErrors::new(Some(vec![ResultCode::RemoteError]), None, None);
        assert!(errors.should_failover(&get(b"k"), &server_error()));
        assert!(!errors.should_failover(&get(b"k"), &timeout()));
    }
}
