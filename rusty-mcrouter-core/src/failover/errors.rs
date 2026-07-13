use rusty_mcrouter_config::FailoverErrorKind;
use rusty_mcrouter_net::NetError;
use rusty_mcrouter_protocol::{Reply, Request};

use crate::routes::{Result, RouteError};

fn classify(result: &Result<Reply>) -> Option<FailoverErrorKind> {
    match result {
        Err(RouteError::Backend(net)) => match net {
            NetError::Timeout { .. } => Some(FailoverErrorKind::Timeout),
            NetError::Io(_) => Some(FailoverErrorKind::Io),
            NetError::Protocol(_) => Some(FailoverErrorKind::Protocol),
            NetError::ClientClosed => Some(FailoverErrorKind::ClientClosed),
            NetError::NoAddresses | NetError::WorkerClosed { .. } => None,
        },
        Err(RouteError::SelectorOutOfRange { .. }) => None,
        Ok(Reply::ServerError(_)) => Some(FailoverErrorKind::ServerError),
        Ok(_) => None,
    }
}

fn is_failover_error(result: &Result<Reply>) -> bool {
    classify(result).is_some()
}

#[derive(Debug, Default)]
pub struct FailoverErrors {
    gets: Option<Vec<FailoverErrorKind>>,
    updates: Option<Vec<FailoverErrorKind>>,
    deletes: Option<Vec<FailoverErrorKind>>,
}

impl FailoverErrors {
    pub(crate) fn new(
        gets: Option<Vec<FailoverErrorKind>>,
        updates: Option<Vec<FailoverErrorKind>>,
        deletes: Option<Vec<FailoverErrorKind>>,
    ) -> Self {
        Self {
            gets,
            updates,
            deletes,
        }
    }

    pub(crate) fn should_failover(&self, req: &Request, result: &Result<Reply>) -> bool {
        let custom = match req {
            Request::Get { .. } => self.gets.as_deref(),
            Request::Set { .. }
            | Request::Add { .. }
            | Request::Replace { .. }
            | Request::Append { .. }
            | Request::Prepend { .. } => self.updates.as_deref(),
            Request::Delete { .. } => self.deletes.as_deref(),
            Request::Incr { .. } | Request::Decr { .. } | Request::Touch { .. } => None,
        };
        match custom {
            None => is_failover_error(result),
            Some(kinds) => classify(result).is_some_and(|k| kinds.contains(&k)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use rusty_mcrouter_net::TimeoutPhase;
    use rusty_mcrouter_protocol::ProtocolError;

    fn get() -> Request {
        Request::Get {
            key: Bytes::from_static(b"k"),
        }
    }

    fn set() -> Request {
        Request::Set {
            key: Bytes::from_static(b"k"),
            flags: 0,
            exptime: 0,
            data: Bytes::from_static(b"v"),
        }
    }

    fn delete() -> Request {
        Request::Delete {
            key: Bytes::from_static(b"k"),
        }
    }

    fn backend(err: NetError) -> Result<Reply> {
        Err(RouteError::Backend(err))
    }

    fn timeout() -> Result<Reply> {
        backend(NetError::Timeout {
            phase: TimeoutPhase::Reply,
        })
    }

    fn server_error() -> Result<Reply> {
        Ok(Reply::ServerError(Bytes::from_static(b"boom")))
    }

    fn miss() -> Result<Reply> {
        Ok(Reply::Get { hits: vec![] })
    }

    #[test]
    fn both_surfaces_are_failover_errors() {
        let cases = [
            backend(NetError::Timeout {
                phase: TimeoutPhase::Reply,
            }),
            backend(NetError::Timeout {
                phase: TimeoutPhase::Connect,
            }),
            backend(NetError::Io(std::io::Error::from(
                std::io::ErrorKind::BrokenPipe,
            ))),
            backend(NetError::Protocol(ProtocolError::Malformed("bad"))),
            backend(NetError::ClientClosed),
            server_error(),
        ];
        for case in &cases {
            assert!(is_failover_error(case), "expected failover for {case:?}");
        }
    }

    #[test]
    fn valid_replies_and_internal_conditions_are_not_failover_errors() {
        let cases = [
            miss(),
            Ok(Reply::NotFound),
            Ok(Reply::Stored),
            Ok(Reply::Numeric(1)),
            Ok(Reply::Error),
            Ok(Reply::ClientError(Bytes::from_static(b"bad"))),
            Err(RouteError::SelectorOutOfRange { idx: 3, len: 2 }),
            backend(NetError::NoAddresses),
            backend(NetError::WorkerClosed { worker: 0 }),
        ];
        for case in &cases {
            assert!(
                !is_failover_error(case),
                "expected no failover for {case:?}"
            );
        }
    }

    #[test]
    fn classify_maps_each_surface_to_its_kind() {
        assert_eq!(classify(&timeout()), Some(FailoverErrorKind::Timeout));
        assert_eq!(
            classify(&server_error()),
            Some(FailoverErrorKind::ServerError)
        );
        assert_eq!(classify(&miss()), None);
    }

    #[test]
    fn default_uses_the_built_in_classifier_for_every_op() {
        let errors = FailoverErrors::default();
        assert!(errors.should_failover(&get(), &timeout()));
        assert!(errors.should_failover(&set(), &timeout()));
        assert!(errors.should_failover(&delete(), &timeout()));
        assert!(!errors.should_failover(&get(), &miss()));
    }

    #[test]
    fn empty_updates_list_blocks_write_failover_but_not_reads() {
        let errors = FailoverErrors::new(None, Some(vec![]), None);
        assert!(
            !errors.should_failover(&set(), &timeout()),
            "a set timeout must not fail over"
        );
        assert!(
            errors.should_failover(&get(), &timeout()),
            "a get timeout still fails over under the default"
        );
    }

    #[test]
    fn explicit_list_matches_only_named_kinds() {
        let errors = FailoverErrors::new(Some(vec![FailoverErrorKind::ServerError]), None, None);
        assert!(errors.should_failover(&get(), &server_error()));
        assert!(!errors.should_failover(&get(), &timeout()));
    }
}
