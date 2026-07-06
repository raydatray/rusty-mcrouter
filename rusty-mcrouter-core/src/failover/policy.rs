use rusty_mcrouter_protocol::Request;

pub trait FailoverPolicy: 'static {
    fn failover_order(&self, req: &Request, n: usize) -> Vec<usize>;

    fn record_outcome(&self, child: usize, failed: bool) {
        let _ = (child, failed);
    }
}

pub struct InOrderPolicy;

impl FailoverPolicy for InOrderPolicy {
    fn failover_order(&self, _req: &Request, n: usize) -> Vec<usize> {
        (1..n).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn req() -> Request {
        Request::Get {
            key: Bytes::from_static(b"k"),
        }
    }

    #[test]
    fn in_order_yields_backups_in_config_order() {
        assert_eq!(InOrderPolicy.failover_order(&req(), 4), vec![1, 2, 3]);
    }

    #[test]
    fn in_order_single_child_has_no_backups() {
        assert_eq!(InOrderPolicy.failover_order(&req(), 1), Vec::<usize>::new());
    }

    #[test]
    fn in_order_record_outcome_does_not_change_order() {
        let policy = InOrderPolicy;
        policy.record_outcome(1, true);
        policy.record_outcome(2, false);
        assert_eq!(policy.failover_order(&req(), 3), vec![1, 2]);
    }
}
