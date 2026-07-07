use std::cell::RefCell;

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

pub struct LeastFailuresPolicy {
    max_tries: usize,
    failures: RefCell<Vec<u32>>,
}

impl LeastFailuresPolicy {
    pub fn new(n: usize, max_tries: usize) -> Self {
        Self {
            max_tries: max_tries.max(1),
            failures: RefCell::new(vec![0; n]),
        }
    }
}

impl FailoverPolicy for LeastFailuresPolicy {
    fn failover_order(&self, _req: &Request, n: usize) -> Vec<usize> {
        let failures = self.failures.borrow();
        let mut backups: Vec<usize> = (1..n).collect();
        backups.sort_by_key(|&i| failures.get(i).copied().unwrap_or(0));
        backups.truncate(self.max_tries.saturating_sub(1));
        backups
    }

    fn record_outcome(&self, child: usize, failed: bool) {
        if let Some(slot) = self.failures.borrow_mut().get_mut(child) {
            *slot = if failed { slot.saturating_add(1) } else { 0 };
        }
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

    #[test]
    fn least_failures_starts_in_config_order() {
        let policy = LeastFailuresPolicy::new(4, 4);
        assert_eq!(policy.failover_order(&req(), 4), vec![1, 2, 3]);
    }

    #[test]
    fn least_failures_never_includes_the_primary() {
        let policy = LeastFailuresPolicy::new(3, 3);
        assert!(!policy.failover_order(&req(), 3).contains(&0));
    }

    #[test]
    fn least_failures_caps_at_max_tries() {
        let policy = LeastFailuresPolicy::new(5, 2);
        assert_eq!(policy.failover_order(&req(), 5).len(), 1);
    }

    #[test]
    fn least_failures_prefers_healthier_backups() {
        let policy = LeastFailuresPolicy::new(4, 4);
        policy.record_outcome(1, true);
        policy.record_outcome(1, true);
        policy.record_outcome(2, true);
        assert_eq!(policy.failover_order(&req(), 4), vec![3, 2, 1]);
    }

    #[test]
    fn least_failures_resets_a_backup_on_success() {
        let policy = LeastFailuresPolicy::new(3, 3);
        policy.record_outcome(1, true);
        policy.record_outcome(1, true);
        policy.record_outcome(1, false);
        assert_eq!(policy.failover_order(&req(), 3), vec![1, 2]);
    }
}
