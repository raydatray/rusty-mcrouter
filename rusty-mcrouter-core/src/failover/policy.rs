use std::cell::RefCell;

use rusty_mcrouter_protocol::Request;

use crate::FailoverPolicyKind;

pub trait FailoverPolicy: 'static {
    fn kind(&self) -> FailoverPolicyKind;

    fn failover_order(&self, request: &Request, n: usize) -> Vec<usize>;

    fn record_outcome(&self, child: usize, is_error: bool) {
        let _ = (child, is_error);
    }
}

pub struct InOrderPolicy;

impl FailoverPolicy for InOrderPolicy {
    fn kind(&self) -> FailoverPolicyKind {
        FailoverPolicyKind::InOrder
    }

    fn failover_order(&self, _request: &Request, n: usize) -> Vec<usize> {
        (1..n).collect()
    }
}

/// Orders backups healthiest-first and caps the candidate sequence to the
/// configured total attempt count, including primary.
pub struct LeastFailuresPolicy {
    failures: RefCell<Vec<u32>>,
    max_tries: usize,
}

impl LeastFailuresPolicy {
    pub fn new(n: usize, max_tries: usize) -> Self {
        assert!(n > 0, "least-failures requires at least one child");
        assert!(max_tries > 0, "least-failures max_tries must be positive");
        Self {
            failures: RefCell::new(vec![0; n]),
            max_tries: max_tries.min(n),
        }
    }
}

impl FailoverPolicy for LeastFailuresPolicy {
    fn kind(&self) -> FailoverPolicyKind {
        FailoverPolicyKind::LeastFailures
    }

    fn failover_order(&self, _request: &Request, n: usize) -> Vec<usize> {
        let failures = self.failures.borrow();
        let mut backups: Vec<usize> = (1..n).collect();
        backups.sort_by_key(|&i| failures.get(i).copied().unwrap_or(0));
        backups.truncate(self.max_tries.saturating_sub(1));
        backups
    }

    fn record_outcome(&self, child: usize, is_error: bool) {
        if let Some(slot) = self.failures.borrow_mut().get_mut(child) {
            *slot = if is_error { slot.saturating_add(1) } else { 0 };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusty_mcrouter_protocol::test_support::get;

    #[test]
    fn in_order_yields_backups_in_config_order() {
        assert_eq!(InOrderPolicy.failover_order(&get(b"k"), 4), vec![1, 2, 3]);
    }

    #[test]
    fn in_order_single_child_has_no_backups() {
        assert_eq!(
            InOrderPolicy.failover_order(&get(b"k"), 1),
            Vec::<usize>::new()
        );
    }

    #[test]
    fn in_order_record_outcome_does_not_change_order() {
        let policy = InOrderPolicy;
        policy.record_outcome(1, true);
        policy.record_outcome(2, false);
        assert_eq!(policy.failover_order(&get(b"k"), 3), vec![1, 2]);
    }

    #[test]
    fn least_failures_starts_in_config_order() {
        let policy = LeastFailuresPolicy::new(4, 4);
        assert_eq!(policy.failover_order(&get(b"k"), 4), vec![1, 2, 3]);
    }

    #[test]
    fn least_failures_never_includes_the_primary() {
        let policy = LeastFailuresPolicy::new(3, 3);
        assert!(!policy.failover_order(&get(b"k"), 3).contains(&0));
    }

    #[test]
    fn least_failures_caps_candidates_including_primary() {
        let policy = LeastFailuresPolicy::new(5, 3);
        assert_eq!(policy.failover_order(&get(b"k"), 5).len(), 2);
    }

    #[test]
    fn least_failures_prefers_healthier_backups() {
        let policy = LeastFailuresPolicy::new(4, 4);
        policy.record_outcome(1, true);
        policy.record_outcome(1, true);
        policy.record_outcome(2, true);
        assert_eq!(policy.failover_order(&get(b"k"), 4), vec![3, 2, 1]);
    }

    #[test]
    fn least_failures_resets_a_backup_on_success() {
        let policy = LeastFailuresPolicy::new(3, 3);
        policy.record_outcome(1, true);
        policy.record_outcome(1, true);
        policy.record_outcome(1, false);
        assert_eq!(policy.failover_order(&get(b"k"), 3), vec![1, 2]);
    }
}
