use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};

/// Fail-open hysteresis thresholds: the gate opens when `enter` destinations
/// are TKO'd (refusing further marks) and closes again once recoveries drain
/// the count to `exit`. Named fields because two bare u64s in a tuple invite
/// a silent swap — and a swapped enter/exit inverts the hysteresis.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FailOpenThresholds {
    /// "upper": TKO'd-destination count at which the pool fails open.
    pub enter: u64,
    /// "lower": count at which recoveries close the gate again.
    pub exit: u64,
}

pub struct PoolTkoTracker {
    name: Arc<str>,
    thresholds: FailOpenThresholds,
    fail_open: AtomicBool,
    num_destinations_tko: AtomicU64,
}

impl PoolTkoTracker {
    pub(crate) fn new(name: Arc<str>, thresholds: FailOpenThresholds) -> Self {
        debug_assert!(
            thresholds.enter > 0 && thresholds.exit > 0 && thresholds.exit <= thresholds.enter
        );

        Self {
            name,
            thresholds,
            fail_open: AtomicBool::new(false),
            num_destinations_tko: AtomicU64::new(0),
        }
    }

    // reserve a TKO slot. returns currently_fail_open, just_entered
    pub fn inc_num_destinations_tko(&self) -> (bool, bool) {
        if self.fail_open.load(Ordering::Acquire) {
            return (true, false);
        }

        let mut cur = self.num_destinations_tko.load(Ordering::Relaxed);
        loop {
            if cur == self.thresholds.enter {
                self.fail_open.store(true, Ordering::Release);
                return (true, true);
            }
            match self.num_destinations_tko.compare_exchange_weak(
                cur,
                cur + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return (false, false),
                Err(actual) => cur = actual,
            }
        }
    }

    // returns true if this call exited a fail-open state
    pub fn dec_num_destinations_tko(&self) -> bool {
        let mut cur = self.num_destinations_tko.load(Ordering::Relaxed);
        loop {
            if self.fail_open.load(Ordering::Acquire) && cur == self.thresholds.exit {
                self.fail_open.store(false, Ordering::Release);
                return true;
            }
            match self.num_destinations_tko.compare_exchange_weak(
                cur,
                cur.saturating_sub(1),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return false,
                Err(actual) => cur = actual,
            }
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gate(enter: u64, exit: u64) -> PoolTkoTracker {
        PoolTkoTracker::new(Arc::from("pool"), FailOpenThresholds { enter, exit })
    }

    fn count(g: &PoolTkoTracker) -> u64 {
        g.num_destinations_tko.load(Ordering::SeqCst)
    }

    /// The verified upstream quirk: the increment that WOULD cross `enter`
    /// is consumed by the state flip, so the counter never exceeds `enter`.
    #[test]
    fn enter_flip_consumes_the_increment() {
        let g = gate(2, 1);
        assert_eq!(g.inc_num_destinations_tko(), (false, false));
        assert_eq!(g.inc_num_destinations_tko(), (false, false));
        assert_eq!(g.inc_num_destinations_tko(), (true, true)); // crossing call
        assert_eq!(count(&g), 2, "counter must never exceed enter");
        assert!(g.fail_open.load(Ordering::SeqCst));
    }

    /// Once fail-open, further reservations are refused WITHOUT touching
    /// the counter, and just_entered fires only for the crossing call.
    #[test]
    fn inc_while_fail_open_returns_without_incrementing() {
        let g = gate(1, 1);
        assert_eq!(g.inc_num_destinations_tko(), (false, false));
        assert_eq!(g.inc_num_destinations_tko(), (true, true));
        assert_eq!(g.inc_num_destinations_tko(), (true, false));
        assert_eq!(count(&g), 1);
    }

    /// Mirror quirk on the way down: the decrement that reaches `exit`
    /// flips the state instead of decrementing, and reports the exit
    /// transition exactly once.
    #[test]
    fn exit_flip_consumes_the_decrement() {
        let g = gate(2, 1);
        g.inc_num_destinations_tko();
        g.inc_num_destinations_tko();
        g.inc_num_destinations_tko(); // flips open; count stays 2
        assert!(!g.dec_num_destinations_tko()); // 2 -> 1
        assert!(g.dec_num_destinations_tko()); // cur == exit: flip, no decrement
        assert_eq!(count(&g), 1, "exiting decrement is consumed by the flip");
        assert!(!g.fail_open.load(Ordering::SeqCst));
    }

    /// The enter/exit gap is hysteresis: after exiting fail-open, new
    /// reservations are admitted again.
    #[test]
    fn hysteresis_reenables_marking_after_exit() {
        let g = gate(2, 1);
        g.inc_num_destinations_tko();
        g.inc_num_destinations_tko();
        assert_eq!(g.inc_num_destinations_tko(), (true, true)); // refused, now open
        g.dec_num_destinations_tko(); // 2 -> 1
        assert!(g.dec_num_destinations_tko()); // exit flip
        assert_eq!(g.inc_num_destinations_tko(), (false, false)); // admitted again
    }

    /// The exit-flip branch requires fail_open: a normal release when the
    /// gate never opened just decrements, even at cur == exit.
    #[test]
    fn dec_without_fail_open_never_flips() {
        let g = gate(3, 1);
        g.inc_num_destinations_tko();
        assert!(!g.dec_num_destinations_tko());
        assert_eq!(count(&g), 0);
        assert!(!g.fail_open.load(Ordering::SeqCst));
    }
}
