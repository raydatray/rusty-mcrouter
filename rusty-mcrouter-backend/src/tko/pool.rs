use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};

use rusty_mcrouter_observability_primitives::Counter;

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

/// Outcome of asking the gate for a TKO slot. An enum rather than the old
/// (bool, bool): `Admitted` with `just_entered` was representable nonsense,
/// and two adjacent bools at a call site say nothing about which is which.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GateDecision {
    /// Slot reserved: the counter was incremented, the caller may mark.
    Admitted,
    /// Fail-open refused the mark; nothing was counted. `just_entered` is
    /// true only for the call that crossed `enter`, so EnterFailOpen can be
    /// emitted exactly once.
    Refused { just_entered: bool },
}

pub struct PoolTkoTracker {
    name: Arc<str>,
    thresholds: FailOpenThresholds,
    fail_open: AtomicBool,
    num_destinations_tko: AtomicU64,
    entered_total: Counter,
    exited_total: Counter,
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
            entered_total: Counter::default(),
            exited_total: Counter::default(),
        }
    }

    // reserve a TKO slot
    pub fn inc_num_destinations_tko(&self) -> GateDecision {
        if self.fail_open.load(Ordering::Acquire) {
            return GateDecision::Refused {
                just_entered: false,
            };
        }

        let mut cur = self.num_destinations_tko.load(Ordering::Relaxed);
        loop {
            if cur == self.thresholds.enter {
                self.fail_open.store(true, Ordering::Release);
                self.entered_total.inc();
                return GateDecision::Refused { just_entered: true };
            }
            match self.num_destinations_tko.compare_exchange_weak(
                cur,
                cur + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return GateDecision::Admitted,
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
                self.exited_total.inc();
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

    pub fn name(&self) -> &Arc<str> {
        &self.name
    }

    pub fn fail_open(&self) -> bool {
        self.fail_open.load(Ordering::Acquire)
    }

    pub fn num_destinations_tko(&self) -> u64 {
        self.num_destinations_tko.load(Ordering::Relaxed)
    }

    pub fn entered_total(&self) -> u64 {
        self.entered_total.load()
    }

    pub fn exited_total(&self) -> u64 {
        self.exited_total.load()
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
        assert_eq!(g.inc_num_destinations_tko(), GateDecision::Admitted);
        assert_eq!(g.inc_num_destinations_tko(), GateDecision::Admitted);
        assert_eq!(
            g.inc_num_destinations_tko(),
            GateDecision::Refused { just_entered: true }
        ); // crossing call
        assert_eq!(count(&g), 2, "counter must never exceed enter");
        assert!(g.fail_open.load(Ordering::SeqCst));
    }

    /// Once fail-open, further reservations are refused WITHOUT touching
    /// the counter, and just_entered fires only for the crossing call.
    #[test]
    fn inc_while_fail_open_returns_without_incrementing() {
        let g = gate(1, 1);
        assert_eq!(g.inc_num_destinations_tko(), GateDecision::Admitted);
        assert_eq!(
            g.inc_num_destinations_tko(),
            GateDecision::Refused { just_entered: true }
        );
        assert_eq!(
            g.inc_num_destinations_tko(),
            GateDecision::Refused {
                just_entered: false
            }
        );
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
        assert_eq!(
            g.inc_num_destinations_tko(),
            GateDecision::Refused { just_entered: true }
        ); // refused, now open
        g.dec_num_destinations_tko(); // 2 -> 1
        assert!(g.dec_num_destinations_tko()); // exit flip
        assert_eq!(g.inc_num_destinations_tko(), GateDecision::Admitted); // admitted again
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
