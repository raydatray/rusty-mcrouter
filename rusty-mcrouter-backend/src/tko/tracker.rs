use std::sync::{
    atomic::{AtomicU64, AtomicU8, Ordering},
    Arc, Mutex, Weak,
};

use crate::{
    classify::ResultCode,
    tko::{
        counters::TkoCounters,
        events::{TkoEvent, TkoEventRecord},
        map::TkoTrackerMap,
        pool::{GateDecision, PoolTkoTracker},
    },
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DestToken(u64);

pub(crate) const TOKEN_BASE: u64 = 1 << 10;
static NEXT_TOKEN: AtomicU64 = AtomicU64::new(TOKEN_BASE);

impl DestToken {
    pub fn allocate() -> DestToken {
        DestToken(NEXT_TOKEN.fetch_add(2, Ordering::Relaxed))
    }
}

#[derive(Clone, Copy, Debug)]
enum TkoKind {
    Soft,
    Hard,
}

pub struct TkoTracker {
    sum_failures: AtomicU64,
    threshold: u64,
    tko_reason: AtomicU8,
    consecutive_failures: AtomicU64,
    global: Arc<TkoCounters>,
    pool: Mutex<Option<Arc<PoolTkoTracker>>>,
    key: Arc<str>,
    map: Weak<TkoTrackerMap>,
}

impl TkoTracker {
    pub(crate) fn new(
        threshold: u64,
        global: Arc<TkoCounters>,
        key: Arc<str>,
        map: Weak<TkoTrackerMap>,
    ) -> Self {
        assert!(threshold > 0 && threshold < TOKEN_BASE);

        Self {
            sum_failures: AtomicU64::new(0),
            threshold,
            tko_reason: AtomicU8::new(ResultCode::Success as u8),
            consecutive_failures: AtomicU64::new(0),
            global,
            pool: Mutex::new(None),
            key,
            map,
        }
    }

    pub(crate) fn set_pool_tracker(&self, pool: Arc<PoolTkoTracker>) {
        *self.pool.lock().unwrap() = Some(pool)
    }

    #[inline]
    pub fn is_tko(&self) -> bool {
        self.sum_failures.load(Ordering::Relaxed) > self.threshold
    }

    pub fn is_hard_tko(&self) -> bool {
        let sum_failures = self.sum_failures.load(Ordering::Acquire);

        sum_failures > self.threshold && sum_failures & 1 == 1
    }

    pub fn is_soft_tko(&self) -> bool {
        let sum_failures = self.sum_failures.load(Ordering::Acquire);

        sum_failures > self.threshold && sum_failures & 1 == 0
    }

    fn is_responsible(&self, dest: DestToken) -> bool {
        self.sum_failures.load(Ordering::Acquire) & !1 == dest.0
    }

    fn increment_tko_count(&self, kind: TkoKind) -> bool {
        if let Some(pool) = self.pool() {
            if let GateDecision::Refused { just_entered } = pool.inc_num_destinations_tko() {
                if just_entered {
                    self.emit(TkoEvent::EnterFailOpen, self.reason(), Some(&pool));
                }
                return false;
            }
        }

        self.counter(kind).fetch_add(1, Ordering::Relaxed);
        true
    }

    fn decrement_tko_count(&self, kind: TkoKind) {
        let old = self.counter(kind).fetch_sub(1, Ordering::Relaxed);

        debug_assert!(old != 0, "{kind:?} underflow: unmark without matching mark");
        if let Some(pool) = self.pool() {
            if pool.dec_num_destinations_tko() {
                self.emit(TkoEvent::ExitFailOpen, ResultCode::Success, Some(&pool));
            }
        }
    }

    pub fn record_soft_failure(&self, dest: DestToken, reason: ResultCode) -> bool {
        self.consecutive_failures.fetch_add(1, Ordering::Relaxed);

        let mut cur = self.sum_failures.load(Ordering::Relaxed);
        let mut value = 0;

        loop {
            if cur == self.threshold - 1 {
                if value != dest.0 && !self.increment_tko_count(TkoKind::Soft) {
                    return false; // pool fail-open
                }

                value = dest.0;
            } else {
                if value == dest.0 {
                    self.decrement_tko_count(TkoKind::Soft); // undo stale reservation
                }

                if cur > self.threshold {
                    return false;
                } // raced - already marked

                value = cur + 1;
            }

            match self.sum_failures.compare_exchange_weak(
                cur,
                value,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => cur = actual,
            }
        }

        if value == dest.0 {
            self.tko_reason.store(reason as u8, Ordering::Relaxed); // caller won responsibility - start probing
            return true;
        }

        false
    }

    pub fn record_hard_failure(&self, dest: DestToken, reason: ResultCode) -> bool {
        self.consecutive_failures.fetch_add(1, Ordering::Relaxed);

        if self.is_hard_tko() {
            return false;
        }

        if self.is_responsible(dest) {
            // we already own this tko - so just convert in place
            self.sum_failures.fetch_or(1, Ordering::AcqRel);
            self.global.hard_tkos.fetch_add(1, Ordering::Relaxed);
            self.global.soft_tkos.fetch_sub(1, Ordering::Relaxed);

            // we were already responsible, probes are already running
            return false;
        }

        let hard_val = dest.0 | 1;
        let mut cur = self.sum_failures.load(Ordering::Relaxed);

        loop {
            if cur > self.threshold {
                return false; // raced - someone else marked while we looped
            }

            if !self.increment_tko_count(TkoKind::Hard) {
                return false; // pool fail-open refused - do NOT mark
            }

            match self.sum_failures.compare_exchange_weak(
                cur,
                hard_val,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => {
                    self.decrement_tko_count(TkoKind::Hard);
                    cur = actual;
                }
            }
        }

        self.tko_reason.store(reason as u8, Ordering::Relaxed);
        true
    }

    pub fn record_success(&self, dest: DestToken) -> bool {
        if self.is_responsible(dest) {
            if self.is_soft_tko() {
                self.decrement_tko_count(TkoKind::Soft);
            }
            if self.is_hard_tko() {
                self.decrement_tko_count(TkoKind::Hard);
            }

            self.tko_reason
                .store(ResultCode::Success as u8, Ordering::Relaxed);
            self.sum_failures.store(0, Ordering::Relaxed);
            self.consecutive_failures.store(0, Ordering::Relaxed);

            return true;
        }

        if self.sum_failures.load(Ordering::Relaxed) != 0 && self.reset_if_not_tko() {
            self.consecutive_failures.store(0, Ordering::Relaxed);
        }

        false
    }

    fn reset_if_not_tko(&self) -> bool {
        let mut cur = self.sum_failures.load(Ordering::Relaxed);
        loop {
            if cur > self.threshold {
                return false;
            }
            match self.sum_failures.compare_exchange_weak(
                cur,
                0,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => cur = actual,
            }
        }
    }

    pub(crate) fn emit(
        &self,
        event: TkoEvent,
        reason: ResultCode,
        pool: Option<&Arc<PoolTkoTracker>>,
    ) {
        let Some(map) = self.map.upgrade() else {
            return;
        };

        map.emit(TkoEventRecord {
            event,
            server: Arc::clone(&self.key),
            pool: pool.map(|p| Arc::clone(p.name())),
            reason,
            consecutive_failures: self.consecutive_failures(),
            global_soft_tkos: self.global.soft_tkos.load(Ordering::Relaxed),
            global_hard_tkos: self.global.hard_tkos.load(Ordering::Relaxed),
        });
    }

    pub fn reason(&self) -> ResultCode {
        ResultCode::from_u8(self.tko_reason.load(Ordering::Relaxed))
    }

    pub fn consecutive_failures(&self) -> u64 {
        self.consecutive_failures.load(Ordering::Relaxed)
    }

    pub fn remove_destination(&self, dest: DestToken) -> bool {
        if self.is_responsible(dest) {
            return self.record_success(dest);
        }

        false
    }

    fn pool(&self) -> Option<Arc<PoolTkoTracker>> {
        self.pool.lock().unwrap().clone()
    }

    fn counter(&self, kind: TkoKind) -> &AtomicU64 {
        match kind {
            TkoKind::Soft => &self.global.soft_tkos,
            TkoKind::Hard => &self.global.hard_tkos,
        }
    }
}

impl Drop for TkoTracker {
    fn drop(&mut self) {
        if let Some(map) = self.map.upgrade() {
            map.remove_dead(&self.key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tko::events::TkoEventSink;
    use crate::tko::map::TkoTrackerMap;
    use crate::tko::pool::FailOpenThresholds;

    fn null_sink() -> TkoEventSink {
        TkoEventSink::new(|_| {})
    }

    /// Tracker via the map (the only production construction path); the map
    /// is returned too so tests can read the global gauge.
    fn tracker(threshold: u64) -> (Arc<TkoTrackerMap>, Arc<TkoTracker>) {
        let map = TkoTrackerMap::with_sink(null_sink());
        let t = map.tracker_for("server:11211", threshold);
        (map, t)
    }

    #[test]
    fn token_allocation_is_even_monotonic_and_above_token_base() {
        let a = DestToken::allocate();
        let b = DestToken::allocate();
        assert_eq!(a.0 % 2, 0, "tokens must be even: LSB is the hard bit");
        assert_eq!(b.0 % 2, 0);
        assert!(b.0 > a.0);
        assert!(a.0 >= TOKEN_BASE, "tokens must be disjoint from counts");
    }

    /// failures_until_tko means CONSECUTIVE: a success in between resets
    /// the count, so 2 + 3 failures with a success between marks only on
    /// the 3rd consecutive one.
    #[test]
    fn marks_soft_only_on_consecutive_threshold_failures() {
        let (_map, t) = tracker(3);
        let dest = DestToken::allocate();

        assert!(!t.record_soft_failure(dest, ResultCode::Timeout));
        assert!(!t.record_soft_failure(dest, ResultCode::Timeout));
        assert!(!t.is_tko());

        assert!(!t.record_success(dest)); // not owner: resets the count only

        assert!(!t.record_soft_failure(dest, ResultCode::Timeout));
        assert!(!t.record_soft_failure(dest, ResultCode::Timeout));
        assert!(!t.is_tko(), "count must have reset on success");
        assert!(t.record_soft_failure(dest, ResultCode::Timeout)); // 3rd consecutive
        assert!(t.is_soft_tko());
        assert_eq!(t.reason(), ResultCode::Timeout);
        assert_eq!(t.consecutive_failures(), 3);
    }

    /// While TKO'd, a success from anyone but the responsible destination
    /// (a straggler reply from a pre-mark request) must NOT unmark.
    #[test]
    fn straggler_success_never_unmarks() {
        let (map, t) = tracker(1);
        let owner = DestToken::allocate();
        let straggler = DestToken::allocate();

        assert!(t.record_soft_failure(owner, ResultCode::Timeout));
        assert!(t.is_soft_tko());

        assert!(!t.record_success(straggler));
        assert!(t.is_soft_tko(), "only the owner may unmark");

        assert!(t.record_success(owner));
        assert!(!t.is_tko());
        assert_eq!(t.reason(), ResultCode::Success);
        assert_eq!(map.global_tkos().total(), 0);
    }

    #[test]
    fn hard_failure_marks_instantly_and_repeat_is_noop() {
        let (map, t) = tracker(3);
        let dest = DestToken::allocate();

        assert!(t.record_hard_failure(dest, ResultCode::ConnectError));
        assert!(t.is_hard_tko());
        assert_eq!(t.reason(), ResultCode::ConnectError);
        assert_eq!(map.global_tkos().hard_tkos.load(Ordering::Relaxed), 1);

        assert!(!t.record_hard_failure(dest, ResultCode::ConnectError));
        assert_eq!(map.global_tkos().hard_tkos.load(Ordering::Relaxed), 1);

        assert!(t.record_success(dest));
        assert_eq!(map.global_tkos().total(), 0);
    }

    /// The owner's soft->hard conversion moves the GLOBAL gauges but leaves
    /// the POOL slot count untouched (same box, still one TKO'd destination).
    /// Proof via capacity: with enter=2, a second box can still mark after
    /// the conversion — a double-counted conversion would have tripped the
    /// gate.
    #[test]
    fn soft_to_hard_conversion_moves_globals_but_not_pool_count() {
        let map = TkoTrackerMap::with_sink(null_sink());
        let gate = map.pool_tracker_for("pool", FailOpenThresholds { enter: 2, exit: 1 });
        let a = map.tracker_for("a:11211", 1);
        let b = map.tracker_for("b:11211", 1);
        a.set_pool_tracker(Arc::clone(&gate));
        b.set_pool_tracker(Arc::clone(&gate));
        let tok_a = DestToken::allocate();
        let tok_b = DestToken::allocate();

        assert!(a.record_soft_failure(tok_a, ResultCode::Timeout)); // pool slot 1
        assert!(!a.record_hard_failure(tok_a, ResultCode::ConnectError)); // convert
        assert!(a.is_hard_tko());
        assert_eq!(map.global_tkos().soft_tkos.load(Ordering::Relaxed), 0);
        assert_eq!(map.global_tkos().hard_tkos.load(Ordering::Relaxed), 1);
        // faithful to upstream: conversion does not rewrite the reason
        assert_eq!(a.reason(), ResultCode::Timeout);

        assert!(b.record_hard_failure(tok_b, ResultCode::ConnectError));
        assert!(
            b.is_hard_tko(),
            "conversion must not have consumed a pool slot"
        );
    }

    #[test]
    fn hard_failure_does_not_take_over_anothers_soft_tko() {
        let (_map, t) = tracker(1);
        let owner = DestToken::allocate();
        let other = DestToken::allocate();

        assert!(t.record_soft_failure(owner, ResultCode::Timeout));
        assert!(!t.record_hard_failure(other, ResultCode::ConnectError));
        assert!(t.is_soft_tko(), "soft ownership is not taken over");

        assert!(t.record_success(owner));
        assert!(!t.is_tko());
    }

    #[test]
    fn remove_destination_unmarks_owner_only() {
        let (map, t) = tracker(1);
        let owner = DestToken::allocate();
        let other = DestToken::allocate();

        assert!(t.record_soft_failure(owner, ResultCode::Timeout));
        assert!(!t.remove_destination(other));
        assert!(t.is_tko());
        assert!(t.remove_destination(owner));
        assert!(!t.is_tko());
        assert_eq!(map.global_tkos().total(), 0);
    }

    /// The pool gate refusing a mark leaves the word untouched: the box
    /// keeps failing naturally instead of being marked.
    #[test]
    fn gate_refusal_leaves_word_unmarked() {
        let map = TkoTrackerMap::with_sink(null_sink());
        let gate = map.pool_tracker_for("pool", FailOpenThresholds { enter: 1, exit: 1 });
        let a = map.tracker_for("a:11211", 1);
        let b = map.tracker_for("b:11211", 1);
        a.set_pool_tracker(Arc::clone(&gate));
        b.set_pool_tracker(Arc::clone(&gate));

        assert!(a.record_soft_failure(DestToken::allocate(), ResultCode::Timeout));
        let tok_b = DestToken::allocate();
        assert!(!b.record_soft_failure(tok_b, ResultCode::Timeout)); // refused
        assert!(!b.is_tko());
        assert!(!b.record_hard_failure(tok_b, ResultCode::ConnectError)); // also refused
        assert!(!b.is_tko());
        assert_eq!(
            map.global_tkos().total(),
            1,
            "only the admitted mark counts"
        );
    }
}
