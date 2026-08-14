use std::sync::atomic::{AtomicU64, Ordering};

// router-wide global TKO counts
#[derive(Default)]
pub struct TkoCounters {
    pub soft_tkos: AtomicU64,
    pub hard_tkos: AtomicU64,
}

impl TkoCounters {
    pub fn total(&self) -> u64 {
        self.soft_tkos.load(Ordering::Relaxed) + self.hard_tkos.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn total_sums_both_gauges() {
        let c = TkoCounters::default();
        c.soft_tkos.fetch_add(2, Ordering::Relaxed);
        c.hard_tkos.fetch_add(3, Ordering::Relaxed);
        assert_eq!(c.total(), 5);
    }
}
