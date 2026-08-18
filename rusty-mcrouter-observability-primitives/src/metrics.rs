use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

#[derive(Default)]
#[repr(transparent)]
pub struct Counter(AtomicU64);

impl Counter {
    pub fn inc(&self) -> u64 {
        self.add(1)
    }

    pub fn add(&self, value: u64) -> u64 {
        self.0.fetch_add(value, Ordering::Relaxed)
    }

    pub fn load(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

#[derive(Default)]
#[repr(transparent)]
pub struct Gauge(AtomicI64);

impl Gauge {
    pub fn inc(&self) -> i64 {
        self.add(1)
    }

    pub fn dec(&self) -> i64 {
        self.sub(1)
    }

    pub fn add(&self, value: i64) -> i64 {
        self.0.fetch_add(value, Ordering::Relaxed)
    }

    pub fn sub(&self, value: i64) -> i64 {
        self.0.fetch_sub(value, Ordering::Relaxed)
    }

    pub fn set(&self, value: i64) {
        self.0.store(value, Ordering::Relaxed);
    }

    pub fn load(&self) -> i64 {
        self.0.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn counters_default_to_zero_and_accumulate() {
        let counter = Counter::default();
        assert_eq!(counter.inc(), 0);
        assert_eq!(counter.add(4), 1);
        assert_eq!(counter.load(), 5);
    }

    #[test]
    fn gauges_move_both_directions_and_can_reset() {
        let gauge = Gauge::default();
        assert_eq!(gauge.add(3), 0);
        assert_eq!(gauge.dec(), 3);
        gauge.set(-4);
        assert_eq!(gauge.load(), -4);
    }

    #[test]
    fn arrays_default_each_cell_independently() {
        let counters: [Counter; 3] = Default::default();
        counters[1].inc();
        assert_eq!(counters[0].load(), 0);
        assert_eq!(counters[1].load(), 1);
        assert_eq!(counters[2].load(), 0);
    }

    #[test]
    fn counter_is_safe_for_concurrent_writers() {
        let counter = Arc::new(Counter::default());
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let counter = Arc::clone(&counter);
                std::thread::spawn(move || {
                    for _ in 0..1_000 {
                        counter.inc();
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }
        assert_eq!(counter.load(), 4_000);
    }
}
