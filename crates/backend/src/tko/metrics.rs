use rusty_mcrouter_observability_primitives::Gauge;

// router-wide global TKO gauges
#[derive(Default)]
pub struct GlobalTkoMetrics {
    pub soft_tkos: Gauge,
    pub hard_tkos: Gauge,
}

impl GlobalTkoMetrics {
    pub fn total(&self) -> i64 {
        self.soft_tkos.load() + self.hard_tkos.load()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn total_sums_both_gauges() {
        let metrics = GlobalTkoMetrics::default();
        metrics.soft_tkos.add(2);
        metrics.hard_tkos.add(3);
        assert_eq!(metrics.total(), 5);
    }
}
