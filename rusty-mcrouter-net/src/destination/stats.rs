use std::time::Duration;

use crate::classify::{ResultCode, RESULT_CODE_COUNT};

#[derive(Clone, Copy, Debug, Default)]
pub struct Stats {
    pub results: [u64; RESULT_CODE_COUNT],
    pub probes_sent: u64,
    pub avg_latency_us: f64,
    pub connects: u64,
    pub idle_closes: u64,
}

impl Stats {
    pub(crate) fn record(&mut self, code: ResultCode, latency: Duration) {
        self.results[code as usize] += 1;
        let sample = latency.as_micros() as f64;
        self.avg_latency_us = if self.avg_latency_us == 0.0 {
            sample
        } else {
            self.avg_latency_us + (sample - self.avg_latency_us) / 16.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_sample_seeds_the_ewma() {
        let mut s = Stats::default();
        s.record(ResultCode::Success, Duration::from_micros(500));
        assert_eq!(s.avg_latency_us, 500.0);
    }

    /// Pins the smoothing window at 16 samples (the verified upstream
    /// ExponentialSmoothData<16>, not the design doc's original 64).
    #[test]
    fn ewma_converges_at_one_sixteenth() {
        let mut s = Stats::default();
        s.record(ResultCode::Success, Duration::from_micros(100));
        s.record(ResultCode::Success, Duration::from_micros(200));
        assert_eq!(s.avg_latency_us, 100.0 + (200.0 - 100.0) / 16.0);
    }

    #[test]
    fn record_indexes_by_result_code() {
        let mut s = Stats::default();
        s.record(ResultCode::Success, Duration::ZERO);
        s.record(ResultCode::Timeout, Duration::ZERO);
        s.record(ResultCode::Timeout, Duration::ZERO);
        assert_eq!(s.results[ResultCode::Success as usize], 1);
        assert_eq!(s.results[ResultCode::Timeout as usize], 2);
        assert_eq!(s.results[ResultCode::Tko as usize], 0);
    }
}
