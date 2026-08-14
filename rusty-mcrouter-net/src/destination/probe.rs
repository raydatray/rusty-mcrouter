use std::{rc::Weak, time::Duration};

use crate::destination::destination::Destination;

const PROBE_EXPONENTIAL_FACTOR: f64 = 1.5;
const PROBE_JITTER_MIN: f64 = 0.05;
const PROBE_JITTER_MAX: f64 = 0.5;

fn jitter(state: &mut u64) -> f64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;

    PROBE_JITTER_MIN
        + (*state >> 11) as f64 / (1u64 << 53) as f64 * (PROBE_JITTER_MAX - PROBE_JITTER_MIN)
}

pub(crate) async fn probe_loop(dest: Weak<Destination>, initial: Duration, max: Duration) {
    let mut rng = Weak::as_ptr(&dest) as u64 ^ initial.as_nanos() as u64; // get some randomness by taking the addr of the destination so we dont thundering herd
    let mut delay_ms = initial.as_millis() as u64;
    loop {
        let delay = Duration::from_millis((delay_ms as f64 * (1.0 + jitter(&mut rng))) as u64);

        tokio::time::sleep(delay).await;

        let Some(dest) = dest.upgrade() else { return };
        dest.send_probe().await;

        if !dest.is_tko() {
            return;
        }
        drop(dest);

        if delay_ms < 2 {
            delay_ms = 2;
        } else {
            delay_ms = (delay_ms as f64 * PROBE_EXPONENTIAL_FACTOR) as u64
        }

        delay_ms = delay_ms.min(max.as_millis() as u64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jitter_stays_in_bounds() {
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        for _ in 0..10_000 {
            let j = jitter(&mut state);
            assert!(
                (PROBE_JITTER_MIN..PROBE_JITTER_MAX).contains(&j),
                "jitter {j} out of [{PROBE_JITTER_MIN}, {PROBE_JITTER_MAX})"
            );
        }
    }

    /// The thundering-herd regression: distinct seeds (distinct destinations)
    /// must produce distinct jitter sequences, or every probe loop in the
    /// router fires in lockstep.
    #[test]
    fn distinct_seeds_decorrelate() {
        let mut a = 1u64;
        let mut b = 2u64;
        let seq_a: Vec<u64> = (0..8).map(|_| (jitter(&mut a) * 1e9) as u64).collect();
        let seq_b: Vec<u64> = (0..8).map(|_| (jitter(&mut b) * 1e9) as u64).collect();
        assert_ne!(seq_a, seq_b);
    }

    #[test]
    fn same_seed_reproduces() {
        let mut a = 42u64;
        let mut b = 42u64;
        for _ in 0..8 {
            assert_eq!(jitter(&mut a).to_bits(), jitter(&mut b).to_bits());
        }
    }
}
