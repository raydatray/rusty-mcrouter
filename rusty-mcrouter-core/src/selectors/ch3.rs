use crate::selectors::furc::{furc_hash, FURC_MAX_POOL_SIZE};
use crate::selectors::{Result, Selector, SelectorBuildError};

pub struct Ch3 {
    n: u32,
}

impl Ch3 {
    pub fn new(n: usize) -> Result<Self> {
        if !(1..=FURC_MAX_POOL_SIZE).contains(&n) {
            return Err(SelectorBuildError::Ch3PoolSizeOutOfRange { n });
        }
        Ok(Self { n: n as u32 })
    }
}

impl Selector for Ch3 {
    fn select(&self, routing_key: &[u8]) -> usize {
        furc_hash(routing_key, self.n) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select_is_in_range() {
        let ch3 = Ch3::new(64).unwrap();
        for i in 0..1000u32 {
            let key = format!("k{i}");
            assert!(ch3.select(key.as_bytes()) < 64);
        }
    }

    #[test]
    fn n_one_always_selects_zero() {
        let ch3 = Ch3::new(1).unwrap();
        assert_eq!(ch3.select(b"anything"), 0);
        assert_eq!(ch3.select(b""), 0);
    }

    #[test]
    fn rejects_pool_size_out_of_range() {
        assert!(Ch3::new(0).is_err());
        assert!(Ch3::new(1 << 23).is_ok()); // 2^23 = furc maximum pool size
        assert!(Ch3::new((1 << 23) + 1).is_err());
    }
}
