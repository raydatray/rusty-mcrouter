use crate::{furc::furc_hash, selector::Selector};

pub struct Ch3 {
    n: u32,
}

impl Ch3 {
    pub fn new(n: usize) -> Self {
        Self { n: n as u32 }
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
        let ch3 = Ch3::new(64);
        for i in 0..1000u32 {
            let key = format!("k{i}");
            assert!(ch3.select(key.as_bytes()) < 64);
        }
    }

    #[test]
    fn n_one_always_selects_zero() {
        let ch3 = Ch3::new(1);
        assert_eq!(ch3.select(b"anything"), 0);
        assert_eq!(ch3.select(b""), 0);
    }
}
