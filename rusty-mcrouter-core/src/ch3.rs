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
