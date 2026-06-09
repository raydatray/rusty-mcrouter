use crate::selector::Selector;

pub struct Salted {
    inner: Box<dyn Selector>,
    // todo - does this really need to be a vec?
    salt: Vec<u8>,
}

impl Salted {
    pub fn new(inner: Box<dyn Selector>, salt: impl Into<Vec<u8>>) -> Self {
        Self {
            inner,
            salt: salt.into(),
        }
    }
}

impl Selector for Salted {
    fn select(&self, routing_key: &[u8]) -> usize {
        // todo - this is a new allocation, make it 0 alloc
        let mut buf = Vec::with_capacity(routing_key.len() + self.salt.len());
        buf.extend_from_slice(routing_key);
        buf.extend_from_slice(&self.salt);

        self.inner.select(&buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ch3::Ch3;

    fn boxed_ch3(n: usize) -> Box<dyn Selector> {
        Box::new(Ch3::new(n))
    }

    #[test]
    fn empty_salt_matches_unsalted() {
        let n = 128;
        let salted = Salted::new(boxed_ch3(n), Vec::new());
        let plain = Ch3::new(n);
        for i in 0..500u32 {
            let key = format!("k{i}");
            assert_eq!(salted.select(key.as_bytes()), plain.select(key.as_bytes()));
        }
    }

    #[test]
    fn different_salts_change_distribution() {
        let n = 1024;
        let a = Salted::new(boxed_ch3(n), b"salt-a".to_vec());
        let b = Salted::new(boxed_ch3(n), b"salt-b".to_vec());
        let mut differ = 0;
        for i in 0..1000u32 {
            let key = format!("k{i}");
            if a.select(key.as_bytes()) != b.select(key.as_bytes()) {
                differ += 1;
            }
        }
        // with n=1024, coincidental agreement is ~1/1024, so nearly all differ.
        assert!(
            differ > 900,
            "only {differ}/1000 keys differed across salts"
        );
    }

    #[test]
    fn salted_is_in_range() {
        let n = 64;
        let s = Salted::new(boxed_ch3(n), b"x".to_vec());
        for i in 0..500u32 {
            let key = format!("k{i}");
            assert!(s.select(key.as_bytes()) < n);
        }
    }
}
