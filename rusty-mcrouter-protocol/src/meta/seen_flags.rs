#[derive(Default)]
pub(super) struct SeenFlags([u64; 4]);

impl SeenFlags {
    /// Returns true when `flag` was not already present.
    pub(super) fn insert(&mut self, flag: u8) -> bool {
        let word = usize::from(flag / 64);
        let bit = 1_u64 << (flag % 64);
        let inserted = self.0[word] & bit == 0;
        self.0[word] |= bit;
        inserted
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_duplicate_values_across_all_words() {
        let mut seen = SeenFlags::default();
        for flag in [0, 63, 64, 127, 128, 191, 192, 255] {
            assert!(seen.insert(flag));
            assert!(!seen.insert(flag));
        }
    }
}
