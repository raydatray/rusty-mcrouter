use crate::selectors::Selector;

const CRC32_TABLE: [u32; 256] = crc_32_table();

const fn crc_32_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0usize;

    while i < 256 {
        let mut crc = i as u32;
        let mut j = 0u32;
        while j < 8 {
            crc = if (crc & 1) != 0 {
                (crc >> 1) ^ 0xEDB8_8320
            } else {
                crc >> 1
            };
            j += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

// mcrouter's `crc32_hash`
fn crc32(key: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &b in key {
        crc = (crc >> 8) ^ CRC32_TABLE[((crc ^ u32::from(b)) & 0xff) as usize];
    }

    !crc
}

pub struct Crc32 {
    n: usize,
}

impl Crc32 {
    pub fn new(n: usize) -> Self {
        Self { n }
    }
}

impl Selector for Crc32 {
    fn select(&self, routing_key: &[u8]) -> usize {
        ((crc32(routing_key) & 0x7fff_ffff) as usize) % self.n
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32_matches_canonical_values() {
        // universally-published CRC-32/ISO-HDLC test vectors.
        assert_eq!(crc32(b""), 0x0000_0000);
        assert_eq!(crc32(b"a"), 0xE8B7_BE43);
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926); // the canonical check value
        assert_eq!(
            crc32(b"The quick brown fox jumps over the lazy dog"),
            0x414F_A339
        );
    }

    #[test]
    fn select_applies_the_0x7fffffff_mask_then_mod() {
        // crc32("123456789") = 0xCBF43926 (high bit set). mcrouter masks off the
        // top bit before `% n`: (0xCBF43926 & 0x7fffffff) = 1_274_296_614,
        // and 1_274_296_614 % 1000 == 614 (vs 262 if the mask were skipped).
        let c = Crc32::new(1000);
        assert_eq!(c.select(b"123456789"), 614);
    }

    #[test]
    fn select_is_in_range() {
        let c = Crc32::new(50);
        for i in 0..1000u32 {
            let key = format!("k{i}");
            assert!(c.select(key.as_bytes()) < 50);
        }
    }
}
