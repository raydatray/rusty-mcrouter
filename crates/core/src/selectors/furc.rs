// port of mcrouter's `furc_hash` consistent hash
// `mcrouter/lib/fbi/hash.c` @ `42aa391189c7` byte-for-byte compatible

const M: u64 = 0xc6a4_a793_5bd1_e995;
const R: u32 = 47;
const SEED: u64 = 4_193_360_111;
const FURC_SHIFT: u32 = 23;
const MAX_TRIES: u32 = 32;
const FURC_CACHE_SIZE: usize = 1024;

// largest pool `furc_hash` supports: `1 << FURC_SHIFT` (8,388,608)
pub(crate) const FURC_MAX_POOL_SIZE: usize = 1 << FURC_SHIFT;

// mcrouter's variant of `murmur_hash_64A`
// - little endian 64-bit word reads
// - seeded as `furc` seeds it
// - tail handling mirrors the `switch (len & 7)` fallthrough
fn murmur_hash_64a(key: &[u8], seed: u64) -> u64 {
    let mut chunks = key.chunks_exact(8);

    let mut h = chunks
        .by_ref()
        .map(|chunk| u64::from_le_bytes(chunk.try_into().unwrap()))
        .fold(seed ^ (key.len() as u64).wrapping_mul(M), |h, mut k| {
            k = k.wrapping_mul(M);
            k ^= k >> R;
            k = k.wrapping_mul(M);
            (h ^ k).wrapping_mul(M)
        });

    let tail = chunks.remainder();
    if !tail.is_empty() {
        let k = tail
            .iter()
            .enumerate()
            .fold(0u64, |k, (i, &b)| k | (u64::from(b) << (8 * i)));

        h = (h ^ k).wrapping_mul(M);
    }

    h ^= h >> R;
    h = h.wrapping_mul(M);
    h ^= h >> R;
    h
}

// mcrouter's `murmur_rehash_64A`
// - derive the next 64-bit word from the previous one
fn murmur_rehash_64a(mut k: u64) -> u64 {
    let mut h = SEED ^ 8u64.wrapping_mul(M);

    k = k.wrapping_mul(M);
    k ^= k >> R;
    k = k.wrapping_mul(M);
    h ^= k;
    h = h.wrapping_mul(M);
    h ^= h >> R;
    h = h.wrapping_mul(M);
    h ^= h >> R;
    h
}

struct Bits<'a> {
    key: &'a [u8],
    words: [u64; FURC_CACHE_SIZE],
    filled: usize,
}

impl<'a> Bits<'a> {
    fn new(key: &'a [u8]) -> Self {
        Self {
            key,
            words: [0u64; FURC_CACHE_SIZE],
            filled: 0,
        }
    }

    fn get(&mut self, idx: u32) -> u32 {
        let ord = (idx >> 6) as usize;
        while self.filled <= ord {
            self.words[self.filled] = if self.filled == 0 {
                murmur_hash_64a(self.key, SEED)
            } else {
                murmur_rehash_64a(self.words[self.filled - 1])
            };
            self.filled += 1;
        }
        ((self.words[ord] >> (idx & 0x3f)) & 0x1) as u32
    }
}

// map `key` to a bucket in `[0,m)`
// - byte for byte compatible with mcrouter's `furc_hash`
// - `m <= 1` always yields 0
pub(crate) fn furc_hash(key: &[u8], m: u32) -> u32 {
    if m <= 1 {
        return 0;
    }

    let mut bits = Bits::new(key);
    // tree depth is the number of bits needed to index `m` buckets
    let mut d = 32 - (m - 1).leading_zeros();
    let mut a = d;

    for _ in 0..MAX_TRIES {
        while bits.get(a) == 0 {
            d -= 1;
            if d == 0 {
                return 0;
            }
            a = d;
        }

        a += FURC_SHIFT;
        let mut num: u32 = 1;
        for _ in 0..(d - 1) {
            num = (num << 1) | bits.get(a);
            a += FURC_SHIFT;
        }

        if num < m {
            return num;
        }
    }
    // give up - return 0
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `(key, m, expected)` generated from mcrouter @ `42aa391189c7`
    /// `lib/fbi/hash.c` (compiled verbatim). Keys cover every `len & 7` tail
    /// case (0..7) plus multi-chunk; `m` spans 1 up to `2^23`.
    const GOLDEN: &[(&str, u32, u32)] = &[
        ("", 1, 0),
        ("", 2, 1),
        ("", 3, 1),
        ("", 5, 1),
        ("", 8, 6),
        ("", 100, 72),
        ("", 1024, 72),
        ("", 8388608, 6173600),
        ("a", 1, 0),
        ("a", 2, 1),
        ("a", 3, 1),
        ("a", 5, 4),
        ("a", 8, 4),
        ("a", 100, 34),
        ("a", 1024, 900),
        ("a", 8388608, 7695180),
        ("ab", 1, 0),
        ("ab", 2, 1),
        ("ab", 3, 2),
        ("ab", 5, 2),
        ("ab", 8, 2),
        ("ab", 100, 35),
        ("ab", 1024, 429),
        ("ab", 8388608, 1749570),
        ("abc", 1, 0),
        ("abc", 2, 0),
        ("abc", 3, 0),
        ("abc", 5, 0),
        ("abc", 8, 5),
        ("abc", 100, 69),
        ("abc", 1024, 985),
        ("abc", 8388608, 6730815),
        ("abcd", 1, 0),
        ("abcd", 2, 0),
        ("abcd", 3, 0),
        ("abcd", 5, 0),
        ("abcd", 8, 0),
        ("abcd", 100, 27),
        ("abcd", 1024, 186),
        ("abcd", 8388608, 4402568),
        ("abcde", 1, 0),
        ("abcde", 2, 0),
        ("abcde", 3, 0),
        ("abcde", 5, 0),
        ("abcde", 8, 0),
        ("abcde", 100, 29),
        ("abcde", 1024, 398),
        ("abcde", 8388608, 2664187),
        ("abcdef", 1, 0),
        ("abcdef", 2, 1),
        ("abcdef", 3, 1),
        ("abcdef", 5, 1),
        ("abcdef", 8, 1),
        ("abcdef", 100, 92),
        ("abcdef", 1024, 92),
        ("abcdef", 8388608, 599355),
        ("abcdefg", 1, 0),
        ("abcdefg", 2, 1),
        ("abcdefg", 3, 2),
        ("abcdefg", 5, 4),
        ("abcdefg", 8, 7),
        ("abcdefg", 100, 28),
        ("abcdefg", 1024, 653),
        ("abcdefg", 8388608, 5319080),
        ("abcdefgh", 1, 0),
        ("abcdefgh", 2, 0),
        ("abcdefgh", 3, 2),
        ("abcdefgh", 5, 2),
        ("abcdefgh", 8, 2),
        ("abcdefgh", 100, 28),
        ("abcdefgh", 1024, 257),
        ("abcdefgh", 8388608, 6241930),
        ("abcdefghi", 1, 0),
        ("abcdefghi", 2, 0),
        ("abcdefghi", 3, 2),
        ("abcdefghi", 5, 2),
        ("abcdefghi", 8, 2),
        ("abcdefghi", 100, 26),
        ("abcdefghi", 1024, 983),
        ("abcdefghi", 8388608, 738669),
        ("abcdefghijklmno", 1, 0),
        ("abcdefghijklmno", 2, 0),
        ("abcdefghijklmno", 3, 2),
        ("abcdefghijklmno", 5, 2),
        ("abcdefghijklmno", 8, 5),
        ("abcdefghijklmno", 100, 15),
        ("abcdefghijklmno", 1024, 313),
        ("abcdefghijklmno", 8388608, 2043024),
        ("abcdefghijklmnop", 1, 0),
        ("abcdefghijklmnop", 2, 0),
        ("abcdefghijklmnop", 3, 0),
        ("abcdefghijklmnop", 5, 0),
        ("abcdefghijklmnop", 8, 0),
        ("abcdefghijklmnop", 100, 49),
        ("abcdefghijklmnop", 1024, 49),
        ("abcdefghijklmnop", 8388608, 7627091),
        ("user:12345", 1, 0),
        ("user:12345", 2, 1),
        ("user:12345", 3, 1),
        ("user:12345", 5, 1),
        ("user:12345", 8, 1),
        ("user:12345", 100, 11),
        ("user:12345", 1024, 239),
        ("user:12345", 8388608, 1362156),
        ("hello world", 1, 0),
        ("hello world", 2, 1),
        ("hello world", 3, 1),
        ("hello world", 5, 4),
        ("hello world", 8, 5),
        ("hello world", 100, 50),
        ("hello world", 1024, 655),
        ("hello world", 8388608, 6738129),
        ("key|#|hashstop", 1, 0),
        ("key|#|hashstop", 2, 1),
        ("key|#|hashstop", 3, 1),
        ("key|#|hashstop", 5, 1),
        ("key|#|hashstop", 8, 5),
        ("key|#|hashstop", 100, 57),
        ("key|#|hashstop", 1024, 463),
        ("key|#|hashstop", 8388608, 4305605),
    ];

    #[test]
    fn matches_mcrouter_golden_vectors() {
        for &(key, m, expected) in GOLDEN {
            assert_eq!(
                furc_hash(key.as_bytes(), m),
                expected,
                "furc_hash({key:?}, {m})"
            );
        }
    }

    #[test]
    fn m_zero_or_one_is_zero() {
        assert_eq!(furc_hash(b"anything", 0), 0);
        assert_eq!(furc_hash(b"anything", 1), 0);
        assert_eq!(furc_hash(b"", 1), 0);
    }

    #[test]
    fn result_is_always_in_range() {
        for m in [2u32, 3, 5, 8, 100, 1024, 4096, FURC_MAX_POOL_SIZE as u32] {
            for i in 0..2000u32 {
                let key = format!("in-range-{i}");
                assert!(furc_hash(key.as_bytes(), m) < m, "m={m} key={key}");
            }
        }
    }

    #[test]
    fn deterministic_across_calls() {
        for m in [2u32, 7, 100, 1024, 65536] {
            for i in 0..500u32 {
                let key = format!("det-{i}");
                assert_eq!(furc_hash(key.as_bytes(), m), furc_hash(key.as_bytes(), m));
            }
        }
    }

    #[test]
    fn distribution_is_not_degenerate() {
        const N: usize = 16;
        let mut counts = [0u32; N];
        for i in 0..20_000u32 {
            let key = format!("dist-{i}");
            counts[furc_hash(key.as_bytes(), N as u32) as usize] += 1;
        }
        for (bucket, &count) in counts.iter().enumerate() {
            assert!(count > 0, "bucket {bucket} received no keys");
        }
    }

    #[test]
    fn consistency_grow_by_one_rehomes_few() {
        let m = 100u32;
        let total = 50_000u32;
        let mut changed = 0u32;
        for i in 0..total {
            let key = format!("consistency-{i}");
            if furc_hash(key.as_bytes(), m) != furc_hash(key.as_bytes(), m + 1) {
                changed += 1;
            }
        }
        // mcrouter's guarantee: growing m -> m+1 re-homes ~1/(m+1) ≈ 0.0099.
        // Generous upper bound guards against a broken descent reshuffling everything.
        let fraction = f64::from(changed) / f64::from(total);
        assert!(
            fraction < 0.03,
            "re-homed fraction {fraction} too high (expected ~0.0099)"
        );
    }
}
