use std::{cmp::Ordering, ops::Range};

#[derive(Clone, Copy, Eq, PartialEq, PartialOrd)]
struct SmallPrefix(u64);

impl SmallPrefix {
    fn new(prefix: &[u8]) -> Self {
        let mut bytes = [0; 8];

        let len = prefix.len().min(bytes.len());
        bytes[..len].copy_from_slice(&prefix[..len]);

        Self(u64::from_be_bytes(bytes))
    }
}

pub(crate) struct PrefixValue<T> {
    prefix: Vec<u8>,
    value: T,
}

impl<T> PrefixValue<T> {
    pub(crate) fn new(prefix: Vec<u8>, value: T) -> Self {
        Self { prefix, value }
    }
}

struct PrefixRecord {
    bytes: Box<[u8]>,
    prev: Option<usize>,
}

struct SmallPrefixBucket {
    prefix: SmallPrefix,
    indices: Range<usize>,
}

pub(crate) struct LowerBoundPrefixMap<T> {
    prefixes: Vec<PrefixRecord>,
    values: Vec<T>,
    buckets: Vec<SmallPrefixBucket>,
}

impl<T> LowerBoundPrefixMap<T> {
    pub(crate) fn new(mut entries: Vec<PrefixValue<T>>) -> Self {
        entries.sort_by(|l, r| l.prefix.cmp(&r.prefix));

        entries.dedup_by(|curr, prev| {
            if curr.prefix != prev.prefix {
                return false;
            }

            std::mem::swap(curr, prev);
            true
        });

        let (mut prefixes, values): (Vec<PrefixRecord>, Vec<T>) = entries
            .into_iter()
            .map(|entry| {
                (
                    PrefixRecord {
                        bytes: entry.prefix.into_boxed_slice(),
                        prev: None,
                    },
                    entry.value,
                )
            })
            .unzip();

        link_prev_prefixes(&mut prefixes);
        let buckets = build_buckets(&prefixes);

        Self {
            prefixes,
            values,
            buckets,
        }
    }

    pub(crate) fn find_prefix(&self, query: &[u8]) -> Option<&T> {
        let small_prefix = SmallPrefix::new(query);

        let bucket_end = self
            .buckets
            .partition_point(|bucket| bucket.prefix <= small_prefix);

        let indices = &self.buckets[bucket_end - 1].indices;

        let offset = self.prefixes[indices.clone()]
            .partition_point(|record| record.bytes.as_ref().cmp(query) != Ordering::Greater);

        let mut cursor = indices.start + offset;

        while cursor != 0 {
            let candidate = cursor - 1;
            let record = &self.prefixes[candidate];

            if query.starts_with(&record.bytes) {
                return Some(&self.values[candidate]);
            }

            cursor = record.prev.map_or(0, |parent| parent + 1);
        }

        None
    }

    pub(crate) fn keys(&self) -> impl Iterator<Item = &[u8]> {
        self.prefixes.iter().map(|record| record.bytes.as_ref())
    }
}

fn link_prev_prefixes(prefixes: &mut [PrefixRecord]) {
    for idx in 0..prefixes.len() {
        let mut candidate = idx.checked_sub(1);
        let mut prev = None;

        while let Some(candidate_idx) = candidate {
            if prefixes[idx]
                .bytes
                .starts_with(&prefixes[candidate_idx].bytes)
            {
                prev = Some(candidate_idx);
                break;
            }

            candidate = prefixes[candidate_idx].prev
        }
        prefixes[idx].prev = prev;
    }
}

fn build_buckets(prefixes: &[PrefixRecord]) -> Vec<SmallPrefixBucket> {
    let mut buckets = vec![SmallPrefixBucket {
        prefix: SmallPrefix(0),
        indices: 0..0,
    }];

    for (idx, record) in prefixes.iter().enumerate() {
        let prefix = SmallPrefix::new(&record.bytes);
        let last = buckets.last_mut().expect("sentinel bucket exists");

        if last.prefix == prefix {
            last.indices.end = idx + 1;
        } else {
            buckets.push(SmallPrefixBucket {
                prefix,
                indices: idx..idx + 1,
            });
        }
    }

    buckets
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> LowerBoundPrefixMap<i32> {
        LowerBoundPrefixMap::new(vec![
            PrefixValue::new(b"b".to_vec(), 4),
            PrefixValue::new(b"bc".to_vec(), 3),
            PrefixValue::new(b"e".to_vec(), 2),
            PrefixValue::new(b"ef:".to_vec(), 1),
        ])
    }

    #[test]
    fn finds_longest_prefix() {
        let map = fixture();
        assert_eq!(map.find_prefix(b"b"), Some(&4));
        assert_eq!(map.find_prefix(b"bc"), Some(&3));
        assert_eq!(map.find_prefix(b"be"), Some(&4));
        assert_eq!(map.find_prefix(b"bcd"), Some(&3));
        assert_eq!(map.find_prefix(b"da"), None);
        assert_eq!(map.find_prefix(b"ef:a"), Some(&1));
    }

    #[test]
    fn last_duplicate_wins() {
        let map = LowerBoundPrefixMap::new(vec![
            PrefixValue::new(b"abc".to_vec(), 1),
            PrefixValue::new(b"abc".to_vec(), 2),
            PrefixValue::new(b"abc".to_vec(), 3),
        ]);

        assert_eq!(map.find_prefix(b"abcdef"), Some(&3));
    }

    #[test]
    fn empty_prefix_matches_everything() {
        let map = LowerBoundPrefixMap::new(vec![
            PrefixValue::new(Vec::new(), 1),
            PrefixValue::new(b"z".to_vec(), 2),
        ]);

        assert_eq!(map.find_prefix(b"abc"), Some(&1));
        assert_eq!(map.find_prefix(b"zebra"), Some(&2));
    }
}
