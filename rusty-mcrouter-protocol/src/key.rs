use bytes::Bytes;

use crate::errors::KeyError;

const HASH_STOP: &[u8] = b"|#|";
pub const MAX_KEY_BYTES: usize = 250;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Key {
    bytes: Bytes,
    routing_prefix_len: usize,
    routing_key_end: usize,
}

impl Key {
    pub fn new(bytes: Bytes) -> Result<Self, KeyError> {
        if bytes.is_empty() {
            return Err(KeyError::Empty);
        }

        if bytes.len() > MAX_KEY_BYTES {
            return Err(KeyError::TooLong {
                actual: bytes.len(),
                maximum: MAX_KEY_BYTES,
            });
        }

        let routing_prefix_len = routing_prefix_len(&bytes);
        let routing_key_end = bytes[routing_prefix_len..]
            .windows(HASH_STOP.len())
            .position(|window| window == HASH_STOP)
            .map_or(bytes.len(), |position| routing_prefix_len + position);

        Ok(Self {
            bytes,
            routing_prefix_len,
            routing_key_end,
        })
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn into_bytes(self) -> Bytes {
        self.bytes
    }

    pub fn routing_prefix(&self) -> Option<&[u8]> {
        (self.routing_prefix_len != 0).then(|| &self.bytes[..self.routing_prefix_len])
    }

    pub fn key_without_routing_prefix(&self) -> &[u8] {
        &self.bytes[self.routing_prefix_len..]
    }

    pub fn routing_key(&self) -> &[u8] {
        &self.bytes[self.routing_prefix_len..self.routing_key_end]
    }

    pub fn hash_stop_suffix(&self) -> Option<&[u8]> {
        (self.routing_key_end != self.bytes.len()).then(|| &self.bytes[self.routing_key_end..])
    }
}

fn routing_prefix_len(key: &[u8]) -> usize {
    if key.first() != Some(&b'/') {
        return 0;
    }

    let Some(first_segment_end) = key[1..].iter().position(|byte| *byte == b'/') else {
        return 0;
    };

    let second_segment_start = first_segment_end + 2;
    let Some(second_segment_end) = key[second_segment_start..]
        .iter()
        .position(|byte| *byte == b'/')
    else {
        return 0;
    };

    second_segment_start + second_segment_end + 1
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(bytes: &'static [u8]) -> Key {
        Key::new(Bytes::from_static(bytes)).unwrap()
    }

    #[test]
    fn plain_key_has_no_optional_parts() {
        let key = key(b"user:1");

        assert_eq!(key.as_bytes(), b"user:1");
        assert_eq!(key.routing_prefix(), None);
        assert_eq!(key.key_without_routing_prefix(), b"user:1");
        assert_eq!(key.routing_key(), b"user:1");
        assert_eq!(key.hash_stop_suffix(), None);
    }

    #[test]
    fn exposes_routing_prefix_and_hash_stop_suffix() {
        let key = key(b"/region/cluster/user:1|#|suffix");

        assert_eq!(key.routing_prefix(), Some(b"/region/cluster/".as_slice()));
        assert_eq!(key.key_without_routing_prefix(), b"user:1|#|suffix");
        assert_eq!(key.routing_key(), b"user:1");
        assert_eq!(key.hash_stop_suffix(), Some(b"|#|suffix".as_slice()));
    }

    #[test]
    fn hash_stop_may_begin_at_zero() {
        let key = key(b"|#|suffix");

        assert_eq!(key.routing_key(), b"");
        assert_eq!(key.hash_stop_suffix(), Some(b"|#|suffix".as_slice()));
    }

    #[test]
    fn rejects_empty_and_oversized_keys() {
        assert_eq!(Key::new(Bytes::new()), Err(KeyError::Empty));
        assert_eq!(
            Key::new(Bytes::from(vec![b'x'; MAX_KEY_BYTES + 1])),
            Err(KeyError::TooLong {
                actual: MAX_KEY_BYTES + 1,
                maximum: MAX_KEY_BYTES,
            })
        );
    }
}
