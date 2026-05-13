use bytes::{BufMut, Bytes, BytesMut};

use crate::wire::write_decimal;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Value {
    pub key: Bytes,
    pub flags: u32,
    pub data: Bytes,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Reply {
    Get { hits: Vec<Value> },
}

impl Reply {
    pub fn serialize_into(&self, out: &mut BytesMut) {
        match self {
            Reply::Get { hits } => {
                hits.iter().for_each(|v| {
                    out.put_slice(b"VALUE ");
                    out.put_slice(&v.key);
                    out.put_u8(b' ');
                    write_decimal(out, v.flags as u64);
                    out.put_u8(b' ');
                    write_decimal(out, v.data.len() as u64);
                    out.put_slice(b"\r\n");
                    out.put_slice(&v.data);
                    out.put_slice(b"\r\n");
                });
                out.put_slice(b"END\r\n");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn val(key: &'static [u8], flags: u32, data: &'static [u8]) -> Value {
        Value {
            key: Bytes::from_static(key),
            flags,
            data: Bytes::from_static(data),
        }
    }

    fn serialize(reply: &Reply) -> BytesMut {
        let mut out = BytesMut::new();
        reply.serialize_into(&mut out);
        out
    }

    #[test]
    fn miss_serializes_to_end_only() {
        let reply = Reply::Get { hits: vec![] };
        assert_eq!(serialize(&reply).as_ref(), b"END\r\n");
    }

    #[test]
    fn single_hit_matches_mcrouter_fixture() {
        let reply = Reply::Get {
            hits: vec![val(b"t", 10, b"te")],
        };
        assert_eq!(serialize(&reply).as_ref(), b"VALUE t 10 2\r\nte\r\nEND\r\n");
    }

    #[test]
    fn empty_value_writes_zero_byte_count() {
        let reply = Reply::Get {
            hits: vec![val(b"t", 5, b"")],
        };
        assert_eq!(serialize(&reply).as_ref(), b"VALUE t 5 0\r\n\r\nEND\r\n");
    }

    #[test]
    fn larger_flags_and_value_match_mcrouter_fixture() {
        let reply = Reply::Get {
            hits: vec![val(b"test", 15889, b"test ")],
        };
        assert_eq!(
            serialize(&reply).as_ref(),
            b"VALUE test 15889 5\r\ntest \r\nEND\r\n"
        );
    }

    #[test]
    fn zero_flags_serialize_as_literal_zero() {
        let reply = Reply::Get {
            hits: vec![val(b"k", 0, b"v")],
        };
        assert_eq!(serialize(&reply).as_ref(), b"VALUE k 0 1\r\nv\r\nEND\r\n");
    }

    #[test]
    fn multiple_hits_concatenate_with_single_terminating_end() {
        let reply = Reply::Get {
            hits: vec![
                val(b"a", 1, b"AA"),
                val(b"bb", 2, b"BBB"),
                val(b"ccc", 3, b"CCCC"),
            ],
        };
        assert_eq!(
            serialize(&reply).as_ref(),
            b"VALUE a 1 2\r\nAA\r\n\
              VALUE bb 2 3\r\nBBB\r\n\
              VALUE ccc 3 4\r\nCCCC\r\n\
              END\r\n"
        );
    }

    #[test]
    fn data_block_is_binary_safe() {
        // Data is a counted byte block, not a line. NULs, CRLFs, and bytes
        // shaped like protocol keywords must pass through unaltered.
        let payload: &[u8] = b"\x00\r\nVALUE fake 0 0\r\nEND\r\n\x01\xff";
        let reply = Reply::Get {
            hits: vec![val(b"k", 0, payload)],
        };

        let mut expected = BytesMut::new();
        expected.extend_from_slice(b"VALUE k 0 ");
        expected.extend_from_slice(payload.len().to_string().as_bytes());
        expected.extend_from_slice(b"\r\n");
        expected.extend_from_slice(payload);
        expected.extend_from_slice(b"\r\nEND\r\n");

        assert_eq!(serialize(&reply), expected);
    }

    #[test]
    fn serialize_into_appends_without_clobbering_existing_bytes() {
        let mut out = BytesMut::from(&b"prefix:"[..]);
        Reply::Get {
            hits: vec![val(b"k", 0, b"v")],
        }
        .serialize_into(&mut out);
        assert_eq!(out.as_ref(), b"prefix:VALUE k 0 1\r\nv\r\nEND\r\n");
    }

}
