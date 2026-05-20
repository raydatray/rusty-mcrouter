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
    Stored,
    NotStored,
    Exists,
    NotFound,
    Deleted,
    // ERROR / CLIENT_ERROR / SERVER_ERROR are modeled as first-class replies
    // (not parser errors) so routes can propagate backend failures
    // semantically instead of dropping the connection on every hiccup.
    Error,
    ClientError(Bytes),
    ServerError(Bytes),
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
            Reply::Stored => out.put_slice(b"STORED\r\n"),
            Reply::NotStored => out.put_slice(b"NOT_STORED\r\n"),
            Reply::Exists => out.put_slice(b"EXISTS\r\n"),
            Reply::NotFound => out.put_slice(b"NOT_FOUND\r\n"),
            Reply::Deleted => out.put_slice(b"DELETED\r\n"),
            Reply::Error => out.put_slice(b"ERROR\r\n"),
            Reply::ClientError(msg) => {
                out.put_slice(b"CLIENT_ERROR ");
                out.put_slice(msg);
                out.put_slice(b"\r\n");
            }
            Reply::ServerError(msg) => {
                out.put_slice(b"SERVER_ERROR ");
                out.put_slice(msg);
                out.put_slice(b"\r\n");
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

    #[test]
    fn storage_acks_serialize_to_status_line() {
        let cases: &[(Reply, &[u8])] = &[
            (Reply::Stored, b"STORED\r\n"),
            (Reply::NotStored, b"NOT_STORED\r\n"),
            (Reply::Exists, b"EXISTS\r\n"),
            (Reply::NotFound, b"NOT_FOUND\r\n"),
            (Reply::Deleted, b"DELETED\r\n"),
        ];
        cases.iter().for_each(|(reply, expected)| {
            assert_eq!(serialize(reply).as_ref(), *expected, "reply={reply:?}");
        });
    }

    #[test]
    fn bare_error_serializes_to_error_line() {
        assert_eq!(serialize(&Reply::Error).as_ref(), b"ERROR\r\n");
    }

    #[test]
    fn client_error_includes_message() {
        let reply = Reply::ClientError(Bytes::from_static(b"bad command line format"));
        assert_eq!(
            serialize(&reply).as_ref(),
            b"CLIENT_ERROR bad command line format\r\n"
        );
    }

    #[test]
    fn server_error_includes_message() {
        let reply = Reply::ServerError(Bytes::from_static(b"out of memory"));
        assert_eq!(
            serialize(&reply).as_ref(),
            b"SERVER_ERROR out of memory\r\n"
        );
    }

    #[test]
    fn error_messages_with_empty_body_still_emit_separator_space() {
        let reply = Reply::ClientError(Bytes::from_static(b""));
        assert_eq!(serialize(&reply).as_ref(), b"CLIENT_ERROR \r\n");
    }
}
