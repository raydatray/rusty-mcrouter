use bytes::{BufMut, Bytes, BytesMut};

use crate::wire::{write_decimal, write_signed_decimal};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Request {
    Get {
        keys: Vec<Bytes>,
    },
    Set {
        key: Bytes,
        flags: u32,
        exptime: i32,
        data: Bytes,
    },
    Delete {
        key: Bytes,
    },
    Add {
        key: Bytes,
        flags: u32,
        exptime: i32,
        data: Bytes,
    },
}

impl Request {
    pub fn serialize_into(&self, out: &mut BytesMut) {
        match self {
            Request::Get { keys } => {
                out.put_slice(b"get");
                keys.iter().for_each(|k| {
                    out.put_u8(b' ');
                    out.put_slice(k);
                });
                out.put_slice(b"\r\n");
            }
            Request::Set {
                key,
                flags,
                exptime,
                data,
            } => {
                out.put_slice(b"set ");
                out.put_slice(key);
                out.put_u8(b' ');
                write_decimal(out, *flags as u64);
                out.put_u8(b' ');
                write_signed_decimal(out, *exptime as i64);
                out.put_u8(b' ');
                write_decimal(out, data.len() as u64);
                out.put_slice(b"\r\n");
                out.put_slice(data);
                out.put_slice(b"\r\n");
            }
            Request::Delete { key } => {
                out.put_slice(b"delete ");
                out.put_slice(key);
                out.put_slice(b"\r\n");
            }
            Request::Add {
                key,
                flags,
                exptime,
                data,
            } => {
                out.put_slice(b"add ");
                out.put_slice(key);
                out.put_u8(b' ');
                write_decimal(out, *flags as u64);
                out.put_u8(b' ');
                write_signed_decimal(out, *exptime as i64);
                out.put_u8(b' ');
                write_decimal(out, data.len() as u64);
                out.put_slice(b"\r\n");
                out.put_slice(data);
                out.put_slice(b"\r\n");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn serialize(req: &Request) -> BytesMut {
        let mut out = BytesMut::new();
        req.serialize_into(&mut out);
        out
    }

    #[test]
    fn get_single_key_serializes() {
        let req = Request::Get {
            keys: vec![Bytes::from_static(b"foo")],
        };
        assert_eq!(serialize(&req).as_ref(), b"get foo\r\n");
    }

    #[test]
    fn get_multiple_keys_space_separated() {
        let req = Request::Get {
            keys: vec![
                Bytes::from_static(b"a"),
                Bytes::from_static(b"bb"),
                Bytes::from_static(b"ccc"),
            ],
        };
        assert_eq!(serialize(&req).as_ref(), b"get a bb ccc\r\n");
    }

    #[test]
    fn set_serializes_to_canonical_wire_format() {
        let req = Request::Set {
            key: Bytes::from_static(b"foo"),
            flags: 0,
            exptime: 0,
            data: Bytes::from_static(b"bar"),
        };
        assert_eq!(serialize(&req).as_ref(), b"set foo 0 0 3\r\nbar\r\n");
    }

    #[test]
    fn set_serializes_flags_and_exptime() {
        let req = Request::Set {
            key: Bytes::from_static(b"k"),
            flags: 42,
            exptime: 3600,
            data: Bytes::from_static(b"v"),
        };
        assert_eq!(serialize(&req).as_ref(), b"set k 42 3600 1\r\nv\r\n");
    }

    #[test]
    fn set_serializes_negative_exptime() {
        let req = Request::Set {
            key: Bytes::from_static(b"k"),
            flags: 0,
            exptime: -1,
            data: Bytes::from_static(b"v"),
        };
        assert_eq!(serialize(&req).as_ref(), b"set k 0 -1 1\r\nv\r\n");
    }

    #[test]
    fn set_serializes_empty_data_with_zero_byte_count() {
        let req = Request::Set {
            key: Bytes::from_static(b"k"),
            flags: 0,
            exptime: 0,
            data: Bytes::from_static(b""),
        };
        assert_eq!(serialize(&req).as_ref(), b"set k 0 0 0\r\n\r\n");
    }

    #[test]
    fn set_data_block_is_binary_safe() {
        let payload: &[u8] = b"\x00\r\nset fake 0 0 0\r\n\xff";
        let req = Request::Set {
            key: Bytes::from_static(b"k"),
            flags: 0,
            exptime: 0,
            data: Bytes::copy_from_slice(payload),
        };
        let mut expected = BytesMut::new();
        expected.extend_from_slice(b"set k 0 0 ");
        expected.extend_from_slice(payload.len().to_string().as_bytes());
        expected.extend_from_slice(b"\r\n");
        expected.extend_from_slice(payload);
        expected.extend_from_slice(b"\r\n");
        assert_eq!(serialize(&req), expected);
    }

    #[test]
    fn set_serializes_max_flags() {
        let req = Request::Set {
            key: Bytes::from_static(b"k"),
            flags: u32::MAX,
            exptime: 0,
            data: Bytes::from_static(b"v"),
        };
        assert_eq!(serialize(&req).as_ref(), b"set k 4294967295 0 1\r\nv\r\n");
    }

    #[test]
    fn delete_serializes_to_canonical_wire_format() {
        let req = Request::Delete {
            key: Bytes::from_static(b"foo"),
        };
        assert_eq!(serialize(&req).as_ref(), b"delete foo\r\n");
    }

    #[test]
    fn add_serializes_to_canonical_wire_format() {
        let req = Request::Add {
            key: Bytes::from_static(b"foo"),
            flags: 0,
            exptime: 0,
            data: Bytes::from_static(b"bar"),
        };
        assert_eq!(serialize(&req).as_ref(), b"add foo 0 0 3\r\nbar\r\n");
    }

    #[test]
    fn add_serializes_flags_and_exptime() {
        let req = Request::Add {
            key: Bytes::from_static(b"k"),
            flags: 42,
            exptime: 3600,
            data: Bytes::from_static(b"v"),
        };
        assert_eq!(serialize(&req).as_ref(), b"add k 42 3600 1\r\nv\r\n");
    }
}
