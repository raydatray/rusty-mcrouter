use bytes::{BufMut, Bytes, BytesMut};

use crate::wire::{write_decimal, write_signed_decimal};

// output of `parse_request`, requests that come of the wire
// - multi-key operations only live at the parse boundary
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Parsed {
    One(Request),
    MultiGet(Vec<Bytes>),
}

// a routable request type
// - `Get` is routed as a single key, a multi-key wire `Get` is split into multiple single-key requests
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Request {
    Get {
        key: Bytes,
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
    Replace {
        key: Bytes,
        flags: u32,
        exptime: i32,
        data: Bytes,
    },
    Append {
        key: Bytes,
        flags: u32,
        exptime: i32,
        data: Bytes,
    },
    Prepend {
        key: Bytes,
        flags: u32,
        exptime: i32,
        data: Bytes,
    },
    Incr {
        key: Bytes,
        delta: u64,
    },
    Decr {
        key: Bytes,
        delta: u64,
    },
    Touch {
        key: Bytes,
        exptime: i32,
    },
}

impl Request {
    pub fn serialize_into(&self, out: &mut BytesMut) {
        match self {
            Request::Get { key } => {
                out.put_slice(b"get ");
                out.put_slice(key);
                out.put_slice(b"\r\n");
            }
            Request::Set {
                key,
                flags,
                exptime,
                data,
            } => write_storage(out, b"set", key, *flags, *exptime, data),
            Request::Add {
                key,
                flags,
                exptime,
                data,
            } => write_storage(out, b"add", key, *flags, *exptime, data),
            Request::Replace {
                key,
                flags,
                exptime,
                data,
            } => write_storage(out, b"replace", key, *flags, *exptime, data),
            Request::Append {
                key,
                flags,
                exptime,
                data,
            } => write_storage(out, b"append", key, *flags, *exptime, data),
            Request::Prepend {
                key,
                flags,
                exptime,
                data,
            } => write_storage(out, b"prepend", key, *flags, *exptime, data),
            Request::Delete { key } => {
                out.put_slice(b"delete ");
                out.put_slice(key);
                out.put_slice(b"\r\n");
            }
            Request::Incr { key, delta } => {
                out.put_slice(b"incr ");
                out.put_slice(key);
                out.put_u8(b' ');
                write_decimal(out, *delta);
                out.put_slice(b"\r\n");
            }
            Request::Decr { key, delta } => {
                out.put_slice(b"decr ");
                out.put_slice(key);
                out.put_u8(b' ');
                write_decimal(out, *delta);
                out.put_slice(b"\r\n");
            }
            Request::Touch { key, exptime } => {
                out.put_slice(b"touch ");
                out.put_slice(key);
                out.put_u8(b' ');
                write_signed_decimal(out, *exptime as i64);
                out.put_slice(b"\r\n");
            }
        }
    }
}

fn write_storage(
    out: &mut BytesMut,
    verb: &[u8],
    key: &Bytes,
    flags: u32,
    exptime: i32,
    data: &Bytes,
) {
    out.put_slice(verb);
    out.put_u8(b' ');
    out.put_slice(key);
    out.put_u8(b' ');
    write_decimal(out, flags as u64);
    out.put_u8(b' ');
    write_signed_decimal(out, exptime as i64);
    out.put_u8(b' ');
    write_decimal(out, data.len() as u64);
    out.put_slice(b"\r\n");
    out.put_slice(data);
    out.put_slice(b"\r\n");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures::serialize;

    #[test]
    fn get_single_key_serializes() {
        let req = Request::Get {
            key: Bytes::from_static(b"foo"),
        };
        assert_eq!(serialize(&req).as_ref(), b"get foo\r\n");
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

    #[test]
    fn replace_serializes_to_canonical_wire_format() {
        let req = Request::Replace {
            key: Bytes::from_static(b"foo"),
            flags: 0,
            exptime: 0,
            data: Bytes::from_static(b"bar"),
        };
        assert_eq!(serialize(&req).as_ref(), b"replace foo 0 0 3\r\nbar\r\n");
    }

    #[test]
    fn append_serializes_to_canonical_wire_format() {
        let req = Request::Append {
            key: Bytes::from_static(b"foo"),
            flags: 0,
            exptime: 0,
            data: Bytes::from_static(b"bar"),
        };
        assert_eq!(serialize(&req).as_ref(), b"append foo 0 0 3\r\nbar\r\n");
    }

    #[test]
    fn prepend_serializes_to_canonical_wire_format() {
        let req = Request::Prepend {
            key: Bytes::from_static(b"foo"),
            flags: 0,
            exptime: 0,
            data: Bytes::from_static(b"bar"),
        };
        assert_eq!(serialize(&req).as_ref(), b"prepend foo 0 0 3\r\nbar\r\n");
    }

    #[test]
    fn incr_serializes_to_canonical_wire_format() {
        let req = Request::Incr {
            key: Bytes::from_static(b"foo"),
            delta: 1,
        };
        assert_eq!(serialize(&req).as_ref(), b"incr foo 1\r\n");
    }

    #[test]
    fn incr_serializes_max_delta() {
        let req = Request::Incr {
            key: Bytes::from_static(b"k"),
            delta: u64::MAX,
        };
        assert_eq!(serialize(&req).as_ref(), b"incr k 18446744073709551615\r\n");
    }

    #[test]
    fn decr_serializes_to_canonical_wire_format() {
        let req = Request::Decr {
            key: Bytes::from_static(b"foo"),
            delta: 1,
        };
        assert_eq!(serialize(&req).as_ref(), b"decr foo 1\r\n");
    }

    #[test]
    fn touch_serializes_to_canonical_wire_format() {
        let req = Request::Touch {
            key: Bytes::from_static(b"foo"),
            exptime: 3600,
        };
        assert_eq!(serialize(&req).as_ref(), b"touch foo 3600\r\n");
    }

    #[test]
    fn touch_serializes_negative_exptime() {
        let req = Request::Touch {
            key: Bytes::from_static(b"k"),
            exptime: -1,
        };
        assert_eq!(serialize(&req).as_ref(), b"touch k -1\r\n");
    }
}
