use bytes::{Bytes, BytesMut};

use crate::{parse_reply, parse_request, Parsed, Reply, Request, Value};

pub(crate) trait SerializeInto {
    fn serialize_into(&self, out: &mut BytesMut);
}

impl SerializeInto for Request {
    fn serialize_into(&self, out: &mut BytesMut) {
        Request::serialize_into(self, out);
    }
}

impl SerializeInto for Reply {
    fn serialize_into(&self, out: &mut BytesMut) {
        Reply::serialize_into(self, out);
    }
}

pub(crate) fn serialize<T: SerializeInto + ?Sized>(x: &T) -> BytesMut {
    let mut out = BytesMut::new();
    x.serialize_into(&mut out);
    out
}

pub(crate) fn storage(
    verb: &str,
    key: &'static [u8],
    flags: u32,
    exptime: i32,
    data: &'static [u8],
) -> Request {
    let key = Bytes::from_static(key);
    let data = Bytes::from_static(data);
    match verb {
        "set" => Request::Set {
            key,
            flags,
            exptime,
            data,
        },
        "add" => Request::Add {
            key,
            flags,
            exptime,
            data,
        },
        "replace" => Request::Replace {
            key,
            flags,
            exptime,
            data,
        },
        "append" => Request::Append {
            key,
            flags,
            exptime,
            data,
        },
        "prepend" => Request::Prepend {
            key,
            flags,
            exptime,
            data,
        },
        other => panic!("unknown storage verb {other}"),
    }
}

pub(crate) fn get(key: &'static [u8]) -> Request {
    Request::Get {
        key: Bytes::from_static(key),
    }
}

pub(crate) fn value(key: &'static [u8], flags: u32, data: &'static [u8]) -> Value {
    Value {
        key: Bytes::from_static(key),
        flags,
        data: Bytes::from_static(data),
    }
}

pub(crate) fn assert_request_round_trips(original: Request) {
    let mut buf = serialize(&original);
    let parsed = parse_request(&mut buf).unwrap().unwrap();
    assert_eq!(parsed, Parsed::One(original));
    assert!(buf.is_empty());
}

pub(crate) fn assert_reply_round_trips(original: Reply) {
    let mut buf = serialize(&original);
    let parsed = parse_reply(&mut buf).unwrap().unwrap();
    assert_eq!(parsed, original);
    assert!(buf.is_empty());
}
