use bytes::{BufMut, Bytes, BytesMut};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Request {
    Get { keys: Vec<Bytes> },
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
        }
    }
}
