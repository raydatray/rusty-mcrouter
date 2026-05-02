use bytes::Bytes;

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
