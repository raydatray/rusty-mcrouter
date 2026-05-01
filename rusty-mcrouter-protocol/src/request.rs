use bytes::Bytes;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Request {
    Get { keys: Vec<Bytes> },
}
