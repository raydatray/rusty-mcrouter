use bytes::Bytes;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DebugReply {
    Hit(DebugHit), // ME
    Miss,          // EN
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DebugHit {
    pub fields: Vec<DebugField>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DebugField {
    pub name: Bytes,
    pub value: Bytes,
}
