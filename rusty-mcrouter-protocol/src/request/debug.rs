use crate::key::Key;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DebugRequest {
    pub key: Key,
}
