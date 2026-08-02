#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetaReplyExpectation {
    Get(GetSuccessShape),
    Store { cas: bool, size: bool },
    Delete,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GetSuccessShape {
    Header,
    Value,
    HeaderOrValue,
}
