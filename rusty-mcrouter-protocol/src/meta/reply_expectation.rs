#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetaReplyExpectation {
    Get(GetSuccessShape),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GetSuccessShape {
    Header,
    Value,
    HeaderOrValue,
}
