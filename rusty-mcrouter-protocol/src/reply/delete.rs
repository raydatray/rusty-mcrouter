#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeleteReply {
    Success,  // HD
    Exists,   // EX
    NotFound, // NF
}
