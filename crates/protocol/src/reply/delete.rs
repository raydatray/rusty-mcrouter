#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeleteReply {
    Success,   // HD
    NotStored, // NS
    Exists,    // EX
    NotFound,  // NF
}
