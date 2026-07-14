#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StoreReply {
    Success(StoreResult),   // HD
    NotStored(StoreResult), // NS
    Exists(StoreResult),    // EX
    NotFound(StoreResult),  // NF
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoreResult {
    pub cas: Option<u64>,  // c
    pub size: Option<u64>, // s
}
