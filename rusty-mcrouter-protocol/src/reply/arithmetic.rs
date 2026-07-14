#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ArithmeticReply {
    Success(ArithmeticResult),   // HD, or VA when v is requested
    NotStored(ArithmeticResult), // NS
    Exists(ArithmeticResult),    // EX
    NotFound(ArithmeticResult),  // NF
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArithmeticResult {
    pub value: Option<u64>, // v, returned as the VA body
    pub cas: Option<u64>,   // c
    pub ttl: Option<i64>,   // t
}
