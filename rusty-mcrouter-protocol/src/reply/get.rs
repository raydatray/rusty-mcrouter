use bytes::Bytes;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GetReply {
    Hit(GetHit), // HD, or VA when a value is returned
    Miss,        // EN
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GetHit {
    pub value: Option<Bytes>,             // v, returned as the VA body
    pub client_flags: Option<u32>,        // f
    pub cas: Option<u64>,                 // c
    pub size: Option<u64>,                // s
    pub ttl: Option<i64>,                 // t
    pub hit_before: Option<bool>,         // h
    pub last_access_seconds: Option<u64>, // l
    pub recache: RecacheState,            // W or Z
    pub stale: bool,                      // X
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RecacheState {
    None,
    Won,        // W
    AlreadyWon, // Z
}
