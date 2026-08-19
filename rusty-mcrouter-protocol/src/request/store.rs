use bytes::Bytes;

use crate::Key;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoreRequest {
    pub key: Key,
    pub value: Bytes,

    // response projections requested from the backend
    pub return_cas: bool,  // c
    pub return_size: bool, // s

    // cache behavior
    pub mode: StoreMode,
    pub client_flags: Option<u32>, // F<n>
    pub ttl: Option<i32>,          // T<n>
    pub compare_cas: Option<u64>,  // C<n>
    pub override_cas: Option<u64>, // E<n>
    pub invalidate: bool,          // I
    pub vivify_ttl: Option<i32>,   // N<n>
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StoreMode {
    Set,     // M<S>, the default
    Add,     // M<E>
    Replace, // M<R>
    Append,  // M<A>
    Prepend, // M<P>
}
