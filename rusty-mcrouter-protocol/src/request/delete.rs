use crate::Key;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteRequest {
    pub key: Key,

    // cache behavior
    pub compare_cas: Option<u64>,  // C<n>
    pub override_cas: Option<u64>, // E<n>
    pub client_flags: Option<u32>, // F<n>, used by x tombstones
    pub invalidate: bool,          // I
    pub ttl: Option<i32>,          // T<n>
    pub remove_value: bool,        // x
}
