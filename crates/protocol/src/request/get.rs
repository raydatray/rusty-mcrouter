use crate::bounded_list::BoundedList;
use crate::Key;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GetRequest {
    pub key: Key,

    // response projections requested from the backend
    pub return_value: bool,        // v
    pub return_client_flags: bool, // f
    pub return_cas: bool,          // c
    pub return_size: bool,         // s
    pub return_hit_state: bool,    // h
    pub return_last_access: bool,  // l

    // cache behavior
    pub check_cas: Option<u64>,    // C<n>
    pub override_cas: Option<u64>, // E<n>
    pub no_lru_bump: bool,         // u

    // order-sensitive N/T/t/R subset
    pub temporal: GetTemporalInstructions,
}

/// `mg` has four distinct order-sensitive flags, so its temporal program
/// never exceeds four instructions.
pub type GetTemporalInstructions = BoundedList<GetTemporalInstruction, 4>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GetTemporalInstruction {
    Vivify(i32),        // N<n>
    UpdateTtl(i32),     // T<n>
    ReturnTtl,          // t
    WinForRecache(i32), // R<n>
}
