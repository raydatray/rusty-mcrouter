use crate::{bounded::BoundedList, key::Key};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArithmeticRequest {
    pub key: Key,

    // response projections requested from the backend
    pub return_value: bool, // v
    pub return_cas: bool,   // c

    // cache behavior
    pub mode: ArithmeticMode,
    pub delta: u64,                 // D<n>, default 1
    pub initial_value: Option<u64>, // J<n>, requires N
    pub compare_cas: Option<u64>,   // C<n>
    pub override_cas: Option<u64>,  // E<n>

    // order-sensitive N/T/t subset
    pub temporal: ArithmeticTemporalInstructions,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ArithmeticMode {
    Increment, // M<I> or M<+>, the default
    Decrement, // M<D> or M<->
}

/// `ma` has three distinct order-sensitive flags, so its temporal program
/// never exceeds three instructions.
pub type ArithmeticTemporalInstructions = BoundedList<ArithmeticTemporalInstruction, 3>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ArithmeticTemporalInstruction {
    Vivify(i32),    // N<n>
    UpdateTtl(i32), // T<n>
    ReturnTtl,      // t
}
