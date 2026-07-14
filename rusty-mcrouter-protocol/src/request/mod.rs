mod arithmetic;
mod debug;
mod delete;
mod get;
mod store;

use crate::key::Key;

pub use arithmetic::{
    ArithmeticMode, ArithmeticRequest, ArithmeticTemporalInstruction,
    ArithmeticTemporalInstructions,
};
pub use debug::DebugRequest;
pub use delete::DeleteRequest;
pub use get::{GetRequest, GetTemporalInstruction, GetTemporalInstructions};
pub use store::{StoreMode, StoreRequest};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Request {
    Arithmetic(ArithmeticRequest),
    Delete(DeleteRequest),
    Debug(DebugRequest),
    Get(GetRequest),
    Store(StoreRequest),
}

impl Request {
    pub fn key(&self) -> &Key {
        match self {
            Self::Arithmetic(request) => &request.key,
            Self::Delete(request) => &request.key,
            Self::Debug(request) => &request.key,
            Self::Get(request) => &request.key,
            Self::Store(request) => &request.key,
        }
    }
}
