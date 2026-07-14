mod arithmetic;
mod debug;
mod delete;
mod error;
mod get;
mod store;

pub use arithmetic::{ArithmeticReply, ArithmeticResult};
pub use debug::{DebugField, DebugHit, DebugReply};
pub use delete::DeleteReply;
pub use error::ErrorReply;
pub use get::{GetHit, GetReply, RecacheState};
pub use store::{StoreReply, StoreResult};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Reply {
    Arithmetic(ArithmeticReply),
    Debug(DebugReply),
    Delete(DeleteReply),
    Error(ErrorReply),
    Get(GetReply),
    Store(StoreReply),
}
