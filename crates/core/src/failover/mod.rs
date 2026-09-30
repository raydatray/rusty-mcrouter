mod errors;
mod policy;

pub(crate) use errors::{code_of_kind, route_code, FailoverErrors};
pub(crate) use policy::{FailoverPolicy, InOrderPolicy, LeastFailuresPolicy};
