mod errors;
mod policy;

pub(crate) use errors::FailoverErrors;
pub(crate) use policy::{FailoverPolicy, InOrderPolicy};
