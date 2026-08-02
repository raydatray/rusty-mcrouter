//! Per-command parse/encode for the Meta protocol, one module per command,
//! each covering all four codec roles: `parse_request`, `encode_request`,
//! `parse_reply`, and `encode_reply`.

pub mod arithmetic;
pub mod debug;
pub mod delete;
pub mod get;
pub mod store;
