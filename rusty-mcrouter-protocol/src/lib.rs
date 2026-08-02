//! The memcached Meta protocol (`mg`/`ms`/`md`/`ma`/`me`/`mn`), spoken on
//! both hops of the proxy: semantic [`Request`]/[`Reply`] types plus the
//! four codec roles in [`meta`]. Wire behavior is pinned by the test suite
//! and verified against memcached 1.6.45.

mod bounded_list;
mod key;
pub mod meta;
pub mod reply;
pub mod request;

pub use key::Key;
pub use reply::Reply;
pub use request::Request;
