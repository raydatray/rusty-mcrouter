use std::sync::{
    atomic::{AtomicI64, AtomicU64},
    Arc,
};

use rusty_mcrouter_backend::counters::COMMAND_KIND_COUNT;

#[derive(Default)]
pub struct FrontendCounters {
    pub request: [AtomicU64; COMMAND_KIND_COUNT],
    pub noops: AtomicU64,
    pub parse_errors: AtomicU64,
    pub failed: AtomicU64,
    pub client_connections: AtomicI64,
    pub processing: AtomicI64,
}

impl FrontendCounters {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
}
