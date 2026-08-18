use std::sync::Arc;

use rusty_mcrouter_observability_primitives::EventSink;

use crate::classify::ResultCode;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TkoEvent {
    MarkSoftTko,
    MarkHardTko,
    UnMarkTko,
    // destination marked TKO is dropped
    RemoveFromConfig,
    EnterFailOpen,
    ExitFailOpen,
}

#[derive(Clone, Debug)]
pub struct TkoEventRecord {
    pub event: TkoEvent,
    pub server: Arc<str>,
    pub pool: Option<Arc<str>>,
    pub reason: ResultCode,
    pub consecutive_failures: u64,
    pub global_soft_tkos: u64,
    pub global_hard_tkos: u64,
}

pub type TkoEventSink = EventSink<TkoEventRecord>;

pub fn default_sink() -> TkoEventSink {
    TkoEventSink::new(|rec| eprintln!("tko: {:?} server = {}", rec.event, rec.server))
}
