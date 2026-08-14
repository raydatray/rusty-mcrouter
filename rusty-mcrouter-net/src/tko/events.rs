// super temporary event sink for tkos until we figure out observability

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

#[derive(Clone, Copy, Debug)]
pub struct TkoEventRecord<'a> {
    pub event: TkoEvent,
    pub server: &'a str,
    pub pool: Option<&'a str>,
    pub reason: ResultCode,
    pub consecutive_failures: u64,
    pub global_soft_tkos: u64,
    pub global_hard_tkos: u64,
}

pub type TkoEventSink = Box<dyn Fn(&TkoEventRecord<'_>) + Send + Sync>;

pub fn default_sink() -> TkoEventSink {
    Box::new(|rec| eprintln!("tko: {:?} server = {}", rec.event, rec.server))
}
