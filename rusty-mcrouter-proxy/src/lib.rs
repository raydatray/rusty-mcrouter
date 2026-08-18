//! Frontend protocol handling and proxy-thread orchestration.

mod counters;
mod events;

pub use counters::FrontendCounters;
pub use events::{WorkerEvent, WorkerEventRecord, WorkerEventSink};
