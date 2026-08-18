use std::sync::Arc;

use rusty_mcrouter_observability_primitives::{Counter, Gauge};
use rusty_mcrouter_protocol::Request;

use crate::classify::{ResultCode, RESULT_CODE_COUNT};

#[derive(Copy, Clone, Eq, Debug, PartialEq)]
#[repr(u8)]
pub enum CommandKind {
    Get = 0,
    Store,
    Delete,
    Arithmetic,
    Debug,
    Version,
}

pub const COMMAND_KIND_COUNT: usize = 6;

impl CommandKind {
    pub fn of(request: &Request) -> Self {
        match request {
            Request::Get(_) => CommandKind::Get,
            Request::Store(_) => CommandKind::Store,
            Request::Delete(_) => CommandKind::Delete,
            Request::Arithmetic(_) => CommandKind::Arithmetic,
            Request::Debug(_) => CommandKind::Debug,
        }
    }

    pub const ALL: [CommandKind; COMMAND_KIND_COUNT] = [
        CommandKind::Get,
        CommandKind::Store,
        CommandKind::Delete,
        CommandKind::Arithmetic,
        CommandKind::Debug,
        CommandKind::Version,
    ];

    pub fn prometheus_label(self) -> &'static str {
        match self {
            CommandKind::Get => "mg",
            CommandKind::Store => "ms",
            CommandKind::Delete => "md",
            CommandKind::Arithmetic => "ma",
            CommandKind::Debug => "me",
            CommandKind::Version => "version",
        }
    }
}

#[derive(Default)]
#[repr(align(64))]
// this is one shard. we must align this to a cache line so that no two
// thread's shards share one cache line
pub struct BackendMetricsShard {
    // monotonic counters
    pub requests: [[Counter; RESULT_CODE_COUNT]; COMMAND_KIND_COUNT],
    pub latency_us_sum: Counter,
    pub connections_opened: Counter,
    pub connections_closed: Counter,
    pub connect_retries: Counter,
    pub connect_success_after_retry: Counter,
    pub write_batches: Counter,
    pub batched_requests: Counter,
    pub queue_full: Counter,
    pub bytes_read: Counter,
    pub bytes_written: Counter,

    // gauges
    pub pending_reqs: Gauge,
    pub inflight_reqs: Gauge,
}

impl BackendMetricsShard {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn record_send(&self, cmd: CommandKind, code: ResultCode, latency_us: u64) {
        self.requests[cmd as usize][code as usize].inc();
        self.latency_us_sum.add(latency_us);
    }

    pub fn record_result(&self, cmd: CommandKind, code: ResultCode) {
        self.requests[cmd as usize][code as usize].inc();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// COMMAND_KIND_COUNT indexes the counter array; every discriminant
    /// must fit. (same pattern as classify.rs's RESULT_CODE_COUNT test)
    #[test]
    fn command_discriminants_fit_the_array() {
        for cmd in CommandKind::ALL {
            assert!((cmd as usize) < COMMAND_KIND_COUNT, "for {cmd:?}");
        }
    }

    /// every (command, result) cell is independently addressable -
    /// no two records land in the same slot.
    #[test]
    fn every_command_result_cell_is_distinct() {
        let metrics = BackendMetricsShard::new();
        for cmd in 0..COMMAND_KIND_COUNT {
            for code in 0..RESULT_CODE_COUNT {
                let cell = &metrics.requests[cmd][code];
                assert_eq!(cell.load(), 0);
                cell.inc();
            }
        }
        let total: u64 = metrics.requests.iter().flatten().map(Counter::load).sum();
        assert_eq!(total, (COMMAND_KIND_COUNT * RESULT_CODE_COUNT) as u64);
    }

    #[test]
    fn record_send_accumulates_latency() {
        let metrics = BackendMetricsShard::new();
        metrics.record_send(CommandKind::Get, ResultCode::Success, 150);
        metrics.record_send(CommandKind::Get, ResultCode::Success, 250);
        metrics.record_send(CommandKind::Get, ResultCode::Timeout, 1000);
        assert_eq!(metrics.latency_us_sum.load(), 1400);
        assert_eq!(
            metrics.requests[CommandKind::Get as usize][ResultCode::Success as usize].load(),
            2
        );
    }

    /// gauges go both ways and settle back to zero after a drain.
    #[test]
    fn gauges_return_to_zero() {
        let metrics = BackendMetricsShard::new();
        metrics.pending_reqs.add(3);
        metrics.pending_reqs.sub(3);
        metrics.inflight_reqs.add(2);
        metrics.inflight_reqs.sub(2);
        assert_eq!(metrics.pending_reqs.load(), 0);
        assert_eq!(metrics.inflight_reqs.load(), 0);
    }

    /// shards must not share cache lines across threads.
    #[test]
    fn shard_is_cache_line_aligned() {
        assert!(std::mem::align_of::<BackendMetricsShard>() >= 64);
    }
}
