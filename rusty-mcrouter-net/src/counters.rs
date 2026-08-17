use std::sync::{
    atomic::{AtomicI64, AtomicU64, Ordering},
    Arc,
};

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
pub struct ProxyCounters {
    // monotonic counters
    pub requests: [[AtomicU64; RESULT_CODE_COUNT]; COMMAND_KIND_COUNT],
    pub latency_us_sum: AtomicU64,
    pub connections_opened: AtomicU64,
    pub connections_closed: AtomicU64,
    pub connect_retries: AtomicU64,
    pub connect_success_after_retry: AtomicU64,
    pub write_batches: AtomicU64,
    pub batched_requests: AtomicU64,
    pub queue_full: AtomicU64,
    pub bytes_read: AtomicU64,
    pub bytes_written: AtomicU64,

    // gauges
    pub pending_reqs: AtomicI64,
    pub inflight_reqs: AtomicI64,
}

impl ProxyCounters {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn record_send(&self, cmd: CommandKind, code: ResultCode, latency_us: u64) {
        self.requests[cmd as usize][code as usize].fetch_add(1, Ordering::Relaxed);
        self.latency_us_sum.fetch_add(latency_us, Ordering::Relaxed);
    }

    pub fn record_result(&self, cmd: CommandKind, code: ResultCode) {
        self.requests[cmd as usize][code as usize].fetch_add(1, Ordering::Relaxed);
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
        let counters = ProxyCounters::new();
        for cmd in 0..COMMAND_KIND_COUNT {
            for code in 0..RESULT_CODE_COUNT {
                let cell = &counters.requests[cmd][code];
                assert_eq!(cell.load(Ordering::Relaxed), 0);
                cell.fetch_add(1, Ordering::Relaxed);
            }
        }
        let total: u64 = counters
            .requests
            .iter()
            .flatten()
            .map(|c| c.load(Ordering::Relaxed))
            .sum();
        assert_eq!(total, (COMMAND_KIND_COUNT * RESULT_CODE_COUNT) as u64);
    }

    #[test]
    fn record_send_accumulates_latency() {
        let counters = ProxyCounters::new();
        counters.record_send(CommandKind::Get, ResultCode::Success, 150);
        counters.record_send(CommandKind::Get, ResultCode::Success, 250);
        counters.record_send(CommandKind::Get, ResultCode::Timeout, 1000);
        assert_eq!(counters.latency_us_sum.load(Ordering::Relaxed), 1400);
        assert_eq!(
            counters.requests[CommandKind::Get as usize][ResultCode::Success as usize]
                .load(Ordering::Relaxed),
            2
        );
    }

    /// gauges go both ways and settle back to zero after a drain.
    #[test]
    fn gauges_return_to_zero() {
        let counters = ProxyCounters::new();
        counters.pending_reqs.fetch_add(3, Ordering::Relaxed);
        counters.pending_reqs.fetch_sub(3, Ordering::Relaxed);
        counters.inflight_reqs.fetch_add(2, Ordering::Relaxed);
        counters.inflight_reqs.fetch_sub(2, Ordering::Relaxed);
        assert_eq!(counters.pending_reqs.load(Ordering::Relaxed), 0);
        assert_eq!(counters.inflight_reqs.load(Ordering::Relaxed), 0);
    }

    /// shards must not share cache lines across threads.
    #[test]
    fn shard_is_cache_line_aligned() {
        assert!(std::mem::align_of::<ProxyCounters>() >= 64);
    }
}
