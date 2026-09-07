use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use bytes::BytesMut;
use hdrhistogram::Histogram;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::task::JoinSet;
use tokio::time::{timeout, Instant};

use crate::codec::{Decoder, ReplyCode, MAX_REPLY_FRAME_BYTES};
use crate::workload::{OpKind, Workload};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const IO_TIMEOUT: Duration = Duration::from_secs(10);
const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);
const WRITE_BATCH_BYTES: usize = 256 * 1024;
const READ_CHUNK_BYTES: usize = 16 * 1024;

#[derive(Clone, Debug)]
pub struct RunConfig {
    pub target: String,
    pub connections: usize,
    pub depth: usize,
    pub warmup: Duration,
    pub duration: Duration,
}

#[derive(Clone)]
pub struct OpStats {
    pub latency_us: Histogram<u64>,
    pub count: u64,
    pub hits: u64,
    pub misses: u64,
    pub errors: u64,
}

impl Default for OpStats {
    fn default() -> Self {
        Self {
            latency_us: histogram(),
            count: 0,
            hits: 0,
            misses: 0,
            errors: 0,
        }
    }
}

#[derive(Clone, Default)]
pub struct RunStats {
    pub ops: [OpStats; 4],
    pub sent: u64,
    pub completed: u64,
    pub measured: u64,
}

impl RunStats {
    fn merge(&mut self, other: &Self) -> Result<()> {
        for (left, right) in self.ops.iter_mut().zip(other.ops.iter()) {
            left.latency_us.add(&right.latency_us)?;
            left.count += right.count;
            left.hits += right.hits;
            left.misses += right.misses;
            left.errors += right.errors;
        }
        self.sent += other.sent;
        self.completed += other.completed;
        self.measured += other.measured;
        Ok(())
    }

    pub fn errors(&self) -> u64 {
        self.ops.iter().map(|stats| stats.errors).sum()
    }
}

pub async fn run_closed(config: RunConfig, workload: Arc<Workload>) -> Result<RunStats> {
    let start = Instant::now();
    let measure_start = start
        .checked_add(config.warmup)
        .context("warmup is too large for the platform clock")?;
    let measure_end = measure_start
        .checked_add(config.duration)
        .context("duration is too large for the platform clock")?;
    let mut tasks = JoinSet::new();
    for connection in 0..config.connections {
        let config = config.clone();
        let workload = workload.clone();
        tasks.spawn(async move {
            drive_connection(
                &config.target,
                config.depth,
                measure_start,
                measure_end,
                workload,
                connection as u64,
            )
            .await
            .with_context(|| format!("connection {connection}"))
        });
    }

    let mut merged = RunStats::default();
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(stats)) => merged.merge(&stats)?,
            Ok(Err(error)) => {
                tasks.abort_all();
                return Err(error);
            }
            Err(error) => {
                tasks.abort_all();
                return Err(anyhow!("connection task failed: {error}"));
            }
        }
    }
    Ok(merged)
}

async fn drive_connection(
    target: &str,
    depth: usize,
    measure_start: Instant,
    measure_end: Instant,
    workload: Arc<Workload>,
    stream_id: u64,
) -> Result<RunStats> {
    let stream = connect(target).await?;
    let (mut reader, mut writer) = stream.into_split();
    let mut generator = workload.generator(stream_id);
    let mut pending = VecDeque::with_capacity(depth);
    let mut write_buffer = BytesMut::with_capacity(WRITE_BATCH_BYTES);
    let mut read_buffer = BytesMut::with_capacity(READ_CHUNK_BYTES);
    let mut read_chunk = [0u8; READ_CHUNK_BYTES];
    let decoder = Decoder::new();
    let drain_deadline = measure_end
        .checked_add(DRAIN_TIMEOUT)
        .context("drain deadline is too large for the platform clock")?;
    let mut stats = RunStats::default();

    loop {
        if Instant::now() < measure_end {
            while pending.len() < depth {
                write_buffer.clear();
                while pending.len() < depth
                    && (write_buffer.is_empty() || write_buffer.len() < WRITE_BATCH_BYTES)
                {
                    let sent_at = Instant::now();
                    let kind = generator.next_request(&mut write_buffer);
                    pending.push_back((kind, sent_at));
                    stats.sent += 1;
                }
                io_timeout("write", writer.write_all(&write_buffer)).await?;
                if Instant::now() >= measure_end {
                    break;
                }
            }
        }
        if pending.is_empty() {
            break;
        }

        let now = Instant::now();
        let read_limit = if now >= measure_end {
            if now >= drain_deadline {
                bail!("drain timed out with {} requests in flight", pending.len());
            }
            IO_TIMEOUT.min(drain_deadline.saturating_duration_since(now))
        } else {
            IO_TIMEOUT
        };
        let bytes_read = timeout(read_limit, reader.read(&mut read_chunk))
            .await
            .map_err(|_| anyhow!("read timed out with {} requests in flight", pending.len()))??;
        if bytes_read == 0 {
            bail!(
                "peer closed the connection with {} requests in flight",
                pending.len()
            );
        }
        if read_buffer.len() + bytes_read > MAX_REPLY_FRAME_BYTES + READ_CHUNK_BYTES {
            bail!("reply buffer exceeded its bounded capacity");
        }
        read_buffer.extend_from_slice(&read_chunk[..bytes_read]);

        while let Some(reply) = decoder.decode(&mut read_buffer)? {
            let (kind, sent_at) = pending
                .pop_front()
                .context("received a reply without a matching request")?;
            validate_reply(kind, reply)?;
            let completed_at = Instant::now();
            stats.completed += 1;
            record_completion(
                &mut stats,
                kind,
                reply,
                sent_at,
                completed_at,
                measure_start,
                measure_end,
            )?;
        }
    }

    if !read_buffer.is_empty() {
        bail!("connection ended with a partial or extra Meta reply");
    }
    Ok(stats)
}

fn record_completion(
    stats: &mut RunStats,
    kind: OpKind,
    reply: ReplyCode,
    sent_at: Instant,
    completed_at: Instant,
    measure_start: Instant,
    measure_end: Instant,
) -> Result<()> {
    if completed_at < measure_start || completed_at >= measure_end {
        return Ok(());
    }
    let op = &mut stats.ops[kind as usize];
    op.count += 1;
    stats.measured += 1;
    let micros = completed_at.saturating_duration_since(sent_at).as_micros();
    let micros = u64::try_from(micros).unwrap_or(u64::MAX).max(1);
    op.latency_us.record(micros)?;
    match reply {
        ReplyCode::Value | ReplyCode::Hit => op.hits += 1,
        ReplyCode::End | ReplyCode::NotFound | ReplyCode::NotStored | ReplyCode::Exists => {
            op.misses += 1;
        }
        ReplyCode::Error => op.errors += 1,
    }
    Ok(())
}

fn validate_reply(kind: OpKind, reply: ReplyCode) -> Result<()> {
    let valid = match kind {
        OpKind::Mg => matches!(reply, ReplyCode::Value | ReplyCode::End | ReplyCode::Error),
        OpKind::Ms => matches!(
            reply,
            ReplyCode::Hit
                | ReplyCode::NotFound
                | ReplyCode::NotStored
                | ReplyCode::Exists
                | ReplyCode::Error
        ),
        OpKind::Md | OpKind::Ma => matches!(
            reply,
            ReplyCode::Hit
                | ReplyCode::NotFound
                | ReplyCode::NotStored
                | ReplyCode::Exists
                | ReplyCode::Error
        ),
    };
    if valid {
        Ok(())
    } else {
        bail!("unexpected {reply:?} reply for {} request", kind.name())
    }
}

async fn connect(target: &str) -> Result<TcpStream> {
    let stream = timeout(CONNECT_TIMEOUT, TcpStream::connect(target))
        .await
        .with_context(|| format!("connect to {target} timed out"))?
        .with_context(|| format!("connect to {target}"))?;
    stream.set_nodelay(true).context("set TCP_NODELAY")?;
    Ok(stream)
}

async fn io_timeout<T>(
    operation: &str,
    future: impl std::future::Future<Output = std::io::Result<T>>,
) -> Result<T> {
    timeout(IO_TIMEOUT, future)
        .await
        .map_err(|_| anyhow!("{operation} timed out"))?
        .with_context(|| operation.to_string())
}

pub async fn prewarm(target: &str, workload: Arc<Workload>, connections: usize) -> Result<u64> {
    let total = workload.prewarm_total()?;
    if total == 0 {
        return Ok(0);
    }
    let connections = connections.min(16).min(total as usize).max(1);
    let per_connection = total / connections as u64 + u64::from(total % connections as u64 != 0);
    let keys_per_prefix = workload.prewarm_keys_per_prefix();
    let mut tasks = JoinSet::new();
    for connection in 0..connections {
        let start = connection as u64 * per_connection;
        let end = (start + per_connection).min(total);
        if start == end {
            continue;
        }
        let target = target.to_string();
        let workload = workload.clone();
        tasks.spawn(async move {
            prewarm_connection(
                &target,
                workload,
                connection as u64,
                start,
                end,
                keys_per_prefix,
            )
            .await
            .with_context(|| format!("prewarm connection {connection}"))
        });
    }

    let mut completed = 0;
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(count)) => completed += count,
            Ok(Err(error)) => {
                tasks.abort_all();
                return Err(error);
            }
            Err(error) => {
                tasks.abort_all();
                return Err(anyhow!("prewarm task failed: {error}"));
            }
        }
    }
    Ok(completed)
}

async fn prewarm_connection(
    target: &str,
    workload: Arc<Workload>,
    stream_id: u64,
    start: u64,
    end: u64,
    keys_per_prefix: u64,
) -> Result<u64> {
    let stream = connect(target).await?;
    let (mut reader, mut writer) = stream.into_split();
    let mut generator = workload.generator(1_000_000 + stream_id);
    let mut next = start;
    let mut outstanding = 0usize;
    let mut completed = 0u64;
    let mut write_buffer = BytesMut::with_capacity(WRITE_BATCH_BYTES);
    let mut read_buffer = BytesMut::with_capacity(READ_CHUNK_BYTES);
    let mut read_chunk = [0u8; READ_CHUNK_BYTES];
    let decoder = Decoder::new();

    while next < end || outstanding > 0 {
        write_buffer.clear();
        while next < end
            && outstanding < 128
            && (write_buffer.is_empty() || write_buffer.len() < WRITE_BATCH_BYTES)
        {
            let prefix = (next / keys_per_prefix) as usize;
            let index = next % keys_per_prefix + 1;
            generator.prewarm_request(prefix, index, &mut write_buffer);
            next += 1;
            outstanding += 1;
        }
        if !write_buffer.is_empty() {
            io_timeout("prewarm write", writer.write_all(&write_buffer)).await?;
        }
        if outstanding == 0 {
            continue;
        }
        let bytes_read = timeout(IO_TIMEOUT, reader.read(&mut read_chunk))
            .await
            .map_err(|_| {
                anyhow!("prewarm read timed out with {outstanding} requests in flight")
            })??;
        if bytes_read == 0 {
            bail!("peer closed during prewarm with {outstanding} requests in flight");
        }
        read_buffer.extend_from_slice(&read_chunk[..bytes_read]);
        while let Some(reply) = decoder.decode(&mut read_buffer)? {
            if reply != ReplyCode::Hit {
                bail!("prewarm store returned {reply:?}");
            }
            outstanding = outstanding
                .checked_sub(1)
                .context("prewarm received an unmatched reply")?;
            completed += 1;
        }
    }
    if !read_buffer.is_empty() {
        bail!("prewarm ended with a partial or extra Meta reply");
    }
    Ok(completed)
}

fn histogram() -> Histogram<u64> {
    Histogram::new_with_bounds(1, 60_000_000, 3).expect("valid histogram bounds")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_only_completions_inside_measurement_window() {
        let start = Instant::now();
        let end = start + Duration::from_secs(1);
        let mut stats = RunStats::default();
        record_completion(
            &mut stats,
            OpKind::Mg,
            ReplyCode::End,
            start,
            start - Duration::from_nanos(1),
            start,
            end,
        )
        .unwrap();
        record_completion(
            &mut stats,
            OpKind::Mg,
            ReplyCode::End,
            start,
            start,
            start,
            end,
        )
        .unwrap();
        record_completion(
            &mut stats,
            OpKind::Mg,
            ReplyCode::End,
            start,
            end,
            start,
            end,
        )
        .unwrap();
        assert_eq!(stats.measured, 1);
        assert_eq!(stats.ops[OpKind::Mg as usize].misses, 1);
    }
}
