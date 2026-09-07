use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use bytes::BytesMut;
use hdrhistogram::Histogram;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Notify;
use tokio::task::JoinSet;
use tokio::time::{timeout, Instant};

use crate::codec::{Decoder, ReplyCode, MAX_REPLY_FRAME_BYTES};
use crate::workload::{OpKind, Workload};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const IO_TIMEOUT: Duration = Duration::from_secs(10);
const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);
const WRITE_BATCH_BYTES: usize = 256 * 1024;
const READ_CHUNK_BYTES: usize = 16 * 1024;

#[derive(Clone, Copy, Debug)]
pub enum RunMode {
    Closed,
    Open { requests_per_second: u64 },
}

#[derive(Clone, Debug)]
pub struct RunConfig {
    pub target: String,
    pub connections: usize,
    pub depth: usize,
    pub duration: Duration,
    pub mode: RunMode,
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

#[derive(Clone)]
pub struct RunStats {
    pub ops: [OpStats; 4],
    pub schedule_lag_us: Histogram<u64>,
    pub scheduled: u64,
    pub sent: u64,
    pub completed_in_window: u64,
    pub completed_during_drain: u64,
    pub dropped: u64,
}

impl Default for RunStats {
    fn default() -> Self {
        Self {
            ops: Default::default(),
            schedule_lag_us: histogram(),
            scheduled: 0,
            sent: 0,
            completed_in_window: 0,
            completed_during_drain: 0,
            dropped: 0,
        }
    }
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
        self.schedule_lag_us.add(&other.schedule_lag_us)?;
        self.scheduled += other.scheduled;
        self.sent += other.sent;
        self.completed_in_window += other.completed_in_window;
        self.completed_during_drain += other.completed_during_drain;
        self.dropped += other.dropped;
        Ok(())
    }

    pub fn errors(&self) -> u64 {
        self.ops.iter().map(|stats| stats.errors).sum()
    }
}

pub struct PreparedRun {
    config: RunConfig,
    streams: Vec<TcpStream>,
}

pub async fn prepare(config: RunConfig) -> Result<PreparedRun> {
    validate_config(&config)?;
    let mut tasks = JoinSet::new();
    for index in 0..config.connections {
        let target = config.target.clone();
        tasks.spawn(async move { connect(&target).await.map(|stream| (index, stream)) });
    }
    let mut streams: Vec<Option<TcpStream>> = (0..config.connections).map(|_| None).collect();
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok((index, stream))) => streams[index] = Some(stream),
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
    let streams = streams
        .into_iter()
        .enumerate()
        .map(|(index, stream)| stream.with_context(|| format!("connection {index} was not opened")))
        .collect::<Result<Vec<_>>>()?;
    Ok(PreparedRun { config, streams })
}

impl PreparedRun {
    pub async fn run(self, workload: Arc<Workload>, start: Instant) -> Result<RunStats> {
        let measure_end = start
            .checked_add(self.config.duration)
            .context("duration is too large for the platform clock")?;
        let pacing = match self.config.mode {
            RunMode::Closed => None,
            RunMode::Open {
                requests_per_second,
            } => Some(Pacing::start(
                self.config.connections,
                requests_per_second,
                start,
                measure_end,
            )?),
        };
        let mut tasks = JoinSet::new();
        for (connection, stream) in self.streams.into_iter().enumerate() {
            let workload = workload.clone();
            let mode = match &pacing {
                None => ConnectionMode::Closed,
                Some(pacing) => ConnectionMode::Open {
                    slot: pacing.slots[connection].clone(),
                    first: pacing.first(connection),
                    interval: pacing.per_connection_interval,
                    expected: scheduled_before(
                        measure_end,
                        pacing.first(connection),
                        pacing.per_connection_interval,
                    ),
                },
            };
            let depth = self.config.depth;
            tasks.spawn(async move {
                drive_connection(
                    stream,
                    depth,
                    start,
                    measure_end,
                    workload,
                    connection as u64,
                    mode,
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
        drop(pacing);
        Ok(merged)
    }
}

pub async fn run(config: RunConfig, workload: Arc<Workload>) -> Result<RunStats> {
    let prepared = prepare(config).await?;
    prepared.run(workload, Instant::now()).await
}

#[derive(Debug)]
struct PendingRequest {
    kind: OpKind,
    issued_at: Instant,
}

enum ConnectionMode {
    Closed,
    Open {
        slot: Arc<PaceSlot>,
        first: Instant,
        interval: Duration,
        expected: u64,
    },
}

async fn drive_connection(
    stream: TcpStream,
    depth: usize,
    measure_start: Instant,
    measure_end: Instant,
    workload: Arc<Workload>,
    stream_id: u64,
    mode: ConnectionMode,
) -> Result<RunStats> {
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
    let mut open_sequence = 0u64;

    loop {
        let now = Instant::now();
        let issuing = now < measure_end;
        if issuing {
            match &mode {
                ConnectionMode::Closed => {
                    let room = depth.saturating_sub(pending.len());
                    if room > 0 {
                        issue_closed(
                            room,
                            &mut generator,
                            &mut writer,
                            &mut write_buffer,
                            &mut pending,
                            &mut stats,
                        )
                        .await?;
                    }
                }
                ConnectionMode::Open {
                    slot,
                    first,
                    interval,
                    ..
                } => {
                    let room = depth.saturating_sub(pending.len());
                    let due = slot.due.load(Ordering::Relaxed).min(room as u64) as usize;
                    if due > 0 {
                        issue_open(
                            due,
                            slot,
                            *first,
                            *interval,
                            &mut open_sequence,
                            &mut generator,
                            &mut writer,
                            &mut write_buffer,
                            &mut pending,
                            &mut stats,
                        )
                        .await?;
                    }
                }
            }
        }

        if !issuing && pending.is_empty() {
            break;
        }
        if issuing && pending.is_empty() {
            if let ConnectionMode::Open { slot, .. } = &mode {
                tokio::select! {
                    _ = slot.notify.notified() => continue,
                    _ = tokio::time::sleep_until(measure_end) => continue,
                }
            }
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
        let read = reader.read(&mut read_chunk);
        let bytes_read = match &mode {
            ConnectionMode::Open { slot, .. } if now < measure_end => {
                tokio::select! {
                    biased;
                    result = read => result?,
                    _ = slot.notify.notified() => continue,
                    _ = tokio::time::sleep_until(measure_end) => continue,
                }
            }
            _ => timeout(read_limit, read).await.map_err(|_| {
                anyhow!("read timed out with {} requests in flight", pending.len())
            })??,
        };
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
            let request = pending
                .pop_front()
                .context("received a reply without a matching request")?;
            validate_reply(request.kind, reply)?;
            record_completion(
                &mut stats,
                request.kind,
                reply,
                request.issued_at,
                Instant::now(),
                measure_start,
                measure_end,
            )?;
        }
    }

    if !read_buffer.is_empty() {
        bail!("connection ended with a partial or extra Meta reply");
    }
    match mode {
        ConnectionMode::Closed => {}
        ConnectionMode::Open { expected, .. } => {
            stats.scheduled = expected;
            stats.dropped = expected.saturating_sub(stats.sent);
        }
    }
    Ok(stats)
}

async fn issue_closed(
    room: usize,
    generator: &mut crate::workload::Generator,
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    write_buffer: &mut BytesMut,
    pending: &mut VecDeque<PendingRequest>,
    stats: &mut RunStats,
) -> Result<()> {
    write_buffer.clear();
    let mut kinds = Vec::with_capacity(room);
    while kinds.len() < room && (write_buffer.is_empty() || write_buffer.len() < WRITE_BATCH_BYTES)
    {
        kinds.push(generator.next_request(write_buffer));
    }
    let issued_at = Instant::now();
    io_timeout("write", writer.write_all(write_buffer)).await?;
    for kind in kinds {
        pending.push_back(PendingRequest { kind, issued_at });
        stats.scheduled += 1;
        stats.sent += 1;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn issue_open(
    due: usize,
    slot: &PaceSlot,
    first: Instant,
    interval: Duration,
    sequence: &mut u64,
    generator: &mut crate::workload::Generator,
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    write_buffer: &mut BytesMut,
    pending: &mut VecDeque<PendingRequest>,
    stats: &mut RunStats,
) -> Result<()> {
    write_buffer.clear();
    let mut requests = Vec::with_capacity(due);
    while requests.len() < due
        && (write_buffer.is_empty() || write_buffer.len() < WRITE_BATCH_BYTES)
    {
        let scheduled_at = instant_for_sequence(first, interval, *sequence);
        *sequence += 1;
        requests.push((generator.next_request(write_buffer), scheduled_at));
    }
    slot.due.fetch_sub(requests.len() as u64, Ordering::Relaxed);
    let issued_at = Instant::now();
    io_timeout("write", writer.write_all(write_buffer)).await?;
    for (kind, scheduled_at) in requests {
        let lag = issued_at
            .saturating_duration_since(scheduled_at)
            .as_micros();
        stats
            .schedule_lag_us
            .record(u64::try_from(lag).unwrap_or(u64::MAX).max(1))?;
        pending.push_back(PendingRequest { kind, issued_at });
        stats.sent += 1;
    }
    Ok(())
}

fn record_completion(
    stats: &mut RunStats,
    kind: OpKind,
    reply: ReplyCode,
    issued_at: Instant,
    completed_at: Instant,
    measure_start: Instant,
    measure_end: Instant,
) -> Result<()> {
    if completed_at < measure_start {
        return Ok(());
    }
    if completed_at >= measure_end {
        stats.completed_during_drain += 1;
        return Ok(());
    }
    let op = &mut stats.ops[kind as usize];
    op.count += 1;
    stats.completed_in_window += 1;
    let micros = completed_at
        .saturating_duration_since(issued_at)
        .as_micros();
    op.latency_us
        .record(u64::try_from(micros).unwrap_or(u64::MAX).max(1))?;
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
        OpKind::Ms | OpKind::Md | OpKind::Ma => matches!(
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

#[derive(Debug, Default)]
struct PaceSlot {
    due: AtomicU64,
    notify: Notify,
}

struct Pacing {
    slots: Vec<Arc<PaceSlot>>,
    first_request: Instant,
    global_interval: Duration,
    per_connection_interval: Duration,
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Pacing {
    fn start(
        connections: usize,
        requests_per_second: u64,
        start: Instant,
        end: Instant,
    ) -> Result<Self> {
        let global_interval = Duration::from_secs_f64(1.0 / requests_per_second as f64);
        if global_interval.is_zero() {
            bail!("open-loop request rate exceeds clock resolution");
        }
        let per_connection_interval =
            Duration::from_secs_f64(connections as f64 / requests_per_second as f64);
        let slots: Vec<_> = (0..connections)
            .map(|_| Arc::new(PaceSlot::default()))
            .collect();
        let thread_slots = slots.clone();
        let thread_start = start.into_std();
        let thread_end = end.into_std();
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = stop.clone();
        let tick = Duration::from_micros(100)
            .min(per_connection_interval / 2)
            .max(Duration::from_micros(1));
        let thread = std::thread::Builder::new()
            .name("loadgen-pacer".to_string())
            .spawn(move || {
                let mut granted = vec![0u64; thread_slots.len()];
                while !thread_stop.load(Ordering::Relaxed) {
                    let now = std::time::Instant::now();
                    if now >= thread_end {
                        break;
                    }
                    for (index, slot) in thread_slots.iter().enumerate() {
                        let first = thread_start + global_interval.mul_f64(index as f64);
                        let should = scheduled_before_std(now, first, per_connection_interval);
                        if should > granted[index] {
                            slot.due
                                .fetch_add(should - granted[index], Ordering::Relaxed);
                            granted[index] = should;
                            slot.notify.notify_one();
                        }
                    }
                    std::thread::sleep(tick);
                }
                for (index, slot) in thread_slots.iter().enumerate() {
                    let first = thread_start + global_interval.mul_f64(index as f64);
                    let should = scheduled_before_std(thread_end, first, per_connection_interval);
                    if should > granted[index] {
                        slot.due
                            .fetch_add(should - granted[index], Ordering::Relaxed);
                    }
                    slot.notify.notify_one();
                }
            })
            .context("spawn open-loop pacer")?;
        Ok(Self {
            slots,
            first_request: start,
            global_interval,
            per_connection_interval,
            stop,
            thread: Some(thread),
        })
    }

    fn first(&self, connection: usize) -> Instant {
        self.first_request + self.global_interval.mul_f64(connection as f64)
    }
}

impl Drop for Pacing {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn scheduled_before(end: Instant, first: Instant, interval: Duration) -> u64 {
    scheduled_count(end.saturating_duration_since(first), end > first, interval)
}

fn scheduled_before_std(
    end: std::time::Instant,
    first: std::time::Instant,
    interval: Duration,
) -> u64 {
    scheduled_count(end.saturating_duration_since(first), end > first, interval)
}

fn scheduled_count(elapsed: Duration, started: bool, interval: Duration) -> u64 {
    if !started {
        return 0;
    }
    let count = (elapsed.as_nanos() - 1) / interval.as_nanos() + 1;
    u64::try_from(count).unwrap_or(u64::MAX)
}

fn instant_for_sequence(first: Instant, interval: Duration, sequence: u64) -> Instant {
    first + interval.mul_f64(sequence as f64)
}

fn validate_config(config: &RunConfig) -> Result<()> {
    if config.target.trim().is_empty() {
        bail!("target must not be empty");
    }
    if config.connections == 0 {
        bail!("connections must be greater than zero");
    }
    if config.depth == 0 {
        bail!("depth must be greater than zero");
    }
    if config.duration.is_zero() {
        bail!("duration must be greater than zero");
    }
    if let RunMode::Open {
        requests_per_second,
    } = config.mode
    {
        if requests_per_second == 0 {
            bail!("open-loop requests per second must be greater than zero");
        }
    }
    Ok(())
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
        assert_eq!(stats.completed_in_window, 1);
        assert_eq!(stats.completed_during_drain, 1);
        assert_eq!(stats.ops[OpKind::Mg as usize].misses, 1);
    }

    #[test]
    fn computes_exact_per_connection_schedule() {
        let start = Instant::now();
        let interval = Duration::from_millis(10);
        assert_eq!(scheduled_before(start, start, interval), 0);
        assert_eq!(
            scheduled_before(start + Duration::from_millis(1), start, interval),
            1
        );
        assert_eq!(
            scheduled_before(start + Duration::from_millis(10), start, interval),
            1
        );
        assert_eq!(
            scheduled_before(start + Duration::from_millis(11), start, interval),
            2
        );
    }
}
