use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::Parser;
use hdrhistogram::Histogram;
use rusty_mcrouter_loadgen::runner::{self, RunConfig, RunStats};
use rusty_mcrouter_loadgen::workload::{OpKind, WorkloadSpec};
use serde::Serialize;

#[derive(Debug, Parser)]
#[command(about = "Closed-loop Meta protocol load generator")]
struct Args {
    #[arg(long)]
    target: String,
    #[arg(long)]
    workload: PathBuf,
    #[arg(long, default_value_t = 32)]
    connections: usize,
    #[arg(long, default_value_t = 16)]
    depth: usize,
    #[arg(long, default_value_t = 2)]
    threads: usize,
    #[arg(long, alias = "duration-s", default_value_t = 10.0)]
    duration: f64,
    #[arg(long, alias = "warmup-s", default_value_t = 2.0)]
    warmup: f64,
    #[arg(long)]
    seed: Option<u64>,
    #[arg(long)]
    no_prewarm: bool,
    #[arg(long)]
    out: Option<PathBuf>,
}

impl Args {
    fn validate(&self) -> Result<(Duration, Duration)> {
        if self.target.trim().is_empty() {
            bail!("--target must not be empty");
        }
        if self.connections == 0 {
            bail!("--connections must be greater than zero");
        }
        if self.depth == 0 {
            bail!("--depth must be greater than zero");
        }
        if self.threads == 0 {
            bail!("--threads must be greater than zero");
        }
        if !self.duration.is_finite() || self.duration <= 0.0 {
            bail!("--duration must be finite and greater than zero");
        }
        if !self.warmup.is_finite() || self.warmup < 0.0 {
            bail!("--warmup must be finite and non-negative");
        }
        let duration = Duration::try_from_secs_f64(self.duration)
            .context("--duration is outside the supported range")?;
        let warmup = Duration::try_from_secs_f64(self.warmup)
            .context("--warmup is outside the supported range")?;
        Ok((duration, warmup))
    }
}

#[derive(Serialize)]
struct Report {
    schema_version: u32,
    target: String,
    workload: String,
    connections: usize,
    depth: usize,
    threads: usize,
    seed: u64,
    duration_seconds: f64,
    warmup_seconds: f64,
    achieved_requests_per_second: f64,
    counts: Counts,
    latency_us: Percentiles,
    ops: BTreeMap<&'static str, OpReport>,
}

#[derive(Serialize)]
struct Counts {
    sent: u64,
    completed: u64,
    measured: u64,
    prewarmed: u64,
    hits: u64,
    misses: u64,
    errors: u64,
}

#[derive(Serialize)]
struct OpReport {
    count: u64,
    hits: u64,
    misses: u64,
    errors: u64,
    latency_us: Percentiles,
}

#[derive(Serialize)]
struct Percentiles {
    count: u64,
    p50: u64,
    p90: u64,
    p99: u64,
    p999: u64,
    max: u64,
    mean: f64,
}

impl Percentiles {
    fn from(histogram: &Histogram<u64>) -> Self {
        Self {
            count: histogram.len(),
            p50: histogram.value_at_quantile(0.50),
            p90: histogram.value_at_quantile(0.90),
            p99: histogram.value_at_quantile(0.99),
            p999: histogram.value_at_quantile(0.999),
            max: histogram.max(),
            mean: histogram.mean(),
        }
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    let (duration, warmup) = args.validate()?;
    let source = std::fs::read_to_string(&args.workload)
        .with_context(|| format!("read {}", args.workload.display()))?;
    let spec = WorkloadSpec::parse(&source)?;
    let mut workload = spec.validate()?;
    if let Some(seed) = args.seed {
        workload = workload.with_seed(seed);
    }
    let workload = Arc::new(workload);
    let seed = workload.seed;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.threads)
        .enable_all()
        .build()
        .context("build Tokio runtime")?;
    let target = args.target.clone();
    let workload_for_run = workload.clone();
    let config = RunConfig {
        target: target.clone(),
        connections: args.connections,
        depth: args.depth,
        warmup,
        duration,
    };
    let (prewarmed, stats) = runtime.block_on(async move {
        let prewarmed = if args.no_prewarm {
            0
        } else {
            runner::prewarm(&target, workload_for_run.clone(), args.connections).await?
        };
        let stats = runner::run_closed(config, workload_for_run).await?;
        Ok::<_, anyhow::Error>((prewarmed, stats))
    })?;
    let report = make_report(&args, seed, prewarmed, &stats)?;
    let json = serde_json::to_string_pretty(&report).context("serialize report")? + "\n";
    if let Some(path) = &args.out {
        std::fs::write(path, json).with_context(|| format!("write {}", path.display()))?;
    } else {
        print!("{json}");
    }
    eprintln!(
        "completed {} measured requests at {:.0} req/s (p99 {} us, {} errors)",
        report.counts.measured,
        report.achieved_requests_per_second,
        report.latency_us.p99,
        report.counts.errors
    );
    Ok(())
}

fn make_report(args: &Args, seed: u64, prewarmed: u64, stats: &RunStats) -> Result<Report> {
    let mut total_latency = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3)?;
    let mut ops = BTreeMap::new();
    let mut hits = 0;
    let mut misses = 0;
    let mut errors = 0;
    for kind in OpKind::ALL {
        let op = &stats.ops[kind as usize];
        total_latency.add(&op.latency_us)?;
        hits += op.hits;
        misses += op.misses;
        errors += op.errors;
        ops.insert(
            kind.name(),
            OpReport {
                count: op.count,
                hits: op.hits,
                misses: op.misses,
                errors: op.errors,
                latency_us: Percentiles::from(&op.latency_us),
            },
        );
    }
    Ok(Report {
        schema_version: 1,
        target: args.target.clone(),
        workload: args.workload.display().to_string(),
        connections: args.connections,
        depth: args.depth,
        threads: args.threads,
        seed,
        duration_seconds: args.duration,
        warmup_seconds: args.warmup,
        achieved_requests_per_second: stats.measured as f64 / args.duration,
        counts: Counts {
            sent: stats.sent,
            completed: stats.completed,
            measured: stats.measured,
            prewarmed,
            hits,
            misses,
            errors,
        },
        latency_us: Percentiles::from(&total_latency),
        ops,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn cli_definition_is_valid() {
        Args::command().debug_assert();
    }

    #[test]
    fn rejects_nonfinite_and_zero_arguments() {
        let args = Args::try_parse_from([
            "loadgen",
            "--target",
            "127.0.0.1:1",
            "--workload",
            "workload.toml",
            "--connections",
            "0",
            "--duration",
            "NaN",
        ])
        .unwrap();
        assert!(args.validate().is_err());
    }
}
