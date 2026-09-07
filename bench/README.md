# rusty-mcrouter benchmark harness

The benchmark runs a Rust Meta-protocol load generator, rusty-mcrouter, and
local memcached processes under a Python scenario runner. Linux is the
canonical environment. `run.sh` gives macOS developers the same Linux image
through Docker Desktop.

## Quick start

Docker must be running. On Apple Silicon the image runs as native
`linux/arm64`; CI uses native `linux/amd64`.

```bash
./bench/run.sh doctor
./bench/run.sh test
./bench/run.sh run bench/scenarios/smoke.toml --fresh
```

`run.sh` rebuilds the image through Docker's layer cache, mounts the repository
at `/work`, and forwards all arguments to `rmc-bench`. Build products and uv
caches live under ignored, architecture-specific paths in `bench/.cache/`.

## Components

```text
bench/loadgen/       isolated Rust workspace and load generator
bench/bench/         Python process, scenario, result, and report code
bench/workloads/     deterministic operation/key/value distributions
bench/routes/        router configuration templates
bench/scenarios/     smoke, PR, full, and fault matrices
bench/resources.toml CPU affinity and memory profiles
bench/run.sh         host-to-Linux wrapper
```

The Rust load generator opens all measurement connections before the shared
clock starts. Closed-loop mode keeps a fixed pipeline full. Open-loop mode
uses an absolute schedule and reports schedule-to-issue lag separately from
issue-to-reply latency. Reports contain p50, p90, p99, and p99.9 HDR histogram
quantiles per operation.

The Python runner starts fresh processes for each repetition, waits for the
load generator's `ready` event, snapshots router metrics and CPU, sends `GO`,
and snapshots again at `measure_end`. Prewarm and warmup traffic are therefore
outside the measured router interval.

## Resource profiles

The default `smoke-4cpu` profile uses a four-CPU, 4 GiB container:

```text
CPU 0-1   load generator
CPU 2     rusty-mcrouter
CPU 3     memcached process(es)
```

The `perf-8cpu` profile uses two loadgen CPUs, four router CPUs, two backend
CPUs, and a 6 GiB limit:

```bash
BENCH_PROFILE=perf-8cpu ./bench/run.sh doctor
BENCH_PROFILE=perf-8cpu ./bench/run.sh run bench/scenarios/full.toml --repeat 1 --fresh
```

The harness records requested and effective affinity, cgroup CPU policy,
memory limits/current usage, throttling, and OOM events. A profile fails before
traffic if Docker does not expose its required resources.

Mac and GitHub numbers are not directly comparable. Use same-host paired A/B
runs for directional results. Performance should gate changes only on a
homogeneous dedicated Linux runner; shared CI results remain advisory.

## Scenarios

- `smoke.toml`: fast NullRoute, direct memcached, and 250k RPS PoolRoute checks.
- `pr.toml`: three paired repetitions including a 500k RPS latency path.
- `full.toml`: seven paired repetitions over depth, values, writes, sharding, and prefixes.
- `faults.toml`: SIGSTOP/SIGCONT failover and TKO recovery assertions.

Pull-request and manually dispatched CI runs execute `faults.toml` as a
separate behavioral gate after the performance comparison.

Run a subset with `--only`:

```bash
./bench/run.sh run bench/scenarios/full.toml \
  --only pool1-large-values prefix-3pools \
  --repeat 1 --fresh
```

## A/B comparison

The comparison command accepts explicit binaries so it works with either Git
or Sapling and never changes the active checkout. Paths passed through
`run.sh` must use the container's `/work` path.

```bash
./bench/run.sh compare bench/scenarios/pr.toml \
  /work/bench/.cache/bin/router-base \
  /work/bench/.cache/bin/router-head \
  --base-revision BASE --head-revision HEAD
```

Execution order alternates `base,head` then `head,base`. Reports use paired
ratios and bootstrap intervals. The PR matrix uses three pairs; a verdict is
emitted only when the direct memcached control is stable and all three pairs
are valid. Shared-runner performance verdicts remain advisory.

An A/A check is useful after changing the harness:

```bash
./bench/run.sh compare bench/scenarios/pr.toml \
  /work/bench/.cache/target-arm64/release/rusty-mcrouter \
  /work/bench/.cache/target-arm64/release/rusty-mcrouter \
  --only direct-control null-ceiling --repeat 2
```

## Native development

Python and Rust unit tests can run without Docker:

```bash
cargo test --manifest-path bench/Cargo.toml --workspace --locked
uv run --project bench --locked --no-config --default-index https://pypi.org/simple pytest bench/tests
```

Native benchmark runs require `memcached` and are useful for debugging only:

```bash
cargo build --release --locked -p rusty-mcrouter
cargo build --release --locked --manifest-path bench/Cargo.toml
uv run --project bench --locked --no-config --default-index https://pypi.org/simple \
  rmc-bench run bench/scenarios/smoke.toml --fresh
```

## Results and validity

JSONL records under `bench/results/` include scenario/workload hashes, binary
hashes and revisions, environment/resource observations, loadgen counts and
histograms, router metric deltas and CPU/RSS, memcached stats, applied faults,
and explicit validity reasons.

A run is invalid on process/protocol failure, insufficient completions,
unexpected drops, unusable steady-state schedule lag, missing failover, or
failure to recover TKO where the scenario requires it. Fault scenarios allow
schedule lag caused by intentional backend unavailability and judge behavioral
recovery instead.
