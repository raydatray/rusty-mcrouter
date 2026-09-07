#!/usr/bin/env bash
set -euo pipefail

repo="$(cd "$(dirname "$0")/.." && pwd)"
image="rusty-mcrouter-bench:rust-1.91.1"
profile="${BENCH_PROFILE:-smoke-4cpu}"
arch="$(uname -m)"

case "$profile" in
  smoke-4cpu)
    cpus="0-3"
    memory="4096m"
    ;;
  perf-8cpu)
    cpus="0-7"
    memory="6144m"
    ;;
  *)
    echo "unknown BENCH_PROFILE: $profile" >&2
    exit 2
    ;;
esac

docker build \
  --file "$repo/bench/docker/Dockerfile" \
  --tag "$image" \
  "$repo/bench/docker"

if [[ "${1:-}" == "image" ]]; then
  exit 0
fi

mkdir -p "$repo/bench/.cache"

if [[ "${1:-}" == "exec" ]]; then
  shift
  command=("$@")
else
  command=(uv run --project bench --locked rmc-bench "$@")
fi

exec docker run --rm --init \
  --user "$(id -u):$(id -g)" \
  --cpuset-cpus "$cpus" \
  --memory "$memory" \
  --memory-swap "$memory" \
  --pids-limit 4096 \
  --ulimit nofile=65536:65536 \
  --security-opt no-new-privileges \
  --mount "type=bind,src=$repo,dst=/work" \
  --workdir /work \
  --env "BENCH_PROFILE=$profile" \
  --env "CARGO_HOME=/work/bench/.cache/cargo-$arch" \
  --env "CARGO_TARGET_DIR=/work/bench/.cache/target-$arch" \
  --env "UV_PROJECT_ENVIRONMENT=/work/bench/.cache/venv-$arch" \
  --env "UV_CACHE_DIR=/work/bench/.cache/uv-$arch" \
  "$image" \
  "${command[@]}"
