from __future__ import annotations

import sys
from dataclasses import asdict, dataclass
from pathlib import Path

import psutil

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

BENCH_DIR = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class ResourceProfile:
    name: str
    container_cpus: str
    container_memory_mb: int
    loadgen_cpus: str
    router_cpus: str
    memcached_cpus: str

    def prefixes(self) -> dict[str, list[str]]:
        return {
            "loadgen": ["taskset", "--cpu-list", self.loadgen_cpus],
            "router": ["taskset", "--cpu-list", self.router_cpus],
            "memcached": ["taskset", "--cpu-list", self.memcached_cpus],
        }

    def requested(self) -> dict:
        return asdict(self)


def load(name: str) -> ResourceProfile:
    document = tomllib.loads((BENCH_DIR / "resources.toml").read_text())
    try:
        raw = document["profile"][name]
    except KeyError as error:
        raise ValueError(f"unknown resource profile {name!r}") from error
    return ResourceProfile(name=name, **raw)


def _read_cgroup(name: str) -> str | None:
    path = Path("/sys/fs/cgroup") / name
    try:
        return path.read_text().strip()
    except OSError:
        return None


def observed() -> dict:
    process = psutil.Process()
    try:
        affinity = process.cpu_affinity()
    except (AttributeError, psutil.Error):
        affinity = []
    return {
        "process_cpu_affinity": affinity,
        "cpuset_cpus_effective": _read_cgroup("cpuset.cpus.effective"),
        "cpu_max": _read_cgroup("cpu.max"),
        "cpu_stat": _read_cgroup("cpu.stat"),
        "memory_max": _read_cgroup("memory.max"),
        "memory_current": _read_cgroup("memory.current"),
        "memory_events": _read_cgroup("memory.events"),
    }


def validate(profile: ResourceProfile) -> dict:
    state = observed()
    allowed = set(state["process_cpu_affinity"])
    required = _parse_cpu_list(profile.container_cpus)
    if allowed and not required.issubset(allowed):
        raise RuntimeError(
            f"profile {profile.name} requires CPUs {sorted(required)}, available {sorted(allowed)}"
        )
    memory_max = state["memory_max"]
    expected_bytes = profile.container_memory_mb * 1024 * 1024
    if memory_max and memory_max != "max" and int(memory_max) < expected_bytes:
        raise RuntimeError(
            f"profile {profile.name} requires {expected_bytes} bytes, cgroup allows {memory_max}"
        )
    return state


def _parse_cpu_list(value: str) -> set[int]:
    result = set()
    for part in value.split(","):
        bounds = part.split("-", 1)
        if len(bounds) == 1:
            result.add(int(bounds[0]))
        else:
            result.update(range(int(bounds[0]), int(bounds[1]) + 1))
    return result
