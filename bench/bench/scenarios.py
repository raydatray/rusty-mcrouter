from __future__ import annotations

import json
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

BENCH_DIR = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class LoadgenConfig:
    threads: int = 2
    connections: int = 16
    depth: int = 16


@dataclass(frozen=True)
class MemcachedConfig:
    count: int = 1
    threads: int = 1
    memory_mb: int = 256


@dataclass(frozen=True)
class RouterConfig:
    num_proxies: int = 2
    listening_sockets: int = 1
    extra_args: tuple[str, ...] = ()


@dataclass(frozen=True)
class Expectations:
    min_completed: int = 1
    max_protocol_errors: int = 0
    max_dropped: int = 0
    max_schedule_lag_p99_us: int | None = None
    min_failovers: int = 0
    max_tko_final: int | None = None


@dataclass(frozen=True)
class Fault:
    at_seconds: float
    action: str
    target: int


@dataclass(frozen=True)
class Scenario:
    name: str
    route: str
    workload: str
    mode: str
    requests_per_second: int | None
    duration_seconds: float
    warmup_seconds: float
    repeat: int
    loadgen: LoadgenConfig
    memcached: MemcachedConfig
    router: RouterConfig
    expect: Expectations
    faults: tuple[Fault, ...]

    def workload_path(self) -> Path:
        return BENCH_DIR / "workloads" / f"{self.workload}.toml"

    def resolved(self) -> dict[str, Any]:
        return asdict(self)


def _merged(defaults: dict[str, Any], override: dict[str, Any] | None) -> dict[str, Any]:
    result = dict(defaults)
    result.update(override or {})
    return result


def _mode(raw: Any) -> tuple[str, int | None]:
    if raw == "closed":
        return "closed", None
    if isinstance(raw, dict) and set(raw) == {"open"}:
        requests_per_second = int(raw["open"]["requests_per_second"])
        if requests_per_second <= 0:
            raise ValueError("open-loop requests_per_second must be positive")
        return "open", requests_per_second
    raise ValueError(f"invalid mode: {raw!r}")


def load(path: Path, only: set[str] | None = None) -> list[Scenario]:
    document = tomllib.loads(path.read_text())
    defaults = document.get("defaults", {})
    loaded = []
    for raw in document.get("scenario", []):
        if only and raw["name"] not in only:
            continue
        mode, requests_per_second = _mode(raw.get("mode", defaults.get("mode", "closed")))
        router = _merged(defaults.get("router", {}), raw.get("router"))
        if "extra_args" in router:
            router["extra_args"] = tuple(router["extra_args"])
        scenario = Scenario(
            name=raw["name"],
            route=raw["route"],
            workload=raw["workload"],
            mode=mode,
            requests_per_second=requests_per_second,
            duration_seconds=float(raw.get("duration_seconds", defaults.get("duration_seconds", 3))),
            warmup_seconds=float(raw.get("warmup_seconds", defaults.get("warmup_seconds", 1))),
            repeat=int(raw.get("repeat", defaults.get("repeat", 1))),
            loadgen=LoadgenConfig(**_merged(defaults.get("loadgen", {}), raw.get("loadgen"))),
            memcached=MemcachedConfig(
                **_merged(defaults.get("memcached", {}), raw.get("memcached"))
            ),
            router=RouterConfig(**router),
            expect=Expectations(**_merged(defaults.get("expect", {}), raw.get("expect"))),
            faults=tuple(Fault(**fault) for fault in raw.get("faults", [])),
        )
        _validate(scenario)
        loaded.append(scenario)
    if not loaded:
        raise ValueError(f"no scenarios selected from {path}")
    return loaded


def _validate(scenario: Scenario) -> None:
    if scenario.duration_seconds <= 0 or scenario.warmup_seconds < 0:
        raise ValueError(f"{scenario.name}: invalid duration or warmup")
    if scenario.repeat <= 0:
        raise ValueError(f"{scenario.name}: repeat must be positive")
    if scenario.loadgen.threads <= 0 or scenario.loadgen.connections <= 0:
        raise ValueError(f"{scenario.name}: loadgen threads and connections must be positive")
    if scenario.loadgen.depth <= 0:
        raise ValueError(f"{scenario.name}: loadgen depth must be positive")
    if scenario.memcached.count < 0 or scenario.memcached.threads <= 0:
        raise ValueError(f"{scenario.name}: invalid memcached configuration")
    if scenario.route == "none" and scenario.memcached.count != 1:
        raise ValueError(f"{scenario.name}: direct mode requires exactly one memcached")
    if scenario.route == "null" and scenario.memcached.count != 0:
        raise ValueError(f"{scenario.name}: null route must not start memcached")
    if scenario.router.listening_sockets > scenario.router.num_proxies:
        raise ValueError(f"{scenario.name}: listening sockets exceed proxy threads")
    for fault in scenario.faults:
        if fault.action not in {"sigstop", "sigcont"}:
            raise ValueError(f"{scenario.name}: unsupported fault action {fault.action!r}")
        if fault.at_seconds < 0 or fault.at_seconds >= scenario.duration_seconds:
            raise ValueError(f"{scenario.name}: fault time is outside the measurement window")
        if fault.target < 0 or fault.target >= scenario.memcached.count:
            raise ValueError(f"{scenario.name}: fault target is outside the backend list")


def render_route(name: str, servers: list[str]) -> dict[str, Any]:
    path = BENCH_DIR / "routes" / ("null.json" if name == "null" else f"{name}.json.tmpl")
    document = json.loads(path.read_text())

    def replace(value: Any) -> Any:
        if value == "$SERVERS":
            return servers
        if isinstance(value, str) and value.startswith("$SERVER_"):
            index = int(value.removeprefix("$SERVER_"))
            try:
                return servers[index]
            except IndexError as error:
                raise ValueError(f"route {name} requires server {index}") from error
        if isinstance(value, list):
            return [replace(item) for item in value]
        if isinstance(value, dict):
            return {key: replace(item) for key, item in value.items()}
        return value

    return replace(document)
