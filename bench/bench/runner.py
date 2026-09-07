from __future__ import annotations

import hashlib
import json
import platform
import subprocess
import tempfile
import time
from contextlib import ExitStack
from pathlib import Path
from typing import Any

import psutil

from .processes import ManagedProcess, Memcached, Router, diff_metrics
from .scenarios import Scenario, render_route

BENCH_DIR = Path(__file__).resolve().parent.parent
REPO = BENCH_DIR.parent


class InvalidRun(RuntimeError):
    pass


def _revision() -> str:
    for command in (["sl", "whereami"], ["git", "rev-parse", "HEAD"]):
        try:
            return subprocess.check_output(
                command, cwd=REPO, text=True, stderr=subprocess.DEVNULL
            ).strip()
        except (OSError, subprocess.CalledProcessError):
            continue
    return "unknown"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _host() -> dict[str, Any]:
    return {
        "platform": platform.platform(),
        "architecture": platform.machine(),
        "logical_cpus": psutil.cpu_count(logical=True),
        "memory_bytes": psutil.virtual_memory().total,
        "python": platform.python_version(),
    }


def _event(child: ManagedProcess, expected: str, timeout: float) -> None:
    def matches(line: str) -> bool:
        try:
            return json.loads(line).get("event") == expected
        except (json.JSONDecodeError, AttributeError):
            return False

    child.wait_line(matches, timeout, f"loadgen {expected} event")


def _loadgen_command(
    binary: Path, target: str, scenario: Scenario, output: Path, command_prefix: list[str]
) -> list[str]:
    command = [
        *command_prefix,
        str(binary),
        "--target",
        target,
        "--workload",
        str(scenario.workload_path()),
        "--connections",
        str(scenario.loadgen.connections),
        "--depth",
        str(scenario.loadgen.depth),
        "--threads",
        str(scenario.loadgen.threads),
        "--mode",
        scenario.mode,
        "--duration",
        str(scenario.duration_seconds),
        "--warmup",
        str(scenario.warmup_seconds),
        "--controlled",
        "--out",
        str(output),
    ]
    if scenario.requests_per_second is not None:
        command.extend(["--requests-per-second", str(scenario.requests_per_second)])
    if scenario.route == "null":
        command.append("--no-prewarm")
    return command


def _validity(scenario: Scenario, report: dict[str, Any]) -> dict[str, Any]:
    reasons = []
    counts = report["counts"]
    if counts["completed_in_window"] < scenario.expect.min_completed:
        reasons.append(
            f"completed {counts['completed_in_window']} < {scenario.expect.min_completed}"
        )
    if counts["errors"] > scenario.expect.max_protocol_errors:
        reasons.append(f"protocol errors {counts['errors']} > {scenario.expect.max_protocol_errors}")
    if counts["dropped"] > scenario.expect.max_dropped:
        reasons.append(f"dropped {counts['dropped']} > {scenario.expect.max_dropped}")
    lag_limit = scenario.expect.max_schedule_lag_p99_us
    lag = report.get("schedule_lag_us")
    if lag_limit is not None and lag and lag["p99"] > lag_limit:
        reasons.append(f"schedule lag p99 {lag['p99']}us > {lag_limit}us")
    return {"valid": not reasons, "reasons": reasons}


def run_scenario(
    scenario: Scenario,
    router_binary: Path,
    loadgen_binary: Path,
    repetition: int,
    label: str,
    *,
    command_prefixes: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    prefixes = command_prefixes or {}
    with tempfile.TemporaryDirectory(prefix="rmc-bench-") as temporary, ExitStack() as stack:
        backends = [
            stack.enter_context(
                Memcached(
                    threads=scenario.memcached.threads,
                    memory_mb=scenario.memcached.memory_mb,
                    command_prefix=prefixes.get("memcached"),
                )
            )
            for _ in range(scenario.memcached.count)
        ]
        router = None
        if scenario.route == "none":
            target = backends[0].address
        else:
            router = stack.enter_context(
                Router(
                    binary=router_binary,
                    config=render_route(scenario.route, [backend.address for backend in backends]),
                    num_proxies=scenario.router.num_proxies,
                    listening_sockets=scenario.router.listening_sockets,
                    extra_args=scenario.router.extra_args,
                    command_prefix=prefixes.get("router"),
                )
            )
            target = router.listen_address

        output = Path(temporary) / "loadgen.json"
        loadgen = ManagedProcess(
            _loadgen_command(
                loadgen_binary,
                target,
                scenario,
                output,
                prefixes.get("loadgen", []),
            ),
            stdin=True,
        )
        stack.callback(loadgen.stop)
        _event(loadgen, "ready", 120)
        metrics_before = router.metrics() if router else {}
        cpu_before = router.cpu_seconds() if router else 0.0
        loadgen.send_line("GO")
        _event(loadgen, "measure_start", 5)
        wall_start = time.monotonic()
        _event(loadgen, "measure_end", scenario.duration_seconds + 5)
        wall_seconds = time.monotonic() - wall_start
        metrics_after = router.metrics() if router else {}
        cpu_after = router.cpu_seconds() if router else 0.0
        loadgen.wait(15)
        report = json.loads(output.read_text())
        validity = _validity(scenario, report)
        router_result = None
        if router:
            cpu_seconds = max(0.0, cpu_after - cpu_before)
            router_result = {
                "binary_sha256": _sha256(router_binary),
                "cpu_seconds": cpu_seconds,
                "cpu_cores_average": cpu_seconds / wall_seconds if wall_seconds else 0.0,
                "rss_mb": router.rss_mb(),
                "metrics": diff_metrics(metrics_before, metrics_after),
                "stderr_tail": router.stderr_tail,
            }
        return {
            "schema_version": 1,
            "scenario": scenario.name,
            "label": label,
            "repetition": repetition,
            "source_revision": _revision(),
            "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "host": _host(),
            "parameters": scenario.resolved(),
            "subject": router_result,
            "loadgen": {"binary_sha256": _sha256(loadgen_binary), "report": report},
            "memcached": [backend.stats() for backend in backends],
            "validity": validity,
        }


def run_matrix(
    scenarios: list[Scenario],
    router_binary: Path,
    loadgen_binary: Path,
    output: Path,
    label: str,
    repeat: int | None = None,
) -> list[dict[str, Any]]:
    records = []
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("a") as destination:
        for scenario in scenarios:
            for repetition in range(repeat or scenario.repeat):
                record = run_scenario(
                    scenario,
                    router_binary,
                    loadgen_binary,
                    repetition,
                    label,
                )
                records.append(record)
                destination.write(json.dumps(record, sort_keys=True) + "\n")
                destination.flush()
    invalid = [record for record in records if not record["validity"]["valid"]]
    if invalid:
        detail = "; ".join(
            f"{record['scenario']}: {', '.join(record['validity']['reasons'])}"
            for record in invalid
        )
        raise InvalidRun(detail)
    return records
