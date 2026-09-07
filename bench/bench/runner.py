from __future__ import annotations

import hashlib
import json
import platform
import subprocess
import tempfile
import threading
import time
from contextlib import ExitStack
from pathlib import Path
from typing import Any

import psutil

from . import resources
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


class FaultController:
    def __init__(self, scenario: Scenario, backends: list[Memcached]):
        self.scenario = scenario
        self.backends = backends
        self.timers: list[threading.Timer] = []
        self.applied: list[dict[str, Any]] = []
        self.errors: list[str] = []
        self._lock = threading.Lock()

    def arm(self, start: float) -> None:
        for fault in self.scenario.faults:
            timer = threading.Timer(fault.at_seconds, self._apply, args=(fault, start))
            timer.daemon = True
            timer.start()
            self.timers.append(timer)

    def _apply(self, fault, start: float) -> None:
        try:
            self.backends[fault.target].send_signal(fault.action)
            with self._lock:
                self.applied.append(
                    {
                        "configured_at_seconds": fault.at_seconds,
                        "applied_at_seconds": time.monotonic() - start,
                        "action": fault.action,
                        "target": fault.target,
                    }
                )
        except Exception as error:
            with self._lock:
                self.errors.append(str(error))

    def cancel(self) -> None:
        for timer in self.timers:
            timer.cancel()


def _metric_sum(metrics: dict[str, float], family: str) -> float:
    return sum(value for name, value in metrics.items() if name.split("{", 1)[0] == family)


def _validity(
    scenario: Scenario,
    report: dict[str, Any],
    router_metrics: dict[str, float],
    faults: FaultController,
) -> dict[str, Any]:
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
    failovers = _metric_sum(router_metrics, "rusty_mcrouter_failover_total")
    if failovers < scenario.expect.min_failovers:
        reasons.append(f"failovers {failovers:g} < {scenario.expect.min_failovers}")
    tko = _metric_sum(router_metrics, "rusty_mcrouter_tko")
    if scenario.expect.max_tko_final is not None and tko > scenario.expect.max_tko_final:
        reasons.append(f"final TKO count {tko:g} > {scenario.expect.max_tko_final}")
    if len(faults.applied) != len(scenario.faults):
        reasons.append(f"applied {len(faults.applied)} of {len(scenario.faults)} configured faults")
    reasons.extend(f"fault injection failed: {error}" for error in faults.errors)
    return {"valid": not reasons, "reasons": reasons}


def run_scenario(
    scenario: Scenario,
    router_binary: Path,
    loadgen_binary: Path,
    repetition: int,
    label: str,
    *,
    command_prefixes: dict[str, list[str]] | None = None,
    resource_info: dict[str, Any] | None = None,
    pair_id: str | None = None,
    order: int | None = None,
    subject_revision: str | None = None,
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
        faults = FaultController(scenario, backends)
        stack.callback(faults.cancel)
        _event(loadgen, "ready", 120)
        metrics_before = router.metrics() if router else {}
        cpu_before = router.cpu_seconds() if router else 0.0
        loadgen.send_line("GO")
        _event(loadgen, "measure_start", 5)
        wall_start = time.monotonic()
        faults.arm(wall_start)
        _event(loadgen, "measure_end", scenario.duration_seconds + 5)
        faults.cancel()
        wall_seconds = time.monotonic() - wall_start
        metrics_after = router.metrics() if router else {}
        cpu_after = router.cpu_seconds() if router else 0.0
        loadgen.wait(15)
        report = json.loads(output.read_text())
        metric_delta = diff_metrics(metrics_before, metrics_after)
        validity = _validity(scenario, report, metric_delta, faults)
        router_result = None
        if router:
            cpu_seconds = max(0.0, cpu_after - cpu_before)
            router_result = {
                "binary_sha256": _sha256(router_binary),
                "source_revision": subject_revision,
                "cpu_seconds": cpu_seconds,
                "cpu_cores_average": cpu_seconds / wall_seconds if wall_seconds else 0.0,
                "rss_mb": router.rss_mb(),
                "metrics": metric_delta,
                "stderr_tail": router.stderr_tail,
            }
        return {
            "schema_version": 1,
            "scenario": scenario.name,
            "label": label,
            "repetition": repetition,
            "pair_id": pair_id,
            "order": order,
            "source_revision": _revision(),
            "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "host": _host(),
            "resources": {
                **(resource_info or {}),
                "observed_after": resources.observed(),
            },
            "parameters": scenario.resolved(),
            "scenario_sha256": hashlib.sha256(
                json.dumps(scenario.resolved(), sort_keys=True).encode()
            ).hexdigest(),
            "workload_sha256": _sha256(scenario.workload_path()),
            "subject": router_result,
            "loadgen": {"binary_sha256": _sha256(loadgen_binary), "report": report},
            "memcached": [backend.stats() for backend in backends],
            "faults": {"configured": len(scenario.faults), "applied": faults.applied},
            "validity": validity,
        }


def run_matrix(
    scenarios: list[Scenario],
    router_binary: Path,
    loadgen_binary: Path,
    output: Path,
    label: str,
    repeat: int | None = None,
    *,
    command_prefixes: dict[str, list[str]] | None = None,
    resource_info: dict[str, Any] | None = None,
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
                    command_prefixes=command_prefixes,
                    resource_info=resource_info,
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


def comparison_order(repetition: int) -> tuple[str, str]:
    return ("base", "head") if repetition % 2 == 0 else ("head", "base")


def run_comparison(
    scenarios: list[Scenario],
    base_binary: Path,
    head_binary: Path,
    loadgen_binary: Path,
    output: Path,
    repeat: int | None = None,
    *,
    command_prefixes: dict[str, list[str]] | None = None,
    resource_info: dict[str, Any] | None = None,
    base_revision: str | None = None,
    head_revision: str | None = None,
) -> list[dict[str, Any]]:
    subjects = {
        "base": (base_binary, base_revision),
        "head": (head_binary, head_revision),
    }
    records = []
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w") as destination:
        for scenario in scenarios:
            for repetition in range(repeat or scenario.repeat):
                pair_id = f"{scenario.name}:{repetition}"
                for order, label in enumerate(comparison_order(repetition)):
                    binary, revision = subjects[label]
                    record = run_scenario(
                        scenario,
                        binary,
                        loadgen_binary,
                        repetition,
                        label,
                        command_prefixes=command_prefixes,
                        resource_info=resource_info,
                        pair_id=pair_id,
                        order=order,
                        subject_revision=revision,
                    )
                    records.append(record)
                    destination.write(json.dumps(record, sort_keys=True) + "\n")
                    destination.flush()
    return records
