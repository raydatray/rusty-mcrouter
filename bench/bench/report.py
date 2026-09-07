from __future__ import annotations

import json
import random
from collections import defaultdict
from pathlib import Path
from statistics import median


def load(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def _rate(value: float) -> str:
    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    if value >= 1_000:
        return f"{value / 1_000:.0f}k"
    return f"{value:.0f}"


def _latency(value: float) -> str:
    return f"{value / 1000:.2f}ms" if value >= 1000 else f"{value:.0f}us"


def summary(records: list[dict]) -> str:
    rows = [
        "| scenario | label | mode | rps | p50 | p99 | errors | dropped | router cores | valid | notes |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---|---|",
    ]
    for record in records:
        report = record["loadgen"]["report"]
        counts = report["counts"]
        subject = record.get("subject") or {}
        notes = list(record["validity"].get("reasons", []))
        faults = record.get("faults", {}).get("applied", [])
        if faults:
            notes.append("faults=" + ",".join(fault["action"] for fault in faults))
        rows.append(
            f"| {record['scenario']} | {record['label']} | {report['mode']} | "
            f"{_rate(report['achieved_requests_per_second'])} | "
            f"{_latency(report['latency_us']['p50'])} | "
            f"{_latency(report['latency_us']['p99'])} | "
            f"{counts['errors']} | {counts['dropped']} | "
            f"{subject.get('cpu_cores_average', '-')} | "
            f"{'yes' if record['validity']['valid'] else 'no'} | {'; '.join(notes)} |"
        )
    return "\n".join(rows)


def _paired(records: list[dict], field) -> list[float]:
    pairs = defaultdict(dict)
    for record in records:
        if record["validity"]["valid"]:
            pairs[record["pair_id"]][record["label"]] = field(record)
    return [values["head"] / values["base"] for values in pairs.values() if set(values) == {"base", "head"} and values["base"] > 0]


def _interval(values: list[float], samples: int = 2000) -> tuple[float, float, float]:
    center = median(values)
    if len(values) == 1:
        return center, center, center
    generator = random.Random(42)
    bootstrapped = sorted(
        median(generator.choice(values) for _ in values) for _ in range(samples)
    )
    return center, bootstrapped[int(samples * 0.025)], bootstrapped[int(samples * 0.975)]


def comparison(records: list[dict]) -> tuple[str, bool]:
    grouped = defaultdict(list)
    for record in records:
        grouped[record["scenario"]].append(record)

    control_noisy = False
    controls = grouped.get("direct-control", [])
    if controls:
        ratios = _paired(controls, lambda record: record["loadgen"]["report"]["achieved_requests_per_second"])
        control_noisy = not ratios or not 0.9 <= median(ratios) <= 1.1

    rows = [
        "| scenario | valid pairs | invalid runs | rps ratio | 95% interval | p99 ratio | 95% interval | verdict | notes |",
        "|---|---:|---:|---:|---:|---:|---:|---|---|",
    ]
    any_regression = False
    for name, scenario_records in grouped.items():
        rps = _paired(
            scenario_records,
            lambda record: record["loadgen"]["report"]["achieved_requests_per_second"],
        )
        p99 = _paired(
            scenario_records,
            lambda record: record["loadgen"]["report"]["latency_us"]["p99"],
        )
        valid_pairs = min(len(rps), len(p99))
        invalid = [record for record in scenario_records if not record["validity"]["valid"]]
        notes = "; ".join(
            f"{record['label']} r{record['repetition']}: {', '.join(record['validity']['reasons'])}"
            for record in invalid
        )
        if not rps or not p99:
            rows.append(f"| {name} | 0 | {len(invalid)} | - | - | - | - | INVALID | {notes} |")
            continue
        rps_mid, rps_low, rps_high = _interval(rps)
        p99_mid, p99_low, p99_high = _interval(p99)
        mode = scenario_records[0]["loadgen"]["report"]["mode"]
        verdict = "INCONCLUSIVE"
        if name == "direct-control":
            verdict = "CONTROL-NOISY" if control_noisy else "CONTROL-STABLE"
        elif valid_pairs >= 3 and not control_noisy:
            regression = rps_high < 0.90 if mode == "closed" else p99_low > 1.15
            improvement = rps_low > 1.10 if mode == "closed" else p99_high < 0.87
            if regression:
                verdict = "REGRESS"
                any_regression = True
            elif improvement:
                verdict = "IMPROVE"
            elif rps_low <= 1 <= rps_high and p99_low <= 1 <= p99_high:
                verdict = "NEUTRAL"
        rows.append(
            f"| {name} | {valid_pairs} | {len(invalid)} | {rps_mid:.3f} | "
            f"{rps_low:.3f}..{rps_high:.3f} | {p99_mid:.3f} | "
            f"{p99_low:.3f}..{p99_high:.3f} | {verdict} | {notes} |"
        )
    note = "\n\nEnvironmental control was noisy; subject verdicts are inconclusive." if control_noisy else ""
    return "\n".join(rows) + note, any_regression
