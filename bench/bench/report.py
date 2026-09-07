from __future__ import annotations

import json
from pathlib import Path


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
