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
        "| scenario | label | rps | p50 | p99 | errors | dropped | valid |",
        "|---|---|---:|---:|---:|---:|---:|---|",
    ]
    for record in records:
        report = record["loadgen"]["report"]
        counts = report["counts"]
        rows.append(
            f"| {record['scenario']} | {record['label']} | "
            f"{_rate(report['achieved_requests_per_second'])} | "
            f"{_latency(report['latency_us']['p50'])} | "
            f"{_latency(report['latency_us']['p99'])} | "
            f"{counts['errors']} | {counts['dropped']} | "
            f"{'yes' if record['validity']['valid'] else 'no'} |"
        )
    return "\n".join(rows)
