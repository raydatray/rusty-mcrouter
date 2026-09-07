from bench.report import comparison, summary


def test_summary_renders_client_metrics() -> None:
    record = {
        "scenario": "pool",
        "label": "head",
        "validity": {"valid": True},
        "subject": {"cpu_cores_average": 1.5},
        "faults": {"applied": []},
        "loadgen": {
            "report": {
                "mode": "open",
                "achieved_requests_per_second": 12345,
                "latency_us": {"p50": 10, "p99": 2000},
                "counts": {"errors": 0, "dropped": 0},
            }
        },
    }
    rendered = summary([record])
    assert "12k" in rendered
    assert "2.00ms" in rendered
    assert "1.5" in rendered


def _comparison_record(label: str, repetition: int, rps: float, p99: int) -> dict:
    return {
        "scenario": "pool",
        "label": label,
        "pair_id": f"pool:{repetition}",
        "validity": {"valid": True},
        "loadgen": {
            "report": {
                "mode": "closed",
                "achieved_requests_per_second": rps,
                "latency_us": {"p99": p99},
            }
        },
    }


def test_comparison_uses_paired_ratios_and_requires_three_pairs() -> None:
    records = []
    for repetition in range(2):
        records.extend(
            [
                _comparison_record("base", repetition, 100, 10),
                _comparison_record("head", repetition, 80, 12),
            ]
        )
    markdown, failed = comparison(records)
    assert "0.800" in markdown
    assert "INCONCLUSIVE" in markdown
    assert not failed
