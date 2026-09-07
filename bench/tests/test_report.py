from bench.report import summary


def test_summary_renders_client_metrics() -> None:
    record = {
        "scenario": "pool",
        "label": "head",
        "validity": {"valid": True},
        "loadgen": {
            "report": {
                "achieved_requests_per_second": 12345,
                "latency_us": {"p50": 10, "p99": 2000},
                "counts": {"errors": 0, "dropped": 0},
            }
        },
    }
    rendered = summary([record])
    assert "12k" in rendered
    assert "2.00ms" in rendered
