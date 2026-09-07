from bench.processes import diff_metrics


def test_metric_diff_deltas_counters_and_keeps_gauges() -> None:
    before = {
        'requests_total{kind="mg"}': 10,
        "connections": 2,
    }
    after = {
        'requests_total{kind="mg"}': 14,
        "connections": 3,
    }
    assert diff_metrics(before, after) == {
        'requests_total{kind="mg"}': 4,
        "connections": 3,
    }
