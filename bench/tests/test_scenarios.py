from pathlib import Path

import pytest

from bench.scenarios import load, render_route


def test_loads_smoke_scenarios() -> None:
    scenarios = load(Path(__file__).parents[1] / "scenarios/smoke.toml")
    assert [scenario.name for scenario in scenarios] == [
        "null-ceiling",
        "direct-memcached",
        "pool1-open250k",
    ]
    assert scenarios[-1].requests_per_second == 250_000


def test_renders_server_sentinel_as_array() -> None:
    route = render_route("pool1", ["127.0.0.1:11211"])
    assert route["pools"]["A"]["servers"] == ["127.0.0.1:11211"]


def test_rejects_empty_selection() -> None:
    with pytest.raises(ValueError, match="no scenarios selected"):
        load(Path(__file__).parents[1] / "scenarios/smoke.toml", {"missing"})
