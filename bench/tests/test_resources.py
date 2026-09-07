from bench.resources import _parse_cpu_list, load


def test_loads_resource_profiles() -> None:
    profile = load("smoke-4cpu")
    assert profile.router_cpus == "2"
    assert profile.prefixes()["loadgen"] == ["taskset", "--cpu-list", "0-1"]


def test_parses_cpu_ranges() -> None:
    assert _parse_cpu_list("0-2,5") == {0, 1, 2, 5}
