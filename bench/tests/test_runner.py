from bench.runner import comparison_order


def test_comparison_order_alternates() -> None:
    assert comparison_order(0) == ("base", "head")
    assert comparison_order(1) == ("head", "base")
