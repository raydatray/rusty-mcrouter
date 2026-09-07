from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

from . import report, runner, scenarios

REPO = runner.REPO
BENCH_DIR = runner.BENCH_DIR


def _targets() -> tuple[Path, Path]:
    configured = os.environ.get("CARGO_TARGET_DIR")
    if configured:
        target = Path(configured)
        return target / "release/rusty-mcrouter", target / "release/rusty-mcrouter-loadgen"
    return REPO / "target/release/rusty-mcrouter", BENCH_DIR / "target/release/rusty-mcrouter-loadgen"


def _build() -> None:
    subprocess.check_call(
        ["cargo", "build", "--release", "--locked", "-p", "rusty-mcrouter"], cwd=REPO
    )
    subprocess.check_call(
        [
            "cargo",
            "build",
            "--release",
            "--locked",
            "--manifest-path",
            str(BENCH_DIR / "Cargo.toml"),
            "-p",
            "rusty-mcrouter-loadgen",
        ],
        cwd=REPO,
    )


def main(argv: list[str] | None = None) -> int:
    default_router, default_loadgen = _targets()
    parser = argparse.ArgumentParser(prog="rmc-bench")
    commands = parser.add_subparsers(dest="command", required=True)

    run = commands.add_parser("run", help="run scenarios against one router binary")
    run.add_argument("scenarios", type=Path)
    run.add_argument("--router", type=Path, default=default_router)
    run.add_argument("--loadgen", type=Path, default=default_loadgen)
    run.add_argument("--output", type=Path, default=BENCH_DIR / "results/latest.jsonl")
    run.add_argument("--label", default="head")
    run.add_argument("--only", nargs="*")
    run.add_argument("--repeat", type=int)
    run.add_argument("--build", action="store_true")
    run.add_argument("--fresh", action="store_true")

    render = commands.add_parser("report", help="render an existing JSONL result")
    render.add_argument("results", type=Path)

    args = parser.parse_args(argv)
    if args.command == "report":
        print(report.summary(report.load(args.results)))
        return 0

    if args.build or not (args.router.exists() and args.loadgen.exists()):
        _build()
    selected = scenarios.load(args.scenarios, set(args.only) if args.only else None)
    if args.fresh:
        args.output.unlink(missing_ok=True)
    try:
        records = runner.run_matrix(
            selected,
            args.router,
            args.loadgen,
            args.output,
            args.label,
            args.repeat,
        )
    except runner.InvalidRun as error:
        print(report.summary(report.load(args.output)))
        print(f"invalid benchmark: {error}", file=sys.stderr)
        return 1
    print(report.summary(records))
    return 0


if __name__ == "__main__":
    sys.exit(main())
