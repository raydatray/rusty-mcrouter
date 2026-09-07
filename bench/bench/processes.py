from __future__ import annotations

import json
import os
import queue
import signal
import socket
import subprocess
import tempfile
import threading
import time
import urllib.request
from pathlib import Path
from typing import TextIO

import psutil


class ProcessError(RuntimeError):
    pass


class ManagedProcess:
    def __init__(self, command: list[str], *, stdin: bool = False, env: dict[str, str] | None = None):
        self.command = command
        self.process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE if stdin else subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            start_new_session=True,
            env=env,
        )
        self.stdout: queue.Queue[str] = queue.Queue()
        self.stderr: list[str] = []
        self._start_pump(self.process.stdout, self.stdout.put)
        self._start_pump(self.process.stderr, self._record_stderr)

    def _start_pump(self, stream: TextIO | None, consume) -> None:
        if stream is None:
            return

        def pump() -> None:
            for line in stream:
                consume(line.rstrip("\n"))

        threading.Thread(target=pump, daemon=True).start()

    def _record_stderr(self, line: str) -> None:
        self.stderr.append(line)
        if len(self.stderr) > 200:
            del self.stderr[0]

    def wait_line(self, predicate, timeout: float, description: str) -> str:
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ProcessError(f"timed out waiting for {description}: {self.diagnostics()}")
            try:
                line = self.stdout.get(timeout=min(remaining, 0.1))
            except queue.Empty:
                if self.process.poll() is not None:
                    raise ProcessError(
                        f"process exited with {self.process.returncode} while waiting for "
                        f"{description}: {self.diagnostics()}"
                    )
                continue
            if predicate(line):
                return line

    def send_line(self, line: str) -> None:
        if self.process.stdin is None:
            raise ProcessError("process has no controller input")
        self.process.stdin.write(line + "\n")
        self.process.stdin.flush()

    def wait(self, timeout: float) -> None:
        try:
            returncode = self.process.wait(timeout=timeout)
        except subprocess.TimeoutExpired as error:
            self.stop()
            raise ProcessError(f"process timed out: {self.diagnostics()}") from error
        if returncode != 0:
            raise ProcessError(f"process exited with {returncode}: {self.diagnostics()}")

    def diagnostics(self) -> str:
        command = " ".join(self.command)
        stderr = "\n".join(self.stderr[-20:])
        return f"command={command}\nstderr:\n{stderr}"

    def cpu_seconds(self) -> float:
        try:
            times = psutil.Process(self.process.pid).cpu_times()
        except psutil.Error:
            return 0.0
        return times.user + times.system

    def rss_mb(self) -> float:
        try:
            return psutil.Process(self.process.pid).memory_info().rss / 1_000_000
        except psutil.Error:
            return 0.0

    def stop(self) -> None:
        if self.process.poll() is not None:
            return
        try:
            os.killpg(self.process.pid, signal.SIGCONT)
        except ProcessLookupError:
            return
        for sig, wait in ((signal.SIGINT, 3), (signal.SIGTERM, 2), (signal.SIGKILL, 1)):
            try:
                os.killpg(self.process.pid, sig)
                self.process.wait(timeout=wait)
                return
            except ProcessLookupError:
                return
            except subprocess.TimeoutExpired:
                continue


def _free_port() -> int:
    with socket.socket() as candidate:
        candidate.bind(("127.0.0.1", 0))
        return candidate.getsockname()[1]


def _wait_port(process: ManagedProcess, port: int, timeout: float = 10) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.process.poll() is not None:
            raise ProcessError(f"process exited during startup: {process.diagnostics()}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise ProcessError(f"port {port} did not become ready: {process.diagnostics()}")


class Memcached:
    def __init__(self, *, threads: int, memory_mb: int, command_prefix: list[str] | None = None):
        self.threads = threads
        self.memory_mb = memory_mb
        self.command_prefix = command_prefix or []
        self.port = 0
        self.child: ManagedProcess | None = None

    @property
    def address(self) -> str:
        return f"127.0.0.1:{self.port}"

    def __enter__(self) -> Memcached:
        for attempt in range(5):
            self.port = _free_port()
            command = [
                *self.command_prefix,
                "memcached",
                "-l",
                "127.0.0.1",
                "-p",
                str(self.port),
                "-U",
                "0",
                "-t",
                str(self.threads),
                "-m",
                str(self.memory_mb),
                "-c",
                "4096",
                "-o",
                "modern",
            ]
            if os.geteuid() == 0:
                command.extend(["-u", "root"])
            self.child = ManagedProcess(command)
            try:
                _wait_port(self.child, self.port)
                return self
            except ProcessError:
                self.child.stop()
                if attempt == 4:
                    raise
        raise AssertionError("unreachable")

    def stats(self) -> dict[str, str]:
        selected = {
            "bytes",
            "cmd_get",
            "cmd_set",
            "curr_connections",
            "curr_items",
            "evictions",
            "get_hits",
            "get_misses",
            "limit_maxbytes",
            "total_connections",
        }
        with socket.create_connection(("127.0.0.1", self.port), timeout=2) as client:
            client.sendall(b"stats\r\n")
            response = bytearray()
            while not response.endswith(b"END\r\n"):
                chunk = client.recv(65536)
                if not chunk:
                    raise ProcessError("memcached closed while reading stats")
                response.extend(chunk)
        result = {}
        for line in response.decode().splitlines():
            parts = line.split()
            if len(parts) == 3 and parts[0] == "STAT" and parts[1] in selected:
                result[parts[1]] = parts[2]
        return result

    def send_signal(self, action: str) -> None:
        if not self.child or self.child.process.poll() is not None:
            raise ProcessError("cannot signal a stopped memcached process")
        selected = {"sigstop": signal.SIGSTOP, "sigcont": signal.SIGCONT}[action]
        os.killpg(self.child.process.pid, selected)

    def __exit__(self, *_args) -> None:
        if self.child:
            self.child.stop()


class Router:
    def __init__(
        self,
        *,
        binary: Path,
        config: dict,
        num_proxies: int,
        listening_sockets: int,
        extra_args: tuple[str, ...],
        command_prefix: list[str] | None = None,
    ):
        self.binary = binary
        self.config = config
        self.num_proxies = num_proxies
        self.listening_sockets = listening_sockets
        self.extra_args = extra_args
        self.command_prefix = command_prefix or []
        self.child: ManagedProcess | None = None
        self.listen_address = ""
        self.metrics_address = ""
        self.config_path: Path | None = None

    def __enter__(self) -> Router:
        descriptor, path = tempfile.mkstemp(prefix="rmc-bench-", suffix=".json")
        with os.fdopen(descriptor, "w") as config_file:
            json.dump(self.config, config_file)
        self.config_path = Path(path)
        command = [
            *self.command_prefix,
            str(self.binary),
            "--config",
            path,
            "--listen",
            "127.0.0.1:0",
            "--metrics-addr",
            "127.0.0.1:0",
            "--num-proxies",
            str(self.num_proxies),
            "--num-listening-sockets",
            str(self.listening_sockets),
            *self.extra_args,
        ]
        try:
            self.child = ManagedProcess(
                command, env={**os.environ, "RUST_LOG": os.environ.get("RUST_LOG", "warn")}
            )
            deadline = time.monotonic() + 15
            while not (self.listen_address and self.metrics_address):
                line = self.child.wait_line(
                    lambda candidate: candidate.startswith(("READY ", "METRICS ")),
                    max(0.01, deadline - time.monotonic()),
                    "router readiness",
                )
                if line.startswith("READY "):
                    self.listen_address = line.removeprefix("READY ").strip()
                else:
                    self.metrics_address = line.removeprefix("METRICS ").strip()
            return self
        except Exception:
            self.__exit__()
            raise

    def metrics(self) -> dict[str, float]:
        with urllib.request.urlopen(f"http://{self.metrics_address}/metrics", timeout=3) as response:
            text = response.read().decode()
        metrics = {}
        for line in text.splitlines():
            if not line or line.startswith("#"):
                continue
            name, separator, raw_value = line.rpartition(" ")
            if separator:
                metrics[name] = float(raw_value)
        return metrics

    def cpu_seconds(self) -> float:
        assert self.child
        return self.child.cpu_seconds()

    def rss_mb(self) -> float:
        assert self.child
        return self.child.rss_mb()

    @property
    def stderr_tail(self) -> list[str]:
        assert self.child
        return self.child.stderr[-20:]

    def __exit__(self, *_args) -> None:
        if self.child:
            self.child.stop()
        if self.config_path:
            self.config_path.unlink(missing_ok=True)


def diff_metrics(before: dict[str, float], after: dict[str, float]) -> dict[str, float]:
    result = {}
    for name, value in after.items():
        base_name = name.split("{", 1)[0]
        result[name] = value - before.get(name, 0.0) if base_name.endswith("_total") else value
    return result
