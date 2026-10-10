"""私有真实 Serve 合成宿主控制；不进入任何 Python 发行包。"""

import hashlib
import json
import os
from pathlib import Path
import queue
import subprocess
import threading
import time
import uuid


class Fixture:
    def __init__(self, node, fixture, cli_root, work_dir, mode="session", data_directory=None):
        self.node, self.fixture, self.cli_root = str(node), Path(fixture), str(cli_root)
        self.work_dir, self.mode = Path(work_dir), mode
        self.data_directory = Path(data_directory) if data_directory is not None else self.work_dir / "data"
        self.info = None
        self.process = None
        self.lines = queue.Queue(maxsize=128)
        self.overflow = threading.Event()
        self.reader = None
        self.log = None
        self._command_lock = threading.Lock()
        self._generation = 0

    def __enter__(self):
        if not self.fixture.is_file():
            raise RuntimeError("explicit real Serve fixture missing")
        if self.process is not None:
            raise RuntimeError("fixture is already running")
        self.work_dir.mkdir(parents=True, exist_ok=self._generation > 0)
        data = self.data_directory
        data.mkdir(exist_ok=self._generation > 0)
        self._generation += 1
        log_name = "host.log" if self._generation == 1 else "host-%d.log" % self._generation
        self.log = (self.work_dir / log_name).open("w", encoding="utf-8", newline="\n")
        env = {
            key: value
            for key, value in os.environ.items()
            if not any(item in key.upper() for item in ("TOKEN", "SECRET", "PASSWORD", "API_KEY", "GROWTH"))
        }
        command = [self.node, str(self.fixture), self.cli_root, str(data), self.mode]
        self.process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=self.log,
            cwd=str(self.work_dir),
            text=True,
            encoding="utf-8",
            errors="strict",
            env=env,
            bufsize=1,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        process = self.process

        def drain():
            try:
                for line in process.stdout:
                    self.log.write(line)
                    self.log.flush()
                    try:
                        self.lines.put_nowait(line.rstrip("\n"))
                    except queue.Full:
                        self.overflow.set()
            finally:
                process.stdout.close()

        self.reader = threading.Thread(target=drain, name="tansr-fixture-log")
        self.reader.start()
        try:
            self.info = self._read("TANSR_GO_FIXTURE ", 20)
            receipt = "fixture.json" if self._generation == 1 else "fixture-%d.json" % self._generation
            (self.work_dir / receipt).write_text(
                json.dumps(
                    {
                        "mode": self.mode,
                        "fixtureSha256": hashlib.sha256(self.fixture.read_bytes()).hexdigest(),
                        "manifestRevision": self.info["manifestRevision"],
                        "realCore": True,
                        "synthetic": ["authentication", "platform", "model"],
                    },
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
            return self
        except BaseException:
            self.close()
            raise

    def restart(self):
        self.close()
        self.lines = queue.Queue(maxsize=128)
        self.overflow.clear()
        self.__enter__()
        return self.info

    def _read(self, prefix, timeout):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            if self.overflow.is_set():
                raise RuntimeError("fixture control queue overflow")
            try:
                line = self.lines.get(timeout=min(0.1, max(0.001, end - time.monotonic())))
            except queue.Empty:
                if self.process.poll() is not None:
                    raise RuntimeError("fixture exited before requested control response")
                continue
            if line.startswith(prefix):
                return json.loads(line[len(prefix) :])
        raise RuntimeError("fixture control deadline exceeded")

    def command(self, command, **kwargs):
        with self._command_lock:
            body = dict(command) if isinstance(command, dict) else dict(kwargs, command=command)
            body.setdefault("requestId", "py-" + uuid.uuid4().hex)
            self.process.stdin.write(json.dumps(body, ensure_ascii=True) + "\n")
            self.process.stdin.flush()
            result = self._read("TANSR_RUST_CONTROL ", 15)
            if result.get("requestId") != body["requestId"] or result.get("ok") is not True:
                raise RuntimeError("fixture control rejected or mismatched")
            return result.get("result")

    def close(self):
        if self.process is None:
            return
        process, self.process = self.process, None
        try:
            if process.poll() is None:
                try:
                    process.stdin.write("stop\n")
                    process.stdin.flush()
                except (OSError, ValueError):
                    pass
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
                raise RuntimeError("real Serve fixture failed graceful shutdown")
        finally:
            process.stdin.close()
            if self.reader is not None:
                self.reader.join(5)
            if self.log is not None:
                self.log.close()
        if self.reader is not None and self.reader.is_alive():
            raise RuntimeError("fixture output reader leaked")
        if process.returncode != 0:
            raise RuntimeError("fixture exit code " + str(process.returncode))

    def __exit__(self, *unused):
        self.close()
