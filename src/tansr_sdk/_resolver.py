"""有界专有 DNS 子进程；绝不把取消误作 getaddrinfo 线程已经结束。"""

import ipaddress
import json
import os
import socket
import subprocess
import sys
import threading
import time
from typing import List, Optional, Tuple

from .errors import Error
from .lifecycle import CancellationToken, now_ms

_MAX_REPLY = 16384
_MAX_ADDRESSES = 64
_WORKER = r"""
import json, socket, sys
try:
    host, port = sys.argv[1], int(sys.argv[2])
    rows = socket.getaddrinfo(host, port, socket.AF_UNSPEC, socket.SOCK_STREAM)
    unique = []
    for family, kind, protocol, name, address in rows:
        if family not in (socket.AF_INET, socket.AF_INET6): continue
        value = [family, list(address)]
        if value not in unique: unique.append(value)
        if len(unique) >= 64: break
    raw = json.dumps(unique, separators=(',', ':')).encode('ascii')
    if not unique or len(raw) > 16384: sys.exit(2)
    sys.stdout.buffer.write(raw)
except Exception:
    sys.exit(2)
"""


def _environment() -> dict:
    # Windows 解释器/系统解析所需变量；不继承 token、代理、PYTHONPATH 等。
    allowed = {"SYSTEMROOT", "WINDIR", "SYSTEMDRIVE", "COMSPEC"}
    return {key: value for key, value in os.environ.items() if key.upper() in allowed}


def _numeric(host: str, port: int) -> Optional[List[Tuple[int, tuple]]]:
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return None
    if address.version == 4:
        return [(socket.AF_INET, (str(address), port))]
    return [(socket.AF_INET6, (str(address), port, 0, 0))]


class Resolver:
    def __init__(self, max_processes: int = 2) -> None:
        if isinstance(max_processes, bool) or not isinstance(max_processes, int) or max_processes < 1:
            raise Error("invalid_argument")
        self._slots = threading.BoundedSemaphore(max_processes)
        self._lock = threading.Lock()
        self._processes = set()  # type: set
        self._closed = False

    def resolve(self, host: str, port: int, deadline_ms: int, cancel: CancellationToken) -> List[Tuple[int, tuple]]:
        if not isinstance(host, str) or not host or len(host) > 253:
            raise Error("invalid_argument")
        if any(ord(char) < 33 or ord(char) > 126 for char in host):
            raise Error("invalid_argument")
        if isinstance(port, bool) or not isinstance(port, int) or not 0 < port < 65536:
            raise Error("invalid_argument")
        end = time.monotonic() + max(0, deadline_ms - now_ms()) / 1000.0

        def check() -> None:
            cancel.check(deadline_ms)
            if time.monotonic() >= end:
                raise Error("timeout")
            with self._lock:
                if self._closed:
                    raise Error("closed")

        check()
        numeric = _numeric(host, port)
        if numeric is not None:
            return numeric
        while not self._slots.acquire(timeout=min(0.025, max(0.001, end - time.monotonic()))):
            check()
        process = None
        reader = None
        chunks = []  # type: list
        reader_error = []  # type: list
        overflow = threading.Event()
        try:
            check()
            executable = os.path.abspath(sys.executable) if sys.executable else ""
            if not executable or not os.path.isfile(executable) or getattr(sys, "frozen", False):
                raise Error("unsupported", "Host must provide a cancellable resolver")
            # -I -S 不装载宿主 site/customize；参数不经 shell 或 pickle。
            with self._lock:
                if self._closed:
                    raise Error("closed")
                process = subprocess.Popen(
                    [executable, "-I", "-S", "-c", _WORKER, host, str(port)],
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.DEVNULL,
                    env=_environment(),
                    shell=False,
                    close_fds=True,
                    creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                )
                self._processes.add(process)
            stdout = process.stdout
            if stdout is None:
                raise Error("network", "DNS reply pipe is not available")

            def read_reply() -> None:
                try:
                    data = stdout.read(_MAX_REPLY + 1)
                    chunks.append(data)
                    if len(data) > _MAX_REPLY:
                        overflow.set()
                        process.kill()
                except (OSError, ValueError) as exc:
                    reader_error.append(type(exc).__name__)

            reader = threading.Thread(target=read_reply, name="tansr-dns-reply")
            try:
                reader.start()
            except RuntimeError:
                raise Error("resource_limit", "Cannot start DNS reply reader") from None
            while process.poll() is None:
                check()
                if overflow.is_set():
                    raise Error("resource_limit")
                cancel.wait(min(0.02, max(0.001, end - time.monotonic())))
            reader.join()
            check()
            if overflow.is_set():
                raise Error("resource_limit")
            if process.returncode != 0 or reader_error or len(chunks) != 1:
                raise Error("network", "DNS resolution failed")
            try:
                values = json.loads(chunks[0].decode("ascii"))
                if not isinstance(values, list) or not 1 <= len(values) <= _MAX_ADDRESSES:
                    raise ValueError()
                result = []
                for value in values:
                    if not isinstance(value, list) or len(value) != 2:
                        raise ValueError()
                    family, address = value
                    if type(family) is not int or family not in (socket.AF_INET, socket.AF_INET6):
                        raise ValueError()
                    if not isinstance(address, list) or len(address) != (2 if family == socket.AF_INET else 4):
                        raise ValueError()
                    if not isinstance(address[0], str) or type(address[1]) is not int or address[1] != port:
                        raise ValueError()
                    parsed = ipaddress.ip_address(address[0])
                    if parsed.version != (4 if family == socket.AF_INET else 6):
                        raise ValueError()
                    if family == socket.AF_INET6 and any(
                        type(v) is not int or not 0 <= v <= 0xFFFFFFFF for v in address[2:]
                    ):
                        raise ValueError()
                    result.append((family, tuple(address)))
                return result
            except (ValueError, TypeError, UnicodeError, OverflowError):
                raise Error("network", "Invalid DNS resolver reply") from None
        except Error:
            raise
        except (OSError, ValueError):
            cancel.check(deadline_ms)
            raise Error("network", "Cannot start DNS resolver") from None
        finally:
            if process is not None:
                if process.poll() is None:
                    try:
                        process.kill()
                    except OSError:
                        if process.poll() is None:
                            raise
                process.wait()
                if reader is not None and reader.ident is not None:
                    reader.join()
                if process.stdout is not None:
                    process.stdout.close()
                with self._lock:
                    self._processes.discard(process)
            self._slots.release()

    def close(self, timeout: float = 30) -> bool:
        with self._lock:
            self._closed = True
            processes = list(self._processes)
        for process in processes:
            if process.poll() is None:
                try:
                    process.kill()
                except OSError:
                    pass
        end = time.monotonic() + max(0, timeout)
        while True:
            with self._lock:
                if not self._processes:
                    return True
            if time.monotonic() >= end:
                return False
            time.sleep(0.005)
