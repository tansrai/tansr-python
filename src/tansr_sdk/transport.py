"""标准库 HTTP 传输；同一截止覆盖排队、DNS、连接、TLS、头和正文。"""

import http.client
import errno
import io
import math
import re
import select
import socket
import ssl
import threading
import time
from dataclasses import dataclass
from typing import Callable, Dict, Iterator, Optional, cast
from urllib.parse import urlsplit

from ._resolver import Resolver
from .errors import Error
from .lifecycle import CancellationToken, now_ms

_TOKEN = re.compile(r"^[!#$%&'*+.^_`|~0-9A-Za-z-]+$")
_HEADER_BYTES = 65536


def _tls_runtime_supported() -> bool:
    # CVE-2022-0778 在验签前解析证书即可触发；取消线程不能打断库内死循环。
    # 这是已知风险的最低运行门，不表示旧 OpenSSL 已恢复安全维护。
    if not ssl.OPENSSL_VERSION.startswith("OpenSSL "):
        return False
    version = ssl.OPENSSL_VERSION_NUMBER
    return 0x101010EF <= version < 0x20000000 or 0x30000020 <= version < 0x40000000


def _wait_socket(exchange, writing: bool = False) -> None:
    while True:
        exchange.check()
        sock = exchange.sock
        ready = select.select(
            [] if writing else [sock], [sock] if writing else [], [sock], min(0.025, exchange.remaining())
        )
        exchange.check()
        if any(ready):
            return


class _SocketReader(io.RawIOBase):
    def __init__(self, adapter) -> None:
        super().__init__()
        self._adapter = adapter

    def readable(self) -> bool:
        return True

    def readinto(self, target) -> int:
        if self.closed:
            raise ValueError("read from closed stream")
        value = self._adapter.recv(len(target))
        target[: len(value)] = value
        return len(value)

    def close(self) -> None:
        if not self.closed:
            try:
                self._adapter.release_file()
            finally:
                super().close()


class _SocketAdapter:
    """只适配可取消I/O；HTTP响应解析仍使用标准库HTTPResponse。"""

    def __init__(self, exchange) -> None:
        self.exchange = exchange
        self._lock = threading.Lock()
        self._files = 0
        self._closed = False

    def makefile(self, mode: str):
        if mode != "rb":
            raise ValueError("Unsupported HTTP stream mode")
        with self._lock:
            if self._closed:
                raise OSError("Socket is closed")
            self._files += 1
        return io.BufferedReader(_SocketReader(self), buffer_size=8192)

    def release_file(self) -> None:
        with self._lock:
            self._files -= 1
            close = self._closed and self._files == 0
        if close:
            self.exchange.sock.close()

    def close(self) -> None:
        with self._lock:
            self._closed = True
            close = self._files == 0
        if close:
            self.exchange.sock.close()

    def recv(self, size: int) -> bytes:
        if not size:
            return b""
        while True:
            self.exchange.check()
            try:
                return self.exchange.sock.recv(size)
            except ssl.SSLWantWriteError:
                _wait_socket(self.exchange, True)
            except (ssl.SSLWantReadError, BlockingIOError, InterruptedError):
                _wait_socket(self.exchange)

    def sendall(self, data) -> None:
        view = memoryview(data)
        offset = 0
        while offset < len(view):
            self.exchange.check()
            try:
                sent = self.exchange.sock.send(view[offset:])
                if not sent:
                    raise OSError("Connection closed during write")
                offset += sent
            except ssl.SSLWantReadError:
                _wait_socket(self.exchange)
            except (ssl.SSLWantWriteError, BlockingIOError, InterruptedError):
                _wait_socket(self.exchange, True)


@dataclass(frozen=True)
class HttpRequest:
    method: str
    url: str
    headers: Dict[str, str]
    body: Optional[bytes]
    deadline_ms: int
    cancel: CancellationToken
    max_response_bytes: int = 8388608


@dataclass(frozen=True)
class HttpResponse:
    status: int
    headers: Dict[str, str]
    body: bytes


class _HeaderReader:
    def __init__(self, wrapped) -> None:
        self._wrapped = wrapped
        self._remaining = _HEADER_BYTES
        self._headers = True

    def readline(self, limit: int = -1) -> bytes:
        if not self._headers:
            return self._wrapped.readline(limit)
        size = self._remaining + 1 if limit < 0 else min(limit, self._remaining + 1)
        value = self._wrapped.readline(size)
        self._remaining -= len(value)
        if self._remaining < 0:
            raise Error("resource_limit", "Response headers too large")
        return value

    def __getattr__(self, name):
        return getattr(self._wrapped, name)


class _BoundedResponse(http.client.HTTPResponse):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._bounded_reader = _HeaderReader(self.fp)
        # HTTPResponse仅依赖文件读接口；包装器保留原reader并委托全部非头部操作。
        self.fp = cast(io.BufferedReader, self._bounded_reader)

    def begin(self) -> None:
        super().begin()
        if self.fp is not None:
            self._bounded_reader._headers = False


def _prepare(req: HttpRequest):
    if not isinstance(req, HttpRequest) or not isinstance(req.cancel, CancellationToken):
        raise Error("invalid_argument")
    if type(req.deadline_ms) is not int or type(req.max_response_bytes) is not int or req.max_response_bytes < 0:
        raise Error("invalid_argument")
    if not isinstance(req.method, str) or not _TOKEN.fullmatch(req.method):
        raise Error("invalid_argument")
    if not isinstance(req.url, str) or not req.url or any(ord(c) < 33 or ord(c) > 126 for c in req.url):
        raise Error("invalid_argument")
    try:
        parsed = urlsplit(req.url)
        if (
            parsed.scheme not in ("http", "https")
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.fragment
        ):
            raise ValueError()
        host = parsed.hostname
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        if parsed.port == 0 or not 0 < port < 65536 or "%" in host or "\\" in req.url:
            raise ValueError()
        target = (parsed.path or "/") + (("?" + parsed.query) if parsed.query else "")
        target.encode("ascii")
    except (ValueError, UnicodeError):
        raise Error("invalid_argument", "Invalid HTTP URL") from None
    if req.body is not None and not isinstance(req.body, bytes):
        raise Error("invalid_argument", "Request body must be bytes")
    if not isinstance(req.headers, dict):
        raise Error("invalid_argument")
    headers = {}  # type: Dict[str, str]
    total = 0
    for name, value in req.headers.items():
        if not isinstance(name, str) or not _TOKEN.fullmatch(name) or not isinstance(value, str):
            raise Error("invalid_argument", "Invalid HTTP header")
        if any(ord(char) < 32 or ord(char) > 126 for char in value):
            raise Error("invalid_argument", "Invalid HTTP header value")
        key = name.lower()
        if key in headers or key in (
            "host",
            "transfer-encoding",
            "connection",
            "proxy-authorization",
            "proxy-connection",
            "upgrade",
            "expect",
        ):
            raise Error("invalid_argument", "Conflicting or unsupported HTTP header")
        if key == "accept-encoding" and value.lower() != "identity":
            raise Error("invalid_argument", "Encoded responses are unsupported")
        headers[key] = value
        total += len(name) + len(value) + 4
    if len(headers) > 100 or total > _HEADER_BYTES:
        raise Error("resource_limit")
    body = req.body
    if "content-length" in headers:
        value = headers["content-length"]
        if not value or not all("0" <= c <= "9" for c in value) or len(value) > 20 or int(value) != len(body or b""):
            raise Error("invalid_argument", "Invalid request length")
    headers["connection"] = "close"
    headers["accept-encoding"] = "identity"
    return parsed.scheme, host, port, target, headers, body


def _headers(response) -> Dict[str, str]:
    result = {}  # type: Dict[str, str]
    for key, value in response.getheaders():
        if not _TOKEN.fullmatch(key) or any(ord(c) < 32 and c != "\t" or ord(c) == 127 for c in value):
            raise Error("contract", "Invalid response header")
        key = key.lower()
        if key in result:
            # 认证/围栏/长度等单值头不允许由不同解释器任选一个。
            control = {
                "content-length",
                "transfer-encoding",
                "content-type",
                "content-encoding",
                "etag",
                "retry-after",
                "location",
            }
            if key.startswith("tansr-") or key in control:
                raise Error("contract", "Duplicate response header")
            if key != "set-cookie":
                result[key] += ", " + value
            continue  # 传输不维护cookie jar。
        result[key] = value
    if "content-length" in result:
        value = result["content-length"]
        if not value or len(value) > 20 or not all("0" <= c <= "9" for c in value):
            raise Error("contract", "Invalid response length")
    if "transfer-encoding" in result:
        if "content-length" in result or result["transfer-encoding"].lower().strip() != "chunked":
            raise Error("contract", "Ambiguous response framing")
    if result.get("content-encoding", "identity").lower().strip() != "identity":
        raise Error("contract", "Encoded response was not negotiated")
    return result


class _Exchange:
    def __init__(self, owner, request: HttpRequest, streaming: bool) -> None:
        self.owner = owner
        self.request = request
        self.streaming = streaming
        self.cancel = CancellationToken()
        self.end = time.monotonic() + max(0, request.deadline_ms - now_ms()) / 1000.0
        self.lock = threading.RLock()
        self.sock = None  # type: Optional[socket.socket]
        self.connection = None  # type: Optional[http.client.HTTPConnection]
        self.response = None  # type: Optional[http.client.HTTPResponse]
        self.timer = None  # type: Optional[threading.Timer]
        self.unregister: Optional[Callable[[], None]] = None
        self.closed = False
        self.disposed = False
        self.in_io = True
        self.timed_out = False

    def start(self) -> None:
        self.cancel.register(self.finish)
        self.unregister = self.request.cancel.register(self.cancel.cancel)
        with self.lock:
            if not self.closed:
                self.timer = threading.Timer(max(0, self.end - time.monotonic()), self.expire)
                self.timer.name = "tansr-http-deadline"
                self.owner._watch(self.timer)
                try:
                    self.timer.start()
                except RuntimeError:
                    raise Error("resource_limit", "Cannot start deadline observer") from None
        self.check()

    def expire(self) -> None:
        with self.lock:
            self.timed_out = True
        self.cancel.cancel()

    def check(self) -> None:
        if self.timed_out or time.monotonic() >= self.end:
            raise Error("timeout")
        self.cancel.check(self.request.deadline_ms)
        self.request.cancel.check(self.request.deadline_ms)
        if self.closed:
            raise Error("closed")

    def remaining(self) -> float:
        self.check()
        return max(0.001, min(self.end - time.monotonic(), (self.request.deadline_ms - now_ms()) / 1000.0))

    def set_socket(self, value) -> None:
        with self.lock:
            if self.closed:
                value.close()
                self.check()
                raise Error("closed")
            self.sock = value

    def begin_io(self) -> None:
        with self.lock:
            self.check()
            if self.in_io:
                raise Error("invalid_state", "Concurrent stream readers")
            self.in_io = True

    def end_io(self) -> None:
        with self.lock:
            self.in_io = False
            dispose = self.closed
        if dispose:
            self._dispose()

    def finish(self) -> None:
        with self.lock:
            self.closed = True
            sock = self.sock
            dispose = not self.in_io
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                sock.close()
            except OSError:
                pass
        if dispose:
            self._dispose()

    def _dispose(self) -> None:
        with self.lock:
            if self.disposed or self.in_io:
                return
            self.disposed = True
            timer = self.timer
        if timer is not None:
            timer.cancel()
            if timer is not threading.current_thread() and timer.ident is not None:
                timer.join()
        try:
            if self.response is not None:
                self.response.close()
            if self.connection is not None:
                self.connection.close()
        finally:
            if self.unregister is not None:
                self.unregister()
            self.owner._release(self)

    def failure(self, exc: Exception) -> Error:
        try:
            self.check()
        except Error as error:
            return error
        if isinstance(exc, Error):
            return exc
        if isinstance(exc, (socket.timeout, TimeoutError)):
            return Error("timeout")
        if isinstance(exc, ssl.SSLError):
            return Error("tls", "TLS handshake or verification failed")
        return Error("network", "HTTP transport failed")


class HttpStream:
    def __init__(self, exchange: _Exchange, chunk_size: int) -> None:
        self._exchange = exchange
        self._chunk_size = chunk_size
        response = exchange.response
        if response is None:
            raise Error("invalid_state", "HTTP response is not available")
        self._response = response
        self.status = response.status
        self.headers = _headers(response)
        self._lock = threading.Lock()
        self._started = False
        self._closed = False

    def iter_bytes(self) -> Iterator[bytes]:
        with self._lock:
            if self._closed or self._started:
                raise Error("closed" if self._closed else "invalid_state")
            self._started = True
        return self._iterate()

    def _iterate(self) -> Iterator[bytes]:
        exchange = self._exchange
        try:
            while True:
                exchange.begin_io()
                try:
                    exchange.check()
                    data = self._response.read1(self._chunk_size)
                    exchange.check()
                    if not data and self._response.length not in (None, 0):
                        raise Error("network", "Response ended before declared length")
                except (Error, OSError, http.client.HTTPException, ValueError) as exc:
                    raise exchange.failure(exc) from None
                finally:
                    exchange.end_io()
                if not data:
                    return
                exchange.check()
                yield data
        finally:
            self.close()

    def close(self) -> None:
        with self._lock:
            self._closed = True
        self._exchange.cancel.cancel()
        self._exchange.finish()

    def __iter__(self) -> Iterator[bytes]:
        return self.iter_bytes()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()


class HttpTransport:
    def __init__(
        self,
        *,
        ssl_context: Optional[ssl.SSLContext] = None,
        ca_file: Optional[str] = None,
        max_connections: int = 16,
        max_dns_processes: int = 2,
        read_chunk_bytes: int = 65536,
        max_streams: Optional[int] = None,
        resolver=None,
    ) -> None:
        if (
            type(max_connections) is not int
            or max_connections < 1
            or type(read_chunk_bytes) is not int
            or not 1 <= read_chunk_bytes <= 1048576
        ):
            raise Error("invalid_argument")
        if ssl_context is not None and ca_file is not None:
            raise Error("invalid_argument")
        if max_streams is None:
            max_streams = min(8, max_connections - 1)
        if type(max_streams) is not int or not 0 <= max_streams < max_connections:
            raise Error("invalid_argument", "Streams must leave capacity for control requests")
        if ssl_context is not None and not isinstance(ssl_context, ssl.SSLContext):
            raise Error("invalid_argument")
        try:
            context = ssl_context or ssl.create_default_context(cafile=ca_file)
            if ssl_context is None:
                context.minimum_version = ssl.TLSVersion.TLSv1_2
            if (
                context.verify_mode != ssl.CERT_REQUIRED
                or not context.check_hostname
                or context.minimum_version < ssl.TLSVersion.TLSv1_2
            ):
                raise Error("invalid_argument", "Verified TLS1.2+ context is required")
        except (OSError, ValueError, TypeError):
            raise Error("invalid_argument", "Invalid TLS configuration") from None
        self._ssl_context = context
        self._max_connections = max_connections
        self._max_streams = max_streams
        self._chunk_size = read_chunk_bytes
        self._resolver = resolver if resolver is not None else Resolver(max_dns_processes)
        self._owns_resolver = resolver is None
        self._condition = threading.Condition()
        self._active = set()  # type: set
        self._watchers = set()  # type: set
        self._closed = False

    def _watch(self, timer) -> None:
        with self._condition:
            self._watchers = {value for value in self._watchers if value.ident is None or value.is_alive()}
            self._watchers.add(timer)

    def _release(self, exchange: _Exchange) -> None:
        with self._condition:
            self._active.discard(exchange)
            self._condition.notify_all()

    def _admit(self, req: HttpRequest, streaming: bool) -> _Exchange:
        end = time.monotonic() + max(0, req.deadline_ms - now_ms()) / 1000.0
        if streaming and not self._max_streams:
            raise Error("resource_limit", "Streaming capacity is disabled")
        with self._condition:
            while True:
                req.cancel.check(req.deadline_ms)
                if time.monotonic() >= end:
                    raise Error("timeout")
                if self._closed:
                    raise Error("closed")
                streams = sum(1 for active in self._active if active.streaming)
                if len(self._active) < self._max_connections and (not streaming or streams < self._max_streams):
                    exchange = _Exchange(self, req, streaming)
                    self._active.add(exchange)
                    return exchange
                self._condition.wait(min(0.025, max(0.001, (req.deadline_ms - now_ms()) / 1000.0)))

    def stream(self, req: HttpRequest) -> HttpStream:
        return self._open(req, streaming=True)

    def _open(self, req: HttpRequest, streaming: bool) -> HttpStream:
        scheme, host, port, target, headers, body = _prepare(req)
        if scheme == "https" and not _tls_runtime_supported():
            raise Error("unsupported_tls_runtime", "Use a supported, patched OpenSSL runtime for HTTPS")
        exchange = self._admit(req, streaming)
        try:
            exchange.start()
            addresses = self._resolver.resolve(host, port, req.deadline_ms, exchange.cancel)
            last = None
            for family, address in addresses:
                exchange.check()
                sock = socket.socket(family, socket.SOCK_STREAM)
                exchange.set_socket(sock)
                try:
                    sock.setblocking(False)
                    status = sock.connect_ex(address)
                    pending = (errno.EINPROGRESS, errno.EWOULDBLOCK, errno.EALREADY, 10035, 10036, 10037)
                    if status not in (0, errno.EISCONN):
                        if status not in pending:
                            raise OSError(status, "Connection failed")
                        _wait_socket(exchange, True)
                        status = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
                        if status:
                            raise OSError(status, "Connection failed")
                    last = None
                    break
                except OSError as exc:
                    sock.close()
                    last = exc
                    exchange.check()
            if last is not None or not addresses:
                raise Error("network", "Connection failed")
            if scheme == "https":
                # wrap本身不做网络I/O；持锁避免取消落在socket所有权转移缝隙。
                with exchange.lock:
                    exchange.check()
                    sock = self._ssl_context.wrap_socket(sock, server_hostname=host, do_handshake_on_connect=False)
                    exchange.sock = sock
                while True:
                    exchange.check()
                    try:
                        sock.do_handshake()
                        break
                    except ssl.SSLWantReadError:
                        _wait_socket(exchange)
                    except ssl.SSLWantWriteError:
                        _wait_socket(exchange, True)
                exchange.check()
            connection = http.client.HTTPConnection(host, port, timeout=exchange.remaining())
            connection.response_class = _BoundedResponse
            connection.sock = _SocketAdapter(exchange)
            exchange.connection = connection
            connection.request(req.method, target, body=body, headers=headers)
            exchange.check()
            exchange.response = connection.getresponse()
            exchange.check()
            result = HttpStream(exchange, self._chunk_size)
            exchange.end_io()
            exchange.check()
            return result
        except BaseException as exc:
            error = exchange.failure(exc) if isinstance(exc, Exception) else None
            exchange.end_io()
            exchange.finish()
            if error is not None:
                raise error from None
            raise

    def request(self, req: HttpRequest) -> HttpResponse:
        stream = self._open(req, streaming=False)
        try:
            length = stream.headers.get("content-length")
            if req.method.upper() != "HEAD" and length is not None and int(length) > req.max_response_bytes:
                raise Error("resource_limit", "Response too large")
            chunks = []
            size = 0
            for chunk in stream.iter_bytes():
                size += len(chunk)
                if size > req.max_response_bytes:
                    raise Error("resource_limit", "Response too large")
                chunks.append(chunk)
            return HttpResponse(stream.status, stream.headers, b"".join(chunks))
        finally:
            stream.close()

    def close(self, timeout: float = 30) -> bool:
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (float, int))
            or not math.isfinite(timeout)
            or timeout < 0
        ):
            raise Error("invalid_argument")
        end = time.monotonic() + timeout
        with self._condition:
            self._closed = True
            active = list(self._active)
        for exchange in active:
            exchange.cancel.cancel()
        if self._owns_resolver:
            self._resolver.close(timeout=max(0, end - time.monotonic()))
        with self._condition:
            while self._active:
                remaining = end - time.monotonic()
                if remaining <= 0:
                    return False
                self._condition.wait(remaining)
            watchers = list(self._watchers)
        for watcher in watchers:
            if watcher is threading.current_thread():
                return False
            if watcher.ident is not None:
                watcher.join(max(0, end - time.monotonic()))
        return not any(watcher.is_alive() for watcher in watchers)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()
