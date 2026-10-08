"""统一合同调用、同身份重放与单引擎异步门面。"""

import asyncio
import dataclasses
import datetime
import hashlib
import re
import threading
import time
import weakref
from urllib.parse import quote, urlsplit

from .. import canonical, strict_json
from ..errors import Error, MISSING
from ..lifecycle import AsyncBridge, CancellationToken, now_ms
from ..transport import HttpRequest, HttpResponse, HttpTransport
from ..sse import frames
from .types import ApiResponse, AuthToken, CallOptions
from .operations import get_operation, manifest
from .schema import validate_wire
from ._schema_data import SCHEMAS

_HEX = re.compile(r"[0-9a-f]{64}\Z")
_REV = re.compile(r"(?:0|[1-9][0-9]{0,18})\Z")
_RESPONSE_FIELDS = frozenset(SCHEMAS["unified-v1"]["definitions"]["ResponseHeaders"]["properties"])
_CONTROL = {
    "tansr-contract",
    "tansr-manifest-revision",
    "tansr-domain",
    "tansr-schema-hash",
    "tansr-closure-id",
    "tansr-event-envelope",
    "content-type",
    "etag",
    "retry-after",
    "x-request-id",
}


def _invalid(message):
    return Error("invalid_input", message)


def _contract(message):
    return Error("contract", message)


def _visible(value, maximum, spaces=False):
    return (
        isinstance(value, str)
        and 0 < len(value) <= maximum
        and all((32 if spaces else 33) <= ord(char) <= 126 for char in value)
    )


def _revision(value):
    if not isinstance(value, str):
        raise _invalid("revision must be a string")
    value = value.strip(" \t")
    if value.startswith('"') and value.endswith('"'):
        value = value[1:-1]
    if not _REV.fullmatch(value):
        raise _invalid("invalid strong revision")
    return value


def _origin(value):
    if not _visible(value, 8192) or any(char in value for char in "\\?#@"):
        raise _invalid("invalid Serve origin")
    try:
        url = urlsplit(value)
        if (
            url.scheme not in ("http", "https")
            or not url.hostname
            or url.path not in ("", "/")
            or url.username
            or url.password
        ):
            raise ValueError()
        if url.port is not None and not 0 < url.port <= 65535:
            raise ValueError()
    except ValueError:
        raise _invalid("Serve URL must be an HTTP(S) origin") from None
    return value.rstrip("/")


def _reference(ref, value):
    if ref is not None:
        family, sep, definition = ref.partition("#")
        if not sep:
            raise _contract("invalid generated schema reference")
        if family != "agent-session-v1":
            validate_wire(family, definition, value)


def _at(value, path):
    for key in path or []:
        if not isinstance(value, dict) or key not in value:
            return MISSING
        value = value[key]
    return value if path else MISSING


def _map(value, path, mapped):
    if value is MISSING or not path:
        return
    for offset, key in enumerate(path):
        if not isinstance(value, dict):
            raise _invalid("header mapping requires object body")
        if offset == len(path) - 1:
            if key in value and strict_json.dumps(value[key]) != strict_json.dumps(mapped):
                raise _invalid("header and body conflict")
            value[key] = mapped
        else:
            value = value.setdefault(key, {})


def _path(op, opts):
    used = set()
    segments = []
    for segment in op["apiPath"].split("/"):
        if segment.startswith(":"):
            key = segment[1:]
            value = opts.parameters.get(key)
            if (
                not isinstance(value, str)
                or not value
                or len(value) > 512
                or value in (".", "..")
                or any(ord(c) < 32 or ord(c) == 127 or c in "/\\" for c in value)
            ):
                raise _invalid("invalid path parameter")
            try:
                segment = quote(value, safe="-._~", encoding="utf-8", errors="strict")
            except UnicodeError:
                raise _invalid("invalid path UTF-8") from None
            used.add(key)
        segments.append(segment)
    if used != set(opts.parameters):
        raise _invalid("unexpected path parameter")
    declared = op.get("query") or []
    if set(opts.query) - set(declared):
        raise _invalid("unknown query parameter")
    queries = []
    family = op.get("family")
    for key in declared:
        value = opts.query.get(key)
        if value is None and key == "protocol":
            if family in ("agent-session-v1", "sdk2-ext-v1", "sdk2-archive-recovery-v1"):
                value = "sdk2-ext-v1"
            elif family in ("sdk2-cache-v1", "sdk2-cache-core-v1"):
                value = family
        if (
            value is None
            and key == "contract"
            and family in ("terminal-services-v1", "terminal-observation-v1", "terminal-profile-v1")
        ):
            value = family
        if value is not None:
            if not isinstance(value, str) or len(value) > 8192:
                raise _invalid("invalid query value")
            try:
                queries.append(quote(key, safe="-._~") + "=" + quote(value, safe="-._~"))
            except UnicodeError:
                raise _invalid("invalid query UTF-8") from None
    return "/".join(segments) + (("?" + "&".join(queries)) if queries else "")


def _headers(raw):
    result = {}
    for key, value in raw.items() if hasattr(raw, "items") else raw:
        key = key.lower()
        if not isinstance(value, str) or any(c in value for c in "\r\n\x00"):
            raise _contract("invalid response header")
        if key in _CONTROL and key in result:
            raise _contract("duplicate control header")
        result[key] = value.strip(" \t")
    return result


def _metadata(response):
    headers = _headers(response.headers)
    try:
        validate_wire("unified-v1", "ResponseHeaders", {k: v for k, v in headers.items() if k in _RESPONSE_FIELDS})
    except Error:
        raise _contract("invalid unified response headers") from None
    if headers.get("tansr-contract") != "unified-v1":
        raise _contract("unified contract marker mismatch")
    revision = headers.get("tansr-manifest-revision", "")
    if not re.fullmatch(r"[1-9][0-9]{0,9}", revision):
        raise _contract("invalid manifest revision header")
    schema_hash = headers.get("tansr-schema-hash", "")
    if schema_hash != "none" and not re.fullmatch(r"sha256:[0-9a-f]{64}", schema_hash):
        raise _contract("invalid schema hash header")
    domain = headers.get("tansr-domain", "")
    closure = headers.get("tansr-closure-id")
    if closure is not None and not _HEX.fullmatch(closure):
        raise _contract("invalid response closure")
    if headers.get("tansr-event-envelope", "unified-v1") != "unified-v1":
        raise _contract("invalid event envelope negotiation")
    etag = headers.get("etag")
    if etag is not None:
        try:
            if not etag.startswith('"') or not etag.endswith('"'):
                etag = None
            else:
                _revision(etag)
        except Error:
            etag = None
    return ApiResponse(
        response.status,
        headers=headers,
        etag=etag,
        capability_closure=closure,
        domain=domain,
        content_type=headers.get("content-type", "").split(";", 1)[0].strip().lower(),
    )


def _retry_delay(headers):
    value = headers.get("retry-after")
    if value is None:
        return None
    # 统一合同只允许正整数字符串，不能按通用HTTP规则接受日期/小数/指数。
    if not re.fullmatch(r"[1-9][0-9]{0,18}", value):
        raise _contract("unsupported Retry-After value")
    return int(value) * 1000


def _fingerprint(request):
    chunks = [request.method.encode(), b"\0", request.url.encode(), b"\0"]
    for key, value in sorted(request.headers.items()):
        if key.lower() != "authorization":
            chunks.extend((key.lower().encode(), b"\0", value.encode(), b"\0"))
    chunks.append(request.body or b"")
    return hashlib.sha256(b"".join(chunks)).hexdigest()


def _error(reply, meta, op):
    if meta.content_type != "application/json":
        raise _contract("non-JSON error response")
    body = strict_json.loads(reply.body)
    if not isinstance(body, dict) or body.get("contract") != "unified-v1":
        if op.get("family") == "archive-sync-v1" and isinstance(body, dict):
            return Error("http", http_status=reply.status, detail=body)
        raise _contract("unknown error envelope")
    validate_wire("unified-v1", "UnifiedError", body)
    if body["status"] != reply.status:
        raise _contract("error HTTP status mismatch")
    return Error(
        "http",
        http_status=reply.status,
        wire_code=body["code"],
        retry_action=body["retryAction"],
        request_id=body.get("requestId"),
        retry_after_ms=body.get("retryAfterMs", _retry_delay(meta.headers)),
        detail=body.get("detail"),
    )


class Client:
    def __init__(
        self,
        base_url,
        token_provider,
        family="sdk1",
        *,
        transport=None,
        timeout=30,
        max_response_bytes=8 * 1024 * 1024,
        max_pending=64,
    ):
        self.base_url = _origin(base_url)
        if family not in ("sdk1", "sdk2-offload-v1"):
            raise _invalid("unsupported session family")
        if not callable(token_provider) or not isinstance(timeout, (int, float)) or not 0 < timeout <= 86400:
            raise _invalid("invalid client configuration")
        if not isinstance(max_response_bytes, int) or not 1024 <= max_response_bytes <= 256 * 1024 * 1024:
            raise _invalid("invalid response capacity")
        self.family = family
        self._provider = token_provider
        self._transport = transport if transport is not None else HttpTransport()
        self._timeout = timeout
        self._max_bytes = max_response_bytes
        if isinstance(max_pending, bool) or not isinstance(max_pending, int) or not 1 <= max_pending <= 4096:
            raise _invalid("invalid pending request capacity")
        self._max_pending = max_pending
        self._condition = threading.Condition()
        self._closed = False
        self._active = set()
        self._owners = {}
        self._principal = None
        self._identity = object()

    def default_deadline_ms(self):
        return now_ms() + int(self._timeout * 1000)

    def snapshot_options(self, options=None, **kwargs):
        if options is None:
            options = CallOptions(**kwargs)
        elif kwargs:
            options = dataclasses.replace(options, **kwargs)
        if not isinstance(options, CallOptions):
            raise _invalid("expected CallOptions")
        result = dataclasses.replace(options)
        result.parameters = dict(options.parameters)
        result.query = dict(options.query)
        result.body = MISSING if options.body is MISSING else strict_json.snapshot(options.body)
        if options.raw_body is not None:
            if not isinstance(options.raw_body, (bytes, bytearray, memoryview)):
                raise _invalid("raw body must be bytes")
            result.raw_body = bytes(options.raw_body)
        if result.deadline_ms is None:
            result.deadline_ms = self.default_deadline_ms()
        if (
            isinstance(result.deadline_ms, bool)
            or not isinstance(result.deadline_ms, int)
            or not 0 < result.deadline_ms <= 253402300799999
        ):
            raise _invalid("invalid deadline")
        if result.cancel is not None and not isinstance(result.cancel, CancellationToken):
            raise _invalid("invalid cancellation token")
        result.deadline_ms = int(result.deadline_ms)
        return result

    def _begin(self, opts):
        token = CancellationToken()
        unregister = opts.cancel.register(token.cancel) if opts.cancel is not None else lambda: None
        with self._condition:
            if self._closed:
                unregister()
                raise Error("closed")
            if len(self._active) >= self._max_pending:
                unregister()
                raise Error("capacity", "client pending request capacity reached")
            self._active.add(token)
            self._owners[token] = threading.get_ident()
        timer = threading.Timer(max(0, (opts.deadline_ms - now_ms()) / 1000), token.cancel)
        timer.daemon = True
        timer.start()
        cleaned = threading.Event()

        def finish():
            if not cleaned.is_set():
                cleaned.set()
                timer.cancel()
                unregister()
                with self._condition:
                    self._active.discard(token)
                    self._owners.pop(token, None)
                    self._condition.notify_all()

        return token, finish

    def _check(self, token, deadline):
        if now_ms() >= deadline:
            raise Error("timeout")
        token.check()

    def _prepare(self, op, opts, token, stream=False):
        self._check(token, opts.deadline_ms)
        path = _path(op, opts)
        if opts.body is not MISSING and opts.raw_body is not None:
            raise _invalid("JSON and byte body are mutually exclusive")
        if opts.raw_body is not None and op["name"] != "session.checkpoint.import":
            raise _invalid("byte body is not declared")
        if stream and (opts.body is not MISSING or opts.raw_body is not None):
            raise _invalid("event stream has no body")
        capacity = self._max_bytes if opts.max_response_bytes is None else opts.max_response_bytes
        if isinstance(capacity, bool) or not isinstance(capacity, int) or not 0 < capacity <= 256 * 1024 * 1024:
            raise _invalid("invalid response capacity")
        capacity = int(capacity)
        headers = {
            "accept": "text/event-stream" if stream else "application/json, application/octet-stream",
            "tansr-session-family": self.family,
        }
        if stream:
            headers["tansr-event-envelope"] = "unified-v1"
        effective = MISSING if opts.body is MISSING else strict_json.snapshot(opts.body)
        if opts.capability_closure is not None:
            if (
                not isinstance(opts.capability_closure, str)
                or not _HEX.fullmatch(opts.capability_closure)
                or op["kind"] != "write"
                or not op["apiPath"].startswith("/api/sessions/:id/")
            ):
                raise _invalid("invalid or inapplicable capability closure")
            headers["tansr-closure-id"] = opts.capability_closure
        if opts.request_key is not None:
            if op["kind"] != "write" or not _visible(opts.request_key, 128):
                raise _invalid("invalid request key")
            _map(effective, op.get("requestIdPath"), opts.request_key)
            headers["idempotency-key"] = opts.request_key
        if opts.if_match is not None:
            revision = _revision(opts.if_match)
            expected = op.get("expectedRevision")
            if op["kind"] != "write" or not expected:
                raise _invalid("If-Match is not applicable")
            mapped = revision
            if expected["kind"] == "integer":
                mapped = int(revision)
                if mapped > 9007199254740991:
                    raise _invalid("unsafe revision")
            _map(effective, expected["path"], mapped)
            headers["if-match"] = '"' + revision + '"'
        seconds, millis = divmod(opts.deadline_ms, 1000)
        headers["deadline"] = (
            datetime.datetime.fromtimestamp(seconds, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
            + ".%03dZ" % millis
        )
        if opts.last_event_id is not None:
            if not stream or not _visible(opts.last_event_id, 256, spaces=True):
                raise _invalid("invalid event cursor")
            headers["last-event-id"] = opts.last_event_id
        if effective is not MISSING:
            _reference(op.get("request"), effective)
        elif (op.get("request") and op.get("family") != "agent-session-v1"
              and op["method"] not in ("GET", "HEAD") and opts.raw_body is None):
            raise _invalid("declared control request body is required")
        body = b""
        if opts.raw_body is not None:
            body = opts.raw_body
            headers["content-type"] = "application/octet-stream"
        elif opts.body is not MISSING:
            # Header-mapped fields validate the effective request. Original body bytes remain immutable.
            body = (
                canonical.encode(opts.body)
                if op.get("family") not in (None, "agent-session-v1", "unified-v1")
                else strict_json.dumps(opts.body)
            )
            headers["content-type"] = "application/json"
        if opts.content_type is not None and opts.content_type != headers.get("content-type", "application/json"):
            raise _invalid("content type differs from declared body")
        if len(body) > capacity:
            raise _invalid("request exceeds capacity")
        return HttpRequest(op["method"], self.base_url + path, headers, body, opts.deadline_ms, token, capacity)

    def _authenticate(self, request):
        self._check(request.cancel, request.deadline_ms)
        token = self._provider(request.cancel)
        self._check(request.cancel, request.deadline_ms)
        if isinstance(token, tuple) and len(token) == 2:
            token = AuthToken(*token)
        if not isinstance(token, AuthToken) or not _visible(token.value, 16384):
            raise _invalid("invalid authentication token")
        if not isinstance(token.principal, str) or not token.principal:
            raise _invalid("authentication principal required")
        try:
            token.principal.encode("utf-8", "strict")
        except UnicodeError:
            raise _invalid("invalid principal UTF-8") from None
        with self._condition:
            if self._principal is not None and self._principal != token.principal:
                raise Error("permission", "principal changed; create a new client")
            self._principal = token.principal
        request.headers["authorization"] = "Bearer " + token.value

    def _decode(self, reply, op, request, opts):
        if 300 <= reply.status < 400:
            raise _contract("redirect refused")
        meta = _metadata(reply)
        if len(reply.body) > request.max_response_bytes:
            raise _contract("response exceeds capacity")
        if reply.status >= 400:
            error = _error(reply, meta, op)
            error._replay = (
                self._identity,
                op["name"],
                _fingerprint(request),
                opts.deadline_ms,
                error.wire_code,
                error.retry_action,
                error.request_id,
                strict_json.dumps(error.detail),
                error.retry_after_ms,
            )
            raise error
        if not 200 <= reply.status < 300:
            raise _contract("unexpected HTTP status")
        meta.raw_body = reply.body
        if meta.status != 204 and meta.content_type not in ("application/json", "application/octet-stream"):
            raise _contract("unexpected response content type")
        if meta.content_type == "application/octet-stream" and op["name"] != "session.checkpoint.export":
            raise _contract("undeclared byte response")
        if op.get("response") and (meta.status == 204 or meta.content_type != "application/json" or not reply.body):
            raise _contract("schema response requires JSON")
        if meta.content_type == "application/json" and meta.status != 204:
            meta.body = strict_json.loads(reply.body)
            _reference(op.get("response"), meta.body)
            name = op["name"]
            if name in ("discovery.manifest", "discovery.capabilities"):
                validate_wire("unified-v1", "Manifest" if name.endswith("manifest") else "Capabilities", meta.body)
                frozen = manifest()
                revision_key = "revision" if name.endswith("manifest") else "manifestRevision"
                schema_prefix = "" if name.endswith("manifest") else "sha256:"
                if (
                    meta.body[revision_key] != frozen["revision"]
                    or meta.headers["tansr-manifest-revision"] != str(frozen["revision"])
                    or meta.body["schemaHash"] != schema_prefix + frozen["schemaHash"]
                    or meta.headers["tansr-schema-hash"] != "sha256:" + frozen["schemaHash"]
                ):
                    raise _contract("discovery differs from frozen contract")
            elif name == "discovery.session.capabilities":
                validate_wire("unified-v1", "CapabilityClosure", meta.body)
                if meta.capability_closure is None or meta.body["closureId"] != meta.capability_closure:
                    raise _contract("closure header differs from body")
        return meta

    def call(self, operation, options=None, **kwargs):
        opts = self.snapshot_options(options, **kwargs)
        op = get_operation(operation)
        if op["kind"] == "stream":
            raise _invalid("stream cannot be used as ordinary request")
        token, finish = self._begin(opts)
        try:
            request = self._prepare(op, opts, token)
            self._authenticate(request)
            reply = self._transport.request(request)
            self._check(token, opts.deadline_ms)
            return self._decode(reply, op, request, opts)
        finally:
            finish()

    def events(self, operation="session.events.observe", options=None, **kwargs):
        opts = self.snapshot_options(options, **kwargs)
        op = get_operation(operation)
        if op["kind"] != "stream":
            raise _invalid("operation is not a stream")
        token, finish = self._begin(opts)
        stream = None
        try:
            request = self._prepare(op, opts, token, stream=True)
            self._authenticate(request)
            stream = self._transport.stream(request)
            self._check(token, opts.deadline_ms)
            if 300 <= stream.status < 400:
                raise _contract("redirect refused")
            meta = _metadata(stream)
            if stream.status >= 400:
                chunks, length = [], 0
                for chunk in stream.iter_bytes():
                    length += len(chunk)
                    if length > request.max_response_bytes:
                        raise _contract("response exceeds capacity")
                    chunks.append(chunk)
                raise _error(HttpResponse(stream.status, stream.headers, b"".join(chunks)), meta, op)
            if (
                stream.status != 200
                or meta.content_type != "text/event-stream"
                or meta.headers.get("tansr-event-envelope") != "unified-v1"
            ):
                raise _contract("unified SSE not negotiated")
            return EventStream(stream, token, opts.deadline_ms, finish)
        except BaseException:
            try:
                if stream is not None:
                    stream.close()
            except Exception:
                pass
            finally:
                finish()
            raise

    def retry_same_request(self, operation, options, previous):
        replay = getattr(previous, "_replay", None)
        if (
            not replay
            or replay[0] is not self._identity
            or replay[1] != operation
            or previous.retry_action != "same-request"
            or previous.wire_code == "result_unknown"
        ):
            raise _invalid("retry requires original same-request evidence")
        if replay[4:] != (
            previous.wire_code,
            previous.retry_action,
            previous.request_id,
            strict_json.dumps(previous.detail),
            previous.retry_after_ms,
        ):
            raise _invalid("retry evidence was modified")
        if isinstance(previous.detail, dict) and previous.detail.get("domainCode") in (
            "result_unknown",
            "commit_unknown",
        ):
            raise _invalid("unknown outcome requires explicit status reconciliation")
        if options is None:
            options = CallOptions(deadline_ms=replay[3])
        elif options.deadline_ms is None:
            options = dataclasses.replace(options, deadline_ms=replay[3])
        opts = self.snapshot_options(options)
        if opts.deadline_ms != replay[3]:
            raise _invalid("retry cannot extend original deadline")
        op = get_operation(operation)
        identity = opts.request_key or _at(opts.body, op.get("requestIdPath"))
        if not isinstance(identity, str) or not identity or (previous.request_id and identity != previous.request_id):
            raise _invalid("retry requires original request identity")
        token, finish = self._begin(opts)
        try:
            request = self._prepare(op, opts, token)
            if _fingerprint(request) != replay[2]:
                raise _invalid("retry bytes or preconditions changed")
            delay = previous.retry_after_ms or 0
            if delay >= opts.deadline_ms - now_ms():
                raise Error("timeout")
            token.wait(delay / 1000.0)
            self._check(token, opts.deadline_ms)
            self._authenticate(request)
            reply = self._transport.request(request)
            self._check(token, opts.deadline_ms)
            return self._decode(reply, op, request, opts)
        finally:
            finish()

    def close(self, timeout=30):
        with self._condition:
            if threading.get_ident() in self._owners.values():
                timeout = 0  # A provider/transport callback cannot wait for its own call.
            self._closed = True
            active = list(self._active)
        end = time.monotonic() + max(0, timeout)
        for token in active:
            token.cancel()
        transport_closed = self._transport.close(timeout=max(0, end - time.monotonic()))
        with self._condition:
            while self._active and time.monotonic() < end:
                self._condition.wait(end - time.monotonic())
            return not self._active and transport_closed is not False

    def __enter__(self):
        return self

    def __exit__(self, *unused):
        self.close()


class EventStream:
    def __init__(self, stream, cancel, deadline, finish):
        self._stream, self._cancel, self._deadline, self._finish = stream, cancel, deadline, finish
        self._frames = iter(frames(stream.iter_bytes()))
        self._reader = threading.Lock()
        self._state_lock = threading.Lock()
        self._closed = False
        self._close_done = False
        self._finished = False
        self._eof = False
        self._remove_cancel = lambda: None
        self._remove_cancel = cancel.register(self.close)

    def __iter__(self):
        return self

    def __next__(self):
        if not self._reader.acquire(False):
            raise Error("reentrant", "stream supports one reader")
        try:
            if self._closed:
                if not self._eof:
                    self._cancel.check(self._deadline)
                raise StopIteration

            def check():
                if now_ms() >= self._deadline:
                    raise Error("timeout")
                self._cancel.check()

            while True:
                check()
                try:
                    frame = next(self._frames)
                except StopIteration:
                    check()  # A shutdown EOF is not a successful end of observation.
                    self._eof = True
                    raise
                if frame.data or frame.id is not None or frame.event is not None:
                    break
            value = strict_json.loads(frame.data)
            validate_wire("unified-v1", "EventEnvelope", value)
            if value["eventId"] != frame.id or value["cursorSet"]["eventCursor"] != frame.id:
                raise _contract("event frame and envelope cursor differ")
            check()
            return frame
        except BaseException:
            try:
                self.close()
            except Exception:
                pass
            raise
        finally:
            self._reader.release()
            self._finish_if_quiet()

    def _finish_if_quiet(self):
        with self._state_lock:
            if not self._close_done or self._reader.locked() or self._finished:
                return
            self._finished = True
        self._finish()

    def close(self):
        with self._state_lock:
            if self._closed:
                return
            self._closed = True
        self._remove_cancel()
        self._cancel.cancel()
        try:
            self._stream.close()
        finally:
            with self._state_lock:
                self._close_done = True
            self._finish_if_quiet()

    cancel = close

    def __enter__(self):
        return self

    def __exit__(self, *unused):
        self.close()


_END = object()


def _next(stream):
    return next(stream, _END)


class AsyncEventStream:
    def __init__(self, stream, bridge, cancel):
        self._stream, self._bridge, self._cancel = stream, bridge, cancel
        self._reading = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._reading:
            raise Error("reentrant", "stream supports one reader")
        self._reading = True
        try:
            value = await self._bridge.run(_next, self._stream, cancel=self._cancel)
            if value is _END:
                raise StopAsyncIteration
            return value
        except asyncio.CancelledError:
            self._stream.close()
            raise
        finally:
            self._reading = False

    async def aclose(self, timeout=30):
        self._bridge.check_loop()
        self._stream.close()
        end = time.monotonic() + max(0, timeout)
        while self._reading or self._stream._reader.locked():
            if time.monotonic() >= end:
                return False
            await asyncio.sleep(0.01)
        return True

    async def __aenter__(self):
        return self

    async def __aexit__(self, *unused):
        await self.aclose()


class AsyncClient:
    """不接管宿主 event loop；stream 与控制请求使用独立有界池。"""

    def __init__(self, base_url=None, token_provider=None, family="sdk1", *, client=None, **kwargs):
        if client is not None and base_url is not None:
            raise _invalid("choose a client or client configuration")
        self.sync = client if client is not None else Client(base_url, token_provider, family, **kwargs)
        self._owns_sync = client is None
        self._calls = AsyncBridge(workers=4, max_pending=16)
        self._streams = AsyncBridge(workers=4, max_pending=8)
        self._streams_open = weakref.WeakSet()  # type: weakref.WeakSet[AsyncEventStream]

    @property
    def base_url(self):
        return self.sync.base_url

    @property
    def family(self):
        return self.sync.family

    def default_deadline_ms(self):
        return self.sync.default_deadline_ms()

    async def call(self, operation, options=None, **kwargs):
        opts = self.sync.snapshot_options(options, **kwargs)
        if opts.cancel is None:
            opts.cancel = CancellationToken()
        return await self._calls.run(self.sync.call, operation, opts, cancel=opts.cancel, _deadline_ms=opts.deadline_ms)

    async def retry_same_request(self, operation, options, previous):
        # Preserve default deadline from the original evidence, not from this await.
        opts = dataclasses.replace(options) if options is not None else CallOptions()
        replay = getattr(previous, "_replay", None)
        if opts.deadline_ms is None and replay:
            opts.deadline_ms = replay[3]
        opts = self.sync.snapshot_options(opts)
        if opts.cancel is None:
            opts.cancel = CancellationToken()
        return await self._calls.run(
            self.sync.retry_same_request, operation, opts, previous, cancel=opts.cancel, _deadline_ms=opts.deadline_ms
        )

    async def events(self, operation="session.events.observe", options=None, **kwargs):
        opts = self.sync.snapshot_options(options, **kwargs)
        if opts.cancel is None:
            opts.cancel = CancellationToken()
        stream = await self._calls.run(
            self.sync.events,
            operation,
            opts,
            cancel=opts.cancel,
            _deadline_ms=opts.deadline_ms,
            _abandon=lambda value: value.close(),
        )
        result = AsyncEventStream(stream, self._streams, opts.cancel)
        self._streams_open.add(result)
        return result

    async def aclose(self, timeout=30):
        self._calls.check_loop()
        self._streams.check_loop()
        end = time.monotonic() + max(0, timeout)
        for stream in list(self._streams_open):
            await stream.aclose(timeout=max(0, end - time.monotonic()))
        self._streams_open.clear()
        # close(0) signals sockets first without blocking the host loop.
        if self._owns_sync:
            self.sync.close(timeout=0)
        calls = await self._calls.aclose(max(0, end - time.monotonic()))
        streams = await self._streams.aclose(max(0, end - time.monotonic()))
        return calls and streams and (not self._owns_sync or self.sync.close(timeout=0))

    async def __aenter__(self):
        return self

    async def __aexit__(self, *unused):
        await self.aclose()
