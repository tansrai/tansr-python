"""显式 stdout/stderr 原字节上传；捕获、ACK、封口分别记录。"""
import base64
import contextvars
import hashlib
import threading
import time
from typing import Any, Dict, Optional

from .. import canonical
from ..errors import Error
from ..lifecycle import CancellationToken
from ._common import TERMINAL, cancellation, equal, snapshot, validate


def _sequence(value: Any) -> int:
    if value is None:
        return -1
    if (not isinstance(value, str) or not value.isascii() or not value.isdigit() or
            len(value) > 1 and value[0] == "0" or int(value) > 9223372036854775807):
        raise Error("invalid_response", "output sequence")
    return int(value)


def validate_output_status(value: Dict[str, Any]) -> None:
    validate("OutputStatus", value, TERMINAL)
    ack, durable, retained, offset = (_sequence(value[key]) for key in
                                      ("acceptedThrough", "durableThrough", "retainedFrom", "nextByteOffset"))
    seal, state = value["seal"], value["state"]
    if durable > ack or retained > ack:
        raise Error("output_integrity", "impossible output watermark")
    if offset == -1:
        if state != "unavailable" or (ack, durable, retained) != (-1, -1, -1) or seal is not None:
            raise Error("output_integrity", "missing output offset")
    elif (ack == -1 and offset != 0) or (ack != -1 and offset <= ack):
        raise Error("output_integrity", "output offset")
    if seal is not None:
        if _sequence(seal["lastSeq"]) != ack or seal["totalBytes"] != value["nextByteOffset"]:
            raise Error("output_integrity", "seal watermark")
        if ack == -1 and (seal["totalBytes"] != "0" or seal["payloadDigest"] != hashlib.sha256(b"").hexdigest()):
            raise Error("output_integrity", "empty seal")
    valid = ((state == "complete" and seal is not None and not seal["truncated"]) or
             (state == "truncated" and seal is not None and seal["truncated"]) or
             (state == "available" and ack == -1 and seal is None) or
             (state == "receiving" and ack != -1 and seal is None) or
             (state == "gap" and ack != -1) or state == "unavailable")
    if not valid:
        raise Error("output_integrity", "output state")


def query_output(client: Any, session: Dict[str, Any], operation: Dict[str, Any], *,
                 max_control_bytes: int = 262144, cancel: Optional[CancellationToken] = None,
                 deadline_ms: Optional[int] = None) -> Dict[str, Any]:
    session, operation = snapshot(session), snapshot(operation)
    validate("SessionReference", session, TERMINAL)
    validate("OperationReference", operation, TERMINAL)
    response = client.call("terminal.output.status", parameters={"id": session["sessionId"]},
                           query={"contract": TERMINAL, "sessionContract": session["sessionContract"],
                                  "operationId": operation["operationId"], "requestDigest": operation["requestDigest"]},
                           max_response_bytes=int(max_control_bytes), cancel=cancel,
                           deadline_ms=deadline_ms if deadline_ms is not None else client.default_deadline_ms())
    if response.status != 200:
        raise Error("invalid_response", "output status HTTP")
    result = snapshot(response.body)
    validate_output_status(result)
    if not equal(result["operation"], operation):
        raise Error("output_integrity", "output operation identity")
    return result


class _Pipe:
    def __init__(self, writer: "OutputWriter", channel: str) -> None:
        self._writer, self._channel = writer, channel

    def write(self, data: bytes) -> int:
        self._writer.capture(self._channel, data)
        # 必须继续排空生产源；返回值不冒充远端 ACK。
        return len(data)

    def flush(self) -> None:
        return None


class OutputWriter:
    def __init__(self, client: Any, session: Dict[str, Any], operation: Dict[str, Any],
                 executor_id: str, connection_id: str, limits: Dict[str, Any], *,
                 encoding: str = "binary", cancel: Optional[CancellationToken] = None,
                 deadline_ms: Optional[int] = None) -> None:
        for definition, value in (("SessionReference", session), ("OperationReference", operation),
                                  ("Id", executor_id), ("Id", connection_id), ("Limits", limits)):
            validate(definition, value, TERMINAL)
        if encoding not in ("binary", "utf-8"):
            raise Error("invalid_request", "output encoding")
        self.client, self.session, self.operation = client, snapshot(session), snapshot(operation)
        # 上传沿输出所属操作的宿主身份；后续生产者线程不替换该上下文。
        self._owner_context = contextvars.copy_context()
        self.executor_id, self.connection_id = executor_id, connection_id
        self.limits, self.encoding = snapshot(limits), encoding
        self._deadline = deadline_ms
        self._cancel = CancellationToken()
        self._unlink = cancellation(cancel).register(self._cancel.cancel)
        self._changed = threading.Condition(threading.RLock())
        self._pending = []  # type: list
        self._pending_bytes = self._inflight_bytes = 0
        self._next = self._offset = self._ack_offset = self._dropped = 0
        self._ack = self._sent = -1
        self._hash = hashlib.sha256()
        self._truncated = self._sealed = self._closed = False
        self._seal = self._status = self._error = None  # type: Any
        self._worker = None  # type: Any
        self._io_lock = threading.Lock()
        self.stdout, self.stderr = _Pipe(self, "stdout"), _Pipe(self, "stderr")

    def _body(self, blocks: list, seal: Any) -> Dict[str, Any]:
        return {"contract": TERMINAL, "session": self.session, "operation": self.operation,
                "executorId": self.executor_id, "connectionId": self.connection_id,
                "blocks": blocks, "seal": seal}

    def _drop(self, count: int) -> None:
        if count:
            self._truncated = True
            self._dropped += count

    def capture(self, channel: str, data: bytes) -> int:
        if channel not in ("stdout", "stderr") or not isinstance(data, bytes):
            raise Error("invalid_request", "output requires channel and bytes")
        with self._changed:
            if self._seal is not None or self._closed:
                raise Error("output_closed")
            if self._truncated or self._error is not None or self._cancel.cancelled:
                self._drop(len(data))
                return 0
            accepted = 0
            maximum = min(self.limits["maxBlockBytes"], self.limits["maxBatchBytes"])
            while accepted < len(data):
                count = min(maximum, len(data) - accepted)
                if (self._offset + count > min(9223372036854775807, self.limits["maxRetainedBytes"]) or
                        self._next >= 9223372036854775807):
                    self._drop(len(data) - accepted)
                    break
                piece = data[accepted:accepted + count]
                block = {"seq": str(self._next), "byteOffset": str(self._offset), "channel": channel,
                         "encoding": self.encoding, "byteLength": count,
                         "payloadDigest": hashlib.sha256(piece).hexdigest(),
                         "base64": base64.b64encode(piece).decode("ascii")}
                cost = len(canonical.encode(block))
                envelope = len(canonical.encode(self._body([], None))) + 2
                # 预留一份编码发送体，pending 中含正在发送的块；inflight 不在 ACK 前释放。
                reserved = max(2 * (self._pending_bytes + cost) + envelope,
                               self._pending_bytes + cost + self._inflight_bytes)
                if reserved > self.limits["maxPendingBytes"]:
                    self._drop(len(data) - accepted)
                    break
                if len(canonical.encode(self._body([block], None))) > self.limits["maxControlBytes"]:
                    self._error = Error("payload_too_large", "output control budget")
                    self._drop(len(data) - accepted)
                    break
                self._pending.append((block, cost))
                self._pending_bytes += cost
                self._hash.update(piece)
                self._next += 1
                self._offset += count
                accepted += count
            self._start()
            self._changed.notify_all()
            return accepted

    def snapshot(self) -> Dict[str, Any]:
        with self._changed:
            return {"pendingBytes": self._pending_bytes + self._inflight_bytes,
                    "pendingBlocks": len(self._pending), "inflightBytes": self._inflight_bytes,
                    "capturedBytes": str(self._offset), "droppedBytes": str(self._dropped),
                    "truncated": self._truncated, "sealed": self._sealed,
                    "failed": self._error is not None or self._cancel.cancelled,
                    "status": snapshot(self._status), "seal": snapshot(self._seal)}

    def _start(self) -> None:
        if (self._worker is not None and self._worker.is_alive() or self._sealed or self._closed or
                self._error is not None or self._cancel.cancelled or not self._pending and self._seal is None):
            return
        self._worker = threading.Thread(target=self._owner_context.copy().run, args=(self._pump,), name="tansr-output")
        self._worker.start()

    def _next_batch(self) -> Any:
        with self._changed:
            if not self._pending and (self._seal is None or self._sealed):
                return None
            blocks = []  # type: list
            raw_count = 0
            for block, _ in self._pending:
                if len(blocks) == 32 or raw_count + block["byteLength"] > self.limits["maxBatchBytes"]:
                    break
                candidate = blocks + [block]
                if len(canonical.encode(self._body(candidate, None))) > self.limits["maxControlBytes"]:
                    break
                blocks = candidate
                raw_count += block["byteLength"]
            if self._pending and not blocks:
                raise Error("payload_too_large", "output block cannot fit")
            body = snapshot(self._body(blocks, None if blocks else self._seal))
            encoded_size = len(canonical.encode(body))
            if (encoded_size > self.limits["maxControlBytes"] or
                    self._pending_bytes + encoded_size > self.limits["maxPendingBytes"]):
                raise Error("payload_too_large", "output encoded budget")
            if blocks:
                self._sent = max(self._sent, int(blocks[-1]["seq"]))
            self._inflight_bytes = encoded_size
            return body, self._sent

    def _request_deadline(self) -> int:
        default = self.client.default_deadline_ms()
        return min(default, self._deadline) if self._deadline is not None else default

    def _query(self, cancel: Optional[CancellationToken] = None) -> Dict[str, Any]:
        token = CancellationToken()
        unlink = self._cancel.register(token.cancel)
        unlink_caller = cancellation(cancel).register(token.cancel)
        try:
            result = query_output(self.client, self.session, self.operation,
                                  max_control_bytes=self.limits["maxControlBytes"], cancel=token,
                                  deadline_ms=self._request_deadline())
            token.check(self._deadline)
            self._accept(result)
            return result
        finally:
            unlink()
            unlink_caller()

    def _acknowledged(self, expected: int, seal: bool) -> bool:
        with self._changed:
            return self._ack >= expected and (not seal or self._sealed)

    def _pump(self) -> None:
        try:
            with self._io_lock:
                while True:
                    self._cancel.check(self._deadline)
                    batch = self._next_batch()
                    if batch is None:
                        return
                    body, expected = batch
                    validate("OutputBatchRequest", body, TERMINAL)
                    last = None  # type: Any
                    for _ in range(2):
                        self._cancel.check(self._deadline)
                        try:
                            result = self.client.call("terminal.output.batch", parameters={"id": self.executor_id},
                                                      body=body, cancel=self._cancel,
                                                      deadline_ms=self._request_deadline(),
                                                      max_response_bytes=int(self.limits["maxControlBytes"]))
                            if result.status != 200:
                                raise Error("invalid_response", "output batch status")
                            validate_output_status(result.body)
                            self._accept(result.body)
                            if self._acknowledged(expected, body["seal"] is not None):
                                last = None
                                break
                            last = Error("output_unknown")
                        except Error as error:
                            if error.code in ("forbidden", "unauthorized", "cancelled", "timeout",
                                              "output_gap", "output_integrity", "invalid_response"):
                                raise
                            last = error
                        # 仅查原输出水位，最多重发同一块/封口；从不重跑业务。
                        self._query()
                        if self._acknowledged(expected, body["seal"] is not None):
                            last = None
                            break
                    if last is not None:
                        raise last
                    with self._changed:
                        self._inflight_bytes = 0
                        self._changed.notify_all()
        except BaseException as error:
            with self._changed:
                self._error = error if isinstance(error, Error) else Error("output_unknown")
                self._truncated = True
        finally:
            with self._changed:
                self._inflight_bytes = 0
                self._worker = None
                self._changed.notify_all()
                # 捕获与空队列退出竞争时，保留后续块的上传责任。
                self._start()

    def _accept(self, status: Dict[str, Any]) -> None:
        validate_output_status(status)
        with self._changed:
            self._cancel.check(self._deadline)
            if not equal(status["operation"], self.operation):
                raise Error("output_integrity", "output operation")
            if status["state"] in ("unavailable", "gap") or status["nextByteOffset"] is None:
                raise Error("output_gap")
            ack = _sequence(status["acceptedThrough"])
            if ack < self._ack or ack > self._sent:
                raise Error("output_integrity", "output ACK beyond sent prefix")
            offset = self._ack_offset
            if ack != self._ack:
                match = next((block for block, _ in self._pending if int(block["seq"]) == ack), None)
                if match is None:
                    raise Error("output_integrity", "unknown output ACK")
                offset = int(match["byteOffset"]) + match["byteLength"]
            if str(offset) != status["nextByteOffset"] or status["seal"] is not None and not equal(status["seal"], self._seal):
                raise Error("output_integrity", "output seal or offset")
            while self._pending and int(self._pending[0][0]["seq"]) <= ack:
                _, cost = self._pending.pop(0)
                self._pending_bytes -= cost
            self._ack, self._ack_offset = ack, offset
            self._status = snapshot(status)
            self._sealed = status["seal"] is not None
            self._changed.notify_all()

    def reconcile(self, cancel: Optional[CancellationToken] = None) -> Dict[str, Any]:
        token = cancellation(cancel)
        while not self._io_lock.acquire(timeout=0.05):
            token.check(self._deadline)
            self._cancel.check(self._deadline)
        try:
            token.check(self._deadline)
            self._cancel.check(self._deadline)
            result = self._query(token)
            with self._changed:
                self._error = None
            return result
        finally:
            self._io_lock.release()
            with self._changed:
                self._start()

    def finish(self, truncated: bool = False, *, cancel: Optional[CancellationToken] = None) -> Dict[str, Any]:
        token = cancellation(cancel)
        with self._changed:
            if self._seal is None:
                self._truncated = self._truncated or truncated
                self._seal = {"lastSeq": str(self._next - 1) if self._next else None,
                              "totalBytes": str(self._offset), "payloadDigest": self._hash.hexdigest(),
                              "truncated": self._truncated}
            while True:
                try:
                    token.check(self._deadline)
                    self._cancel.check(self._deadline)
                except Error:
                    self.abort()
                    raise
                if self._error is not None:
                    raise self._error
                if self._sealed:
                    return snapshot(self._status)
                self._start()
                self._changed.wait(0.05)

    def abort(self) -> None:
        self._cancel.cancel()
        with self._changed:
            if not self._sealed:
                self._truncated = True
            self._changed.notify_all()

    def close(self, timeout: float = 30.0) -> bool:
        end = time.monotonic() + max(0.0, timeout)
        self.abort()
        with self._changed:
            self._closed = True
            worker = self._worker
        if worker is threading.current_thread():
            raise Error("reentrant_call")
        if worker is not None:
            worker.join(max(0.0, end - time.monotonic()))
            if worker.is_alive():
                return False
        # 显式 reconcile 也是本 writer 的在途 I/O，不能只等待后台 pump。
        if not self._io_lock.acquire(timeout=max(0.0, end - time.monotonic())):
            return False
        self._io_lock.release()
        self._unlink()
        return True

    def __enter__(self) -> "OutputWriter":
        return self

    def __exit__(self, *args: Any) -> None:
        if not self.close():
            raise Error("not_quiescent")
