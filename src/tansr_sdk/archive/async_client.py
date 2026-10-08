"""同步协议引擎的 asyncio 门面；等待取消不伪装磁盘事务已经回滚。"""

import asyncio
import copy
import threading
import time
from typing import Any, Optional
from ..errors import Error
from ..lifecycle import AsyncBridge, CancellationToken
from .. import strict_json
from .client import ArchiveClient


class AsyncArchiveClient:
    def __init__(
        self,
        client: Any,
        bridge: Optional[AsyncBridge] = None,
        *,
        stream_workers: int = 2,
        max_stream_pending: int = 8,
        storage_workers: int = 2,
        max_storage_pending: int = 8,
    ) -> None:
        self.sync = client if isinstance(client, ArchiveClient) else ArchiveClient(client)
        self._bridge = bridge if bridge is not None else AsyncBridge()
        self._own_bridge = bridge is None
        self._streams_bridge = AsyncBridge(workers=stream_workers, max_pending=max_stream_pending)
        self._storage_bridge = AsyncBridge(workers=storage_workers, max_pending=max_storage_pending)
        self._lock = threading.Lock()
        self._tokens = set()  # type: set
        self._streams = set()  # type: set
        self._closed = False
        self._loop = None  # type: Optional[asyncio.AbstractEventLoop]
        self._readers = 0

    def _bind(self) -> None:
        self._bridge.check_loop()
        self._streams_bridge.check_loop()
        self._storage_bridge.check_loop()
        loop = asyncio.get_running_loop()
        if self._loop is None:
            self._loop = loop
        elif self._loop is not loop:
            raise Error("invalid_input", "archive client belongs to another loop")

    async def _invoke(self, name: str, *args: Any, **kwargs: Any) -> Any:
        self._bind()
        # 提交到有界队列之前固定 owned JSON 与绝对截止，排队不能延长写入寿命。
        args = tuple(strict_json.snapshot(value) if isinstance(value, (dict, list)) else value for value in args)
        kwargs = {
            key: strict_json.snapshot(value) if isinstance(value, (dict, list)) else value
            for key, value in kwargs.items()
        }
        options = kwargs.get("options")
        if options is not None:
            options = copy.copy(options)
            options.parameters = strict_json.snapshot(options.parameters)
            options.query = strict_json.snapshot(options.query)
            kwargs["options"] = options
        token = kwargs.get("cancel") or getattr(options, "cancel", None) or CancellationToken()
        kwargs["cancel"] = token
        if kwargs.get("deadline_ms") is None:
            kwargs["deadline_ms"] = getattr(options, "deadline_ms", None) or self.sync.default_deadline_ms()
        token.check(kwargs["deadline_ms"])
        ticket = object()
        with self._lock:
            if self._closed:
                raise Error("closed")
            self._tokens.add((ticket, token))
        started = [False]

        def call() -> Any:
            started[0] = True
            try:
                token.check(kwargs["deadline_ms"])
                if self._closed:
                    raise Error("closed")
                value = getattr(self.sync, name)(*args, **kwargs)
                if name == "events":
                    stream = AsyncArchiveEventStream(value, self, token, kwargs["deadline_ms"])
                    with self._lock:
                        self._streams.add(stream)
                    if token.cancelled or self._closed:
                        value.close()
                        token.check()
                        raise Error("closed")
                    return stream
                return value
            finally:
                with self._lock:
                    self._tokens.discard((ticket, token))

        try:
            if name == "events":
                bridge = self._streams_bridge
            elif name in ("prepare_materials", "prepare_materials_before", "sync_once", "recover_pending"):
                bridge = self._storage_bridge
            else:
                bridge = self._bridge
            return await bridge.run(
                call,
                cancel=token,
                _deadline_ms=kwargs["deadline_ms"],
                _abandon=(lambda stream: stream._close_now()) if name == "events" else None,
            )
        finally:
            if not started[0]:
                with self._lock:
                    self._tokens.discard((ticket, token))

    async def capabilities(self, **kwargs: Any) -> dict:
        return await self._invoke("capabilities", **kwargs)

    async def binding_target(self, session: str, **kwargs: Any) -> dict:
        return await self._invoke("binding_target", session, **kwargs)

    async def prepare_create(self, session: str, source: str, request_id: str, **kwargs: Any) -> dict:
        return await self._invoke("prepare_create", session, source, request_id, **kwargs)

    async def create_binding(self, intent: Any, **kwargs: Any) -> dict:
        return await self._invoke("create_binding", intent, **kwargs)

    async def close_binding(self, body: dict, **kwargs: Any) -> dict:
        return await self._invoke("close_binding", body, **kwargs)

    async def binding(self, identity: str, **kwargs: Any) -> dict:
        return await self._invoke("binding", identity, **kwargs)

    async def status(self, identity: str, **kwargs: Any) -> dict:
        return await self._invoke("status", identity, **kwargs)

    async def records(self, binding: dict, after: Optional[str] = None, **kwargs: Any) -> dict:
        return await self._invoke("records", binding, after, **kwargs)

    async def artifact(self, binding: dict, reference: dict, **kwargs: Any) -> bytes:
        return await self._invoke("artifact", binding, reference, **kwargs)

    async def acknowledge(self, ack: dict, **kwargs: Any) -> dict:
        return await self._invoke("acknowledge", ack, **kwargs)

    async def operation(self, binding: str, operation: str, request: dict, **kwargs: Any) -> dict:
        return await self._invoke("operation", binding, operation, request, **kwargs)

    async def creation_operation(self, session: str, request: dict, **kwargs: Any) -> dict:
        return await self._invoke("creation_operation", session, request, **kwargs)

    async def rebase_acknowledgement(self, body: dict, **kwargs: Any) -> dict:
        return await self._invoke("rebase_acknowledgement", body, **kwargs)

    async def events(self, binding: dict, cursor: Optional[str] = None, **kwargs: Any) -> Any:
        return await self._invoke("events", binding, cursor, **kwargs)

    async def upload_material_chunk(self, body: dict, **kwargs: Any) -> dict:
        return await self._invoke("upload_material_chunk", body, **kwargs)

    async def material_upload_status(self, binding: str, request: str, artifact: str, **kwargs: Any) -> dict:
        return await self._invoke("material_upload_status", binding, request, artifact, **kwargs)

    async def prepare_materials(self, store: Any, request: Any, identity: dict, **kwargs: Any) -> dict:
        return await self._invoke("prepare_materials", store, request, identity, **kwargs)

    async def prepare_materials_before(
        self, store: Any, request: dict, identity: dict, original_deadline_ms: int, **kwargs: Any
    ) -> dict:
        return await self._invoke("prepare_materials_before", store, request, identity, original_deadline_ms, **kwargs)

    async def submit_materials(self, intent: Any, **kwargs: Any) -> dict:
        return await self._invoke("submit_materials", intent, **kwargs)

    async def material_status(self, binding: str, request: str, **kwargs: Any) -> dict:
        return await self._invoke("material_status", binding, request, **kwargs)

    async def sync_once(self, store: Any, request_id: str, **kwargs: Any) -> Any:
        return await self._invoke("sync_once", store, request_id, **kwargs)

    async def recover_pending(self, store: Any, request_id: str, **kwargs: Any) -> Any:
        return await self._invoke("recover_pending", store, request_id, **kwargs)

    async def aclose(self, timeout: float = 30) -> bool:
        self._bind()
        end = time.monotonic() + max(0, timeout)
        with self._lock:
            self._closed = True
            tokens, streams = list(self._tokens), list(self._streams)
        for _, token in tokens:
            token.cancel()
        for stream in streams:
            stream._close_now()
        # 一份关闭预算覆盖三类自有资源；借用的控制桥始终留给宿主。
        control_closed = True
        if self._own_bridge:
            control_closed = await self._bridge.aclose(max(0, end - time.monotonic()))
        streams_closed = await self._streams_bridge.aclose(max(0, end - time.monotonic()))
        storage_closed = await self._storage_bridge.aclose(max(0, end - time.monotonic()))
        while True:
            with self._lock:
                if not self._tokens and not self._readers:
                    return control_closed and streams_closed and storage_closed
            if time.monotonic() >= end:
                return False
            await asyncio.sleep(0.01)

    async def __aenter__(self) -> "AsyncArchiveClient":
        self._bind()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.aclose()


class AsyncArchiveEventStream:
    def __init__(self, stream: Any, owner: AsyncArchiveClient, cancel: CancellationToken, deadline_ms: int) -> None:
        self._stream, self._owner, self._cancel = stream, owner, cancel
        self._reading = False
        self._closed = False
        self._deadline_ms = deadline_ms

    def __aiter__(self) -> "AsyncArchiveEventStream":
        return self

    async def __anext__(self) -> Any:
        self._owner._bind()
        if self._closed:
            raise StopAsyncIteration
        if self._reading:
            raise Error("reentrant")
        self._reading = True

        def read() -> Any:
            with self._owner._lock:
                self._owner._readers += 1
            try:
                self._cancel.check()
                try:
                    return True, next(self._stream)
                except StopIteration:
                    return False, None
            finally:
                with self._owner._lock:
                    self._owner._readers -= 1

        try:
            present, value = await self._owner._streams_bridge.run(
                read, cancel=self._cancel, _deadline_ms=self._deadline_ms
            )
            if not present:
                await self.aclose()
                raise StopAsyncIteration
            return value
        except BaseException:
            await self.aclose()
            raise
        finally:
            self._reading = False

    async def aclose(self) -> None:
        self._owner._bind()
        self._close_now()

    def _close_now(self) -> None:
        self._cancel.cancel()
        self._stream.close()
        self._closed = True
        with self._owner._lock:
            self._owner._streams.discard(self)

    async def __aenter__(self) -> "AsyncArchiveEventStream":
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.aclose()
