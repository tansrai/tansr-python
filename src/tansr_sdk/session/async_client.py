"""有界asyncio门面：控制与长流分池，所有业务语义调用同步实现。"""

from dataclasses import fields, is_dataclass, replace
from typing import Any, Optional, Set

from ..errors import Error
from ..lifecycle import AsyncBridge, CancellationToken
from ..strict_json import snapshot
from ._validation import linked_cancel
from .client import Session, SessionClient
from .types import CreateOptions, WriteOptions


def _capture(value: Any) -> Any:
    """排队前冻结请求，token保留同一个可取消信号，不复制其锁。"""
    if isinstance(value, CancellationToken):
        return value
    if is_dataclass(value) and not isinstance(value, type):
        return type(value)(**{field.name: _capture(getattr(value, field.name)) for field in fields(value)})
    if isinstance(value, tuple):
        return tuple(_capture(item) for item in value)
    if isinstance(value, list):
        return [_capture(item) for item in value]
    if isinstance(value, dict):
        return snapshot(value)
    return value


class AsyncSessionClient:
    """借用同步Client/SessionClient，aclose只收回自己调度和观察资源。"""

    def __init__(
        self,
        client: Any,
        *,
        workers: int = 4,
        max_pending: int = 16,
        stream_workers: int = 2,
        max_stream_pending: int = 8,
    ) -> None:
        self.sync = client if isinstance(client, SessionClient) else SessionClient(client)
        self._control = AsyncBridge(workers=workers, max_pending=max_pending)
        self._streams_bridge = AsyncBridge(workers=stream_workers, max_pending=max_stream_pending)
        self._streams: Set["AsyncSessionEventStream"] = set()
        self._closed = False

    @property
    def api(self) -> Any:
        return self.sync.api

    def _check_open(self) -> None:
        self._control.check_loop()
        self._streams_bridge.check_loop()
        if self._closed:
            raise Error("closed", "async session client is closed")

    async def _read(self, method: Any, *args: Any, **kwargs: Any) -> Any:
        self._check_open()
        args = _capture(args)
        kwargs = {key: _capture(value) for key, value in kwargs.items()}
        if kwargs.get("deadline_ms") is None:
            kwargs["deadline_ms"] = self.api.default_deadline_ms()
        original_cancel = kwargs.pop("cancel", None)
        with linked_cancel(original_cancel) as cancel:
            kwargs["cancel"] = cancel
            return await self._control.run(
                lambda: method(*args, **kwargs), cancel=cancel, _deadline_ms=kwargs["deadline_ms"]
            )

    async def _write(self, method: Any, *args: Any, **kwargs: Any) -> Any:
        self._check_open()
        args = _capture(args)
        kwargs = {key: _capture(value) for key, value in kwargs.items()}
        write = kwargs.pop("write", None) or WriteOptions()
        if write.deadline_ms is None:
            write = replace(write, deadline_ms=self.api.default_deadline_ms())
        with linked_cancel(write.cancel) as cancel:
            kwargs["write"] = replace(write, cancel=cancel)
            return await self._control.run(
                lambda: method(*args, **kwargs), cancel=cancel, _deadline_ms=write.deadline_ms
            )

    async def create(self, options: Optional[CreateOptions] = None, **fields: Any) -> "AsyncSession":
        self._check_open()
        if options is not None and fields:
            from ._validation import invalid

            raise invalid("pass CreateOptions or keyword fields, not both")
        value = _capture(options if options is not None else CreateOptions(**fields))
        if value.write.deadline_ms is None:
            value.write = replace(value.write, deadline_ms=self.api.default_deadline_ms())
        with linked_cancel(value.write.cancel) as cancel:
            value.write = replace(value.write, cancel=cancel)
            session = await self._control.run(
                lambda: self.sync.create(value), cancel=cancel, _deadline_ms=value.write.deadline_ms
            )
            return AsyncSession(self, session)

    async def attach(
        self, session_id: str, *, cancel: Optional[CancellationToken] = None, deadline_ms: Optional[int] = None
    ) -> "AsyncSession":
        session = await self._read(self.sync.attach, session_id, cancel=cancel, deadline_ms=deadline_ms)
        return AsyncSession(self, session)

    async def resume(self, session_id: str, write: Optional[WriteOptions] = None) -> "AsyncSession":
        session = await self._write(self.sync.resume, session_id, write=write)
        return AsyncSession(self, session)

    async def list(
        self,
        offset: int = 0,
        limit: int = 100,
        *,
        cancel: Optional[CancellationToken] = None,
        deadline_ms: Optional[int] = None,
    ) -> Any:
        return await self._read(self.sync.list, offset, limit, cancel=cancel, deadline_ms=deadline_ms)

    async def aclose(self, timeout: float = 30) -> bool:
        import time

        self._control.check_loop()
        self._streams_bridge.check_loop()
        self._closed = True
        end = time.monotonic() + max(0, timeout)
        # 先取消真实阻塞读，不能把close排在两个被长流占满的worker之后。
        for stream in list(self._streams):
            await stream.aclose()
        control = await self._control.aclose(max(0, end - time.monotonic()))
        streams = await self._streams_bridge.aclose(max(0, end - time.monotonic()))
        return control and streams

    async def __aenter__(self) -> "AsyncSessionClient":
        self._check_open()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        await self.aclose()


class AsyncSession:
    """异步Session句柄；close显式关闭远端，client.aclose只清本地。"""

    def __init__(self, client: AsyncSessionClient, sync: Session) -> None:
        self.client = client
        self.sync = sync

    @property
    def id(self) -> str:
        return self.sync.id

    @property
    def created(self) -> Any:
        return self.sync.created

    @property
    def api(self) -> Any:
        return self.sync.api

    async def capabilities(self, **options: Any) -> Any:
        return await self.client._read(self.sync.capabilities, **options)

    async def meta(self, **options: Any) -> Any:
        return await self.client._read(self.sync.meta, **options)

    async def application_prompt_meta(self, **options: Any) -> Any:
        return await self.client._read(self.sync.application_prompt_meta, **options)

    async def send(self, prompt: str, write: Optional[WriteOptions] = None) -> Any:
        return await self.client._write(self.sync.send, prompt, write=write)

    async def send_blocks(self, blocks: list, write: Optional[WriteOptions] = None) -> Any:
        return await self.client._write(self.sync.send_blocks, blocks, write=write)

    async def interrupt(self, write: Optional[WriteOptions] = None) -> Any:
        return await self.client._write(self.sync.interrupt, write=write)

    async def close(self, write: Optional[WriteOptions] = None) -> Any:
        return await self.client._write(self.sync.close, write=write)

    async def permission(self, ticket: str, digest: str, verdict: str, write: Optional[WriteOptions] = None) -> Any:
        return await self.client._write(self.sync.permission, ticket, digest, verdict, write=write)

    async def answer(self, ticket: str, answers: list, write: Optional[WriteOptions] = None) -> Any:
        return await self.client._write(self.sync.answer, ticket, answers, write=write)

    async def tool_result(self, call_id: str, receipt: dict, write: Optional[WriteOptions] = None) -> Any:
        return await self.client._write(self.sync.tool_result, call_id, receipt, write=write)

    async def history(self, offset: int = 0, limit: int = 100, **options: Any) -> Any:
        return await self.client._read(self.sync.history, offset, limit, **options)

    async def input_capabilities(self, **options: Any) -> Any:
        return await self.client._read(self.sync.input_capabilities, **options)

    async def submit_input(self, value: Any, write: Optional[WriteOptions] = None) -> Any:
        return await self.client._write(self.sync.submit_input, value, write=write)

    async def input_status(self, input_id: str, target: Any, **options: Any) -> Any:
        return await self.client._read(self.sync.input_status, input_id, target, **options)

    async def compact(self, options: Any = None, write: Optional[WriteOptions] = None) -> Any:
        return await self.client._write(self.sync.compact, options, write=write)

    async def checkpoints(self, **options: Any) -> Any:
        return await self.client._read(self.sync.checkpoints, **options)

    async def checkpoint(self, label: str = "", write: Optional[WriteOptions] = None) -> Any:
        return await self.client._write(self.sync.checkpoint, label, write=write)

    async def restore(self, checkpoint_id: str, checkpoint: bool = True, write: Optional[WriteOptions] = None) -> Any:
        return await self.client._write(self.sync.restore, checkpoint_id, checkpoint, write=write)

    async def delete_checkpoint(self, checkpoint_id: str, write: Optional[WriteOptions] = None) -> None:
        await self.client._write(self.sync.delete_checkpoint, checkpoint_id, write=write)

    async def export_checkpoint(self, checkpoint_id: str, **options: Any) -> bytes:
        return await self.client._read(self.sync.export_checkpoint, checkpoint_id, **options)

    async def import_checkpoint(self, data: bytes, label: str = "", write: Optional[WriteOptions] = None) -> Any:
        return await self.client._write(self.sync.import_checkpoint, data, label, write=write)

    async def set_cwd(self, cwd: str, write: Optional[WriteOptions] = None) -> Any:
        return await self.client._write(self.sync.set_cwd, cwd, write=write)

    async def transcribe(self, request: Any, write: Optional[WriteOptions] = None) -> Any:
        return await self.client._write(self.sync.transcribe, request, write=write)

    async def speak(self, request: Any, write: Optional[WriteOptions] = None) -> Any:
        return await self.client._write(self.sync.speak, request, write=write)

    async def events(
        self,
        last_event_id: Optional[str] = None,
        *,
        cancel: Optional[CancellationToken] = None,
        deadline_ms: Optional[int] = None,
    ) -> "AsyncSessionEventStream":
        self.client._check_open()
        if deadline_ms is None:
            deadline_ms = self.api.default_deadline_ms()
        local = CancellationToken()
        remove = cancel.register(local.cancel) if cancel is not None else lambda: None
        sync = None
        try:
            sync = await self.client._control.run(
                lambda: self.sync.events(last_event_id, cancel=local, deadline_ms=deadline_ms),
                cancel=local,
                _deadline_ms=deadline_ms,
                _abandon=lambda stream: stream.close(),
            )
            if self.client._closed:
                raise Error("closed", "async session client closed while opening stream")
            local.check(deadline_ms)
            result = AsyncSessionEventStream(self.client, sync, local, remove, deadline_ms)
            self.client._streams.add(result)
            return result
        except BaseException:
            local.cancel()
            if sync is not None:
                sync.close()
            remove()
            raise


class AsyncSessionEventStream:
    def __init__(
        self, client: AsyncSessionClient, sync: Any, cancel: CancellationToken, remove: Any, deadline_ms: Optional[int]
    ) -> None:
        self.client = client
        self.sync = sync
        self._cancel = cancel
        self._remove = remove
        self._closed = False
        self._reading = False
        self._deadline_ms = deadline_ms

    @property
    def last_event_id(self) -> Optional[str]:
        return self.sync.last_event_id

    def __aiter__(self) -> "AsyncSessionEventStream":
        return self

    async def __anext__(self) -> Any:
        self.client._streams_bridge.check_loop()
        if self._closed:
            raise StopAsyncIteration
        if self._reading:
            raise Error("reentrant", "session event stream has one consumer")
        self._reading = True
        try:
            event = await self.client._streams_bridge.run(
                self.sync.next, cancel=self._cancel, _deadline_ms=self._deadline_ms
            )
            if event is None:
                await self.aclose()
                raise StopAsyncIteration
            return event
        except BaseException:
            await self.aclose()
            raise
        finally:
            self._reading = False

    async def aclose(self) -> None:
        self.client._control.check_loop()
        self.client._streams_bridge.check_loop()
        if self._closed:
            return
        self._closed = True
        self._cancel.cancel()
        try:
            self.sync.close()
        finally:
            self._remove()
            self.client._streams.discard(self)

    async def __aenter__(self) -> "AsyncSessionEventStream":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        await self.aclose()
