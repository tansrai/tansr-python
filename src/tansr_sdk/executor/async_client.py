"""异步门面复用同步执行状态机；桥取消不代表 handler 已静止。"""
import functools
import asyncio
import threading
import time
from typing import Any, Optional

from ..errors import Error
from ..lifecycle import AsyncBridge, CancellationToken
from .client import ExecutorClient
from .runner import Runner
from ._common import snapshot, expiry


class _AsyncOwner:
    def _setup(self, bridge: Optional[AsyncBridge], workers: int, capacity: int) -> None:
        self._owned = bridge is None
        self._bridge = bridge if bridge is not None else AsyncBridge(workers=workers, max_pending=capacity)
        self._lock = threading.Lock()
        self._pending = {}  # type: dict
        self._closed = False

    async def _run(self, function: Any, token: CancellationToken, deadline: Any) -> Any:
        self._bridge.check_loop()
        ticket = {"started": False, "abandoned": False, "cancel": token}
        key = object()
        with self._lock:
            if self._closed:
                raise Error("closed")
            self._pending[key] = ticket

        def invoke() -> Any:
            with self._lock:
                if ticket["abandoned"]:
                    raise Error("cancelled")
                ticket["started"] = True
            try:
                token.check(deadline)
                return function()
            finally:
                with self._lock:
                    self._pending.pop(key, None)
        try:
            return await self._bridge.run(invoke, cancel=token, _deadline_ms=deadline)
        finally:
            with self._lock:
                # await 在容量等待或尚未开始时退出：迟到的排队函数不得使用门面资源。
                if not ticket["started"]:
                    ticket["abandoned"] = True
                    self._pending.pop(key, None)

    async def _close_owner(self, timeout: float) -> bool:
        self._bridge.check_loop()
        with self._lock:
            self._closed = True
            tokens = [ticket["cancel"] for ticket in self._pending.values()]
        for token in tokens:
            token.cancel()
        if self._owned:
            return await self._bridge.aclose(timeout)
        end = time.monotonic() + max(0.0, timeout)
        while True:
            with self._lock:
                if not self._pending:
                    return True
            if time.monotonic() >= end:
                return False
            await asyncio.sleep(0.01)

    async def __aenter__(self) -> Any:
        self._bridge.check_loop()
        if self._closed:
            raise Error("closed")
        return self


class AsyncExecutorClient(_AsyncOwner):
    def __init__(self, client: Any, scope: Any = None, restricted: bool = False, *,
                 bridge: Optional[AsyncBridge] = None) -> None:
        self.sync = client if isinstance(client, ExecutorClient) else ExecutorClient(client, scope, restricted)
        self._setup(bridge, 4, 16)

    async def _call(self, name: str, *args: Any, **kwargs: Any) -> Any:
        token = kwargs.pop("cancel", None) or CancellationToken()
        prepared_args = tuple(snapshot(value) for value in args)
        prepared = {key: snapshot(value) for key, value in kwargs.items()}
        deadline = prepared.get("deadline_ms")
        if deadline is None:
            deadline = self.sync.api.default_deadline_ms()
        prepared.update(cancel=token, deadline_ms=deadline)
        return await self._run(functools.partial(getattr(self.sync, name), *prepared_args, **prepared), token, deadline)

    async def register(self, registration: Any, **kwargs: Any) -> Any:
        return await self._call("register", registration, **kwargs)

    register_executor = register

    async def heartbeat(self, connection: Any, **kwargs: Any) -> Any:
        return await self._call("heartbeat", connection, **kwargs)

    async def poll(self, connection: Any, **kwargs: Any) -> Any:
        return await self._call("poll", connection, **kwargs)

    async def initialize(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call("initialize", *args, **kwargs)

    async def capabilities(self, session_id: str, **kwargs: Any) -> Any:
        return await self._call("capabilities", session_id, **kwargs)

    execution_capabilities = capabilities

    async def bind(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call("bind", *args, **kwargs)

    async def negotiate_output(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call("negotiate_output", *args, **kwargs)

    async def status(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call("status", *args, **kwargs)

    async def executor_status(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call("executor_status", *args, **kwargs)

    async def submit(self, *args: Any, **kwargs: Any) -> Any:
        return await self._call("submit", *args, **kwargs)

    async def aclose(self, timeout: float = 30.0) -> bool:
        return await self._close_owner(timeout)

    async def __aexit__(self, *args: Any) -> None:
        if not await self.aclose():
            raise Error("not_quiescent")


class AsyncRunner(_AsyncOwner):
    def __init__(self, runner: Runner, *, bridge: Optional[AsyncBridge] = None) -> None:
        self.sync = runner
        self._setup(bridge, 2, 2)

    async def _call(self, name: str, *args: Any, **kwargs: Any) -> Any:
        token = kwargs.pop("cancel", None) or CancellationToken()
        prepared_args = tuple(snapshot(value) for value in args)
        prepared = {key: snapshot(value) for key, value in kwargs.items()}
        deadline = prepared.pop("deadline_ms", None)
        if name != "run":
            if deadline is None:
                deadline = self.sync.client.api.default_deadline_ms()
            if prepared_args:
                deadline = min(deadline, expiry(prepared_args[0]["expiresAt"]))
            prepared["deadline_ms"] = deadline
        prepared["cancel"] = token
        return await self._run(functools.partial(getattr(self.sync, name), *prepared_args, **prepared), token, deadline)

    async def connect(self, **kwargs: Any) -> Any:
        return await self._call("connect", **kwargs)

    async def run(self, **kwargs: Any) -> None:
        await self._call("run", **kwargs)

    async def execute(self, operation: Any, **kwargs: Any) -> Any:
        return await self._call("execute", operation, **kwargs)

    async def execute_with_output(self, operation: Any, **kwargs: Any) -> Any:
        return await self._call("execute_with_output", operation, **kwargs)

    async def aclose(self, timeout: float = 30.0) -> bool:
        # 不在 loop 中等待同步 handler；先发合作取消，再由桥等待其真实结束。
        self._bridge.check_loop()
        end = time.monotonic() + max(0.0, timeout)
        self.sync.close(timeout=0)
        if not await self._close_owner(timeout):
            return False
        while not self.sync.close(timeout=0):
            if time.monotonic() >= end:
                return False
            await asyncio.sleep(0.01)
        return True

    async def __aexit__(self, *args: Any) -> None:
        if not await self.aclose():
            raise Error("not_quiescent")
