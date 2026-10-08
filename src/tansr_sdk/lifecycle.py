"""取消是合作信号；它不证明已受理写入或业务副作用未发生。"""

import threading
import time
import asyncio
import contextvars
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Optional
from .errors import Error


def now_ms() -> int:
    return int(time.time() * 1000)


class CancellationToken:
    def __init__(self) -> None:
        self._event = threading.Event()
        self._lock = threading.Lock()
        self._callbacks = {}  # type: dict

    @property
    def cancelled(self) -> bool:
        return self._event.is_set()

    def cancel(self) -> None:
        with self._lock:
            if self._event.is_set():
                return
            self._event.set()
            callbacks = list(self._callbacks.values())
            self._callbacks.clear()
        for callback in callbacks:
            try:
                callback()
            except Exception:
                pass  # 清理其它资源仍须继续。

    def check(self, deadline_ms: Optional[int] = None) -> None:
        if self.cancelled:
            raise Error("cancelled")
        if deadline_ms is not None and now_ms() >= deadline_ms:
            raise Error("timeout")

    def register(self, callback: Callable[[], None]) -> Callable[[], None]:
        key = object()
        with self._lock:
            ready = self._event.is_set()
            if not ready:
                self._callbacks[key] = callback
        if ready:
            callback()

        def unregister() -> None:
            with self._lock:
                self._callbacks.pop(key, None)

        return unregister

    def wait(self, seconds: float) -> bool:
        return self._event.wait(seconds)


def remaining_seconds(deadline_ms: int, cancel: CancellationToken) -> float:
    cancel.check(deadline_ms)
    return max(0.001, (deadline_ms - now_ms()) / 1000.0)


class AsyncBridge:
    """有界私有执行池，异步门面复用同一同步协议状态机。

    cancel 仅控制桥的合作取消信号，调用方须把同一个 token 传给 fn。
    取消 await 不会将仍运行的函数从待清理集合移除。
    """

    def __init__(self, workers=4, max_pending=16):
        if not isinstance(workers, int) or isinstance(workers, bool) or workers < 1:
            raise Error("invalid_input", "invalid bridge worker count")
        if not isinstance(max_pending, int) or max_pending < workers:
            raise Error("invalid_input", "invalid bridge capacity")
        self._executor = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="tansr")
        self._capacity = max_pending
        self._loop = None
        self._lock = threading.Lock()
        self._pending = {}
        self._closed = False
        self._worker_context = threading.local()

    def _bind_loop(self):
        loop = asyncio.get_running_loop()
        with self._lock:
            if self._loop is None:
                self._loop = loop
            elif self._loop is not loop:
                raise Error("invalid_input", "async instance belongs to another event loop")
        return loop

    def check_loop(self):
        """验证所属事件循环；应在异步资源门面修改状态之前调用。"""
        return self._bind_loop()

    async def run(self, fn, *args, cancel=None, _deadline_ms=None, _abandon=None, **kwargs):
        loop = self._bind_loop()
        # 每个请求独立捕获宿主身份/追踪上下文；不能继承复用worker的上次状态。
        context = contextvars.copy_context()
        token = cancel if cancel is not None else CancellationToken()
        ticket = object()
        try:
            while True:
                token.check(_deadline_ms)
                with self._lock:
                    if self._closed:
                        raise Error("closed")
                    if len(self._pending) < self._capacity:
                        self._pending[ticket] = (token, None)
                        break
                await asyncio.sleep(0.01)
            try:

                def invoke():
                    self._worker_context.active = True
                    try:
                        return context.run(fn, *args, **kwargs)
                    finally:
                        self._worker_context.active = False

                future = self._executor.submit(invoke)
            except BaseException:
                with self._lock:
                    self._pending.pop(ticket, None)
                raise
            with self._lock:
                self._pending[ticket] = (token, future)

            state = {"done": False, "delivered": False, "abandon": False, "settling": False}

            def settle():
                with self._lock:
                    if not state["done"] or not state["delivered"] or state["settling"]:
                        return
                    state["settling"] = True
                    abandoned = state["abandon"]
                try:
                    if abandoned and _abandon is not None and not future.cancelled() and future.exception() is None:
                        context.run(_abandon, future.result())
                except Exception:
                    pass  # Cleanup does not replace the original cancellation or timeout.
                finally:
                    with self._lock:
                        self._pending.pop(ticket, None)

            def finished(unused):
                with self._lock:
                    state["done"] = True
                settle()

            future.add_done_callback(finished)
            wrapped = asyncio.wrap_future(future, loop=loop)

            def consume(done):
                if not done.cancelled():
                    done.exception()

            # wait 不向实际worker传播await取消，也不依赖3.14 shield的异常上报回调。
            # worker的真正完成与废弃资源回收仍由future/settle独立负责。
            try:
                timeout = None if _deadline_ms is None else max(0, (_deadline_ms - now_ms()) / 1000.0)
                completed, _ = await asyncio.wait({wrapped}, timeout=timeout)
                if not completed:
                    raise asyncio.TimeoutError()
                return wrapped.result()
            except asyncio.TimeoutError:
                token.cancel()
                state["abandon"] = True
                future.cancel()
                wrapped.add_done_callback(consume)
                raise Error("timeout") from None
            except asyncio.CancelledError:
                token.cancel()
                state["abandon"] = True
                future.cancel()  # Only removes work that has not started.

                wrapped.add_done_callback(consume)
                raise
            finally:
                with self._lock:
                    state["delivered"] = True
                settle()
        except asyncio.CancelledError:
            token.cancel()
            raise

    async def aclose(self, timeout=30):
        self._bind_loop()
        self._stop()
        end = time.monotonic() + max(0, timeout)
        while True:
            with self._lock:
                if not self._pending:
                    return True
            if time.monotonic() >= end:
                return False
            await asyncio.sleep(min(0.01, max(0, end - time.monotonic())))

    def _stop(self):
        with self._lock:
            self._closed = True
            pending = list(self._pending.values())
        for token, future in pending:
            token.cancel()
            if future is not None:
                future.cancel()
        self._executor.shutdown(wait=False)

    def close(self, timeout=30):
        if getattr(self._worker_context, "active", False):
            timeout = 0
        self._stop()
        end = time.monotonic() + max(0, timeout)
        while True:
            with self._lock:
                if not self._pending:
                    return True
            if time.monotonic() >= end:
                return False
            time.sleep(min(0.01, max(0, end - time.monotonic())))

    async def __aenter__(self):
        self._bind_loop()
        return self

    async def __aexit__(self, *unused):
        await self.aclose()
