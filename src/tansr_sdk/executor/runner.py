"""单执行资格的业务 Runner；取消不能释放仍在运行的 handler。"""
import asyncio
import contextvars
import inspect
import threading
from typing import Any, Callable, Dict, Optional

from .. import strict_json
from ..errors import Error
from ..lifecycle import CancellationToken, now_ms
from ._common import (cancellation, equal, expiry, live, receipt_for, snapshot,
                      validate, validate_operation, validate_receipt)
from .client import ExecutorClient, validate_registration
from .output import OutputWriter, query_output
from .tool import Rejected, Tool, parse_tool_arguments, verify_tool_result


class _ExecutionToken(CancellationToken):
    def __init__(self, deadline_ms: int) -> None:
        super().__init__()
        self.deadline_ms = deadline_ms

    def check(self, deadline_ms: Optional[int] = None) -> None:
        super().check(min(deadline_ms, self.deadline_ms) if deadline_ms is not None else self.deadline_ms)


class ToolContext:
    def __init__(self, cancel: CancellationToken, deadline_ms: int,
                 output: Optional[OutputWriter]) -> None:
        self.cancel, self.deadline_ms, self.output = cancel, deadline_ms, output

    @property
    def cancelled(self) -> bool:
        return self.cancel.cancelled or now_ms() >= self.deadline_ms

    def check(self) -> None:
        self.cancel.check(self.deadline_ms)


class AsyncHandler:
    """显式 async handler 适配器：自有工作线程上的自有 loop，不接管宿主 loop。"""
    def __init__(self, handler: Callable[..., Any]) -> None:
        self.handler = handler

    def __call__(self, context: ToolContext, arguments: Dict[str, Any]) -> Any:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            pass
        else:
            raise Error("reentrant_call", "async handler adapter requires a worker thread")
        loop = asyncio.new_event_loop()
        task = loop.create_task(self.handler(context, arguments))
        def cancel_task() -> None:
            loop.call_soon_threadsafe(task.cancel)
        unlink = context.cancel.register(cancel_task)
        try:
            return loop.run_until_complete(task)
        finally:
            unlink()
            # 子任务仍属这个 handler。取消后等待真实清理，不 detach 遗留执行。
            pending = asyncio.all_tasks(loop)
            for child in pending:
                child.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()


def adapt_async_handler(handler: Callable[..., Any]) -> AsyncHandler:
    return AsyncHandler(handler)


class ExecutionOutcome:
    def __init__(self, receipt: Dict[str, Any], output_status: Any = None,
                 output_confirmed: bool = True, output_error: Optional[Error] = None) -> None:
        self.receipt = snapshot(receipt)
        self.output_status = snapshot(output_status)
        self.output_confirmed = output_confirmed
        self.output_error = output_error


class Runner:
    def __init__(self, client: ExecutorClient, registration: Dict[str, Any],
                 tools: Dict[str, Tool], journal: Any,
                 authorize: Callable[[Dict[str, Any], CancellationToken], Any], *,
                 connection: Optional[Dict[str, Any]] = None, terminal: Any = None,
                 require_output: bool = False, poll_interval: float = 0.1,
                 on_receipt: Optional[Callable[..., Any]] = None) -> None:
        registration = snapshot(registration)
        validate_registration(registration)
        if (not callable(authorize) or journal is None or poll_interval < 0.01 or
                set(registration["operations"]) != {"tool.invoke"}):
            raise Error("invalid_request", "runner options")
        self.client, self.registration = client, registration
        self.tools = dict(tools)
        expected = {item["name"]: item["definitionDigest"] for item in registration.get("tools", [])}
        actual = {name: tool.definition_digest for name, tool in self.tools.items()
                  if isinstance(tool, Tool) and name == tool.name}
        if expected != actual or len(actual) != len(self.tools):
            raise Error("conflict", "runner tools differ from registration")
        if "TansrTerminalShellSandbox" in actual:
            raise Error("unsupported", "the business runner has no shell-sandbox adapter")
        # 保存 handler 与声明，不让调用方之后改 Tool 对象换掉执行函数或摘要。
        self._handlers = {name: (tool.definition_digest, tool.handler) for name, tool in self.tools.items()}
        self.journal, self.authorize = journal, authorize
        self.terminal = snapshot(terminal)
        self.require_output, self.poll_interval, self.on_receipt = require_output, poll_interval, on_receipt
        if terminal is not None:
            validate("BindingResponse", self.terminal, "terminal-services-v1")
            if (not equal(self.terminal["scope"], client.scope) or
                    "execution-stream-v1" not in self.terminal["accepted"]):
                raise Error("conflict", "runner terminal binding")
        if (require_output or client.restricted) and terminal is None:
            raise Error("invalid_request", "explicit terminal negotiation required")
        if connection is not None:
            live(connection)
            if connection["executorId"] != registration["executorId"]:
                raise Error("conflict", "runner connection")
        self._connection = snapshot(connection)
        self._lock = threading.RLock()
        self._connect_lock = threading.Lock()
        self._execute_lock = threading.Lock()
        self._stop = CancellationToken()
        self._idle = threading.Event()
        self._idle.set()
        self._closed = self._running = self._executing = False
        self._active = None  # type: Any
        self._callback = threading.local()

    @property
    def connection(self) -> Any:
        with self._lock:
            return snapshot(self._connection)

    def _guard(self) -> None:
        if getattr(self._callback, "active", False):
            raise Error("reentrant_call", "runner callback cannot synchronously wait on itself")

    def _invoke_callback(self, function: Callable[..., Any], *args: Any) -> Any:
        previous = getattr(self._callback, "active", False)
        self._callback.active = True
        try:
            return function(*args)
        finally:
            self._callback.active = previous

    def connect(self, cancel: Optional[CancellationToken] = None,
                deadline_ms: Optional[int] = None) -> Dict[str, Any]:
        self._guard()
        token = cancellation(cancel)
        while not self._connect_lock.acquire(timeout=0.05):
            token.check(deadline_ms)
            self._stop.check()
        try:
            token.check(deadline_ms)
            self._stop.check()
            current = self.connection
            if current is not None:
                live(current)
                return current
            connected = self.client.register(self.registration, cancel=token, deadline_ms=deadline_ms)
            with self._lock:
                self._connection = snapshot(connected)
            return snapshot(connected)
        finally:
            self._connect_lock.release()

    def _current(self) -> Dict[str, Any]:
        current = self.connection
        if current is None:
            raise Error("lease_expired", "runner is not connected")
        live(current)
        return current

    def _check(self, operation: Dict[str, Any], token: CancellationToken) -> None:
        before = self._current()
        deadline = min(expiry(operation["expiresAt"]), expiry(before["expiresAt"]))
        token.check(deadline)
        self.client.check_identity(before, operation)
        target = operation["binding"]["target"]
        if not any(workspace["workspaceId"] == target["workspaceId"] and
                   workspace["revision"] == target["workspaceRevision"]
                   for workspace in self.registration["workspaces"]):
            raise Error("conflict", "unregistered workspace")
        if self.terminal is not None:
            if (self.terminal["session"]["sessionId"] != operation["sessionId"] or
                    not equal(self.terminal["executionBinding"], operation["binding"])):
                raise Error("conflict", "execution binding changed")
        result = self._invoke_callback(self.authorize, snapshot(operation), token)
        if result is False:
            raise Error("forbidden", "host authorization rejected")
        token.check(deadline)
        after = self._current()
        if any(before[key] != after[key] for key in ("executorId", "connectionId", "connectionRevision")):
            raise Error("conflict", "connection changed during authorization")

    def _read_status(self, operation: Dict[str, Any], token: CancellationToken) -> Dict[str, Any]:
        current = self._current()
        deadline = min(expiry(operation["expiresAt"]), expiry(current["expiresAt"]),
                       self.client.api.default_deadline_ms())
        if self.client.restricted:
            value = self.client.executor_status(self.terminal["session"], current, operation,
                                                cancel=token, deadline_ms=deadline)
        else:
            value = self.client.status(operation["sessionId"], operation["operationId"],
                                       cancel=token, deadline_ms=deadline)
        if not equal(value["operation"], operation):
            raise Error("conflict", "remote operation changed")
        return value

    def _output_state(self, operation: Dict[str, Any], token: CancellationToken) -> Any:
        if self.terminal is None:
            return None
        deadline = min(expiry(operation["expiresAt"]), live(self._current()),
                       self.client.api.default_deadline_ms())
        token_deadline = getattr(token, "deadline_ms", None)
        if token_deadline is not None:
            deadline = min(deadline, token_deadline)
        return query_output(self.client.api, self.terminal["session"],
                            {"operationId": operation["operationId"], "requestDigest": operation["digest"]},
                            max_control_bytes=self.terminal["limits"]["maxControlBytes"], cancel=token,
                            deadline_ms=deadline)

    def _persist(self, operation: Dict[str, Any], receipt: Dict[str, Any], state: Any = None) -> ExecutionOutcome:
        validate_receipt(operation, receipt)
        # 已运行的业务事实独立于外部取消；同步存储完成之前不释放执行资格。
        self.journal.complete(snapshot(operation), snapshot(receipt))
        confirmed = state is None or state["state"] in ("complete", "truncated", "unavailable")
        return ExecutionOutcome(receipt, state, confirmed)

    def execute_with_output(self, operation: Dict[str, Any], *,
                            cancel: Optional[CancellationToken] = None,
                            deadline_ms: Optional[int] = None) -> ExecutionOutcome:
        self._guard()
        with self._lock:
            if self._running:
                raise Error("already_running")
        return self._execute(operation, cancellation(cancel), False, deadline_ms)

    def execute(self, operation: Dict[str, Any], *, cancel: Optional[CancellationToken] = None,
                deadline_ms: Optional[int] = None) -> Dict[str, Any]:
        outcome = self.execute_with_output(operation, cancel=cancel, deadline_ms=deadline_ms)
        if not outcome.output_confirmed:
            raise Error("output_incomplete", "business receipt is durable",
                        detail={"receipt": snapshot(outcome.receipt), "output": snapshot(outcome.output_status)})
        return outcome.receipt

    def _execute(self, operation: Dict[str, Any], parent: CancellationToken, monitored: bool,
                 deadline_ms: Optional[int] = None) -> ExecutionOutcome:
        operation = snapshot(operation)
        validate_operation(operation)
        tool = self._handlers.get(operation["toolName"])
        if (operation["request"]["operation"] != "tool.invoke" or tool is None or
                tool[0] != operation["request"]["args"]["definitionDigest"]):
            raise Error("unsupported", "tool is not explicitly installed")
        if not self._execute_lock.acquire(False):
            raise Error("already_running")
        original_deadline = expiry(operation["expiresAt"])
        if deadline_ms is not None:
            original_deadline = min(original_deadline, deadline_ms)
        token = _ExecutionToken(original_deadline)
        unlink_parent, unlink_stop = parent.register(token.cancel), self._stop.register(token.cancel)
        observer = watchdog = output = None  # type: Any
        monitor_stop = CancellationToken()
        monitor_cancel = CancellationToken()
        unlink_monitor = token.register(monitor_cancel.cancel)
        unlink_monitor_stop = monitor_stop.register(monitor_cancel.cancel)
        with self._lock:
            self._executing = True
            self._idle.clear()
            self._active = token
        try:
            self._check(operation, token)
            remote = self._read_status(operation, token)
            if remote["status"] != "pending" and remote["receipt"] is None:
                raise Error("outcome_unknown", "remote operation is not pending")
            state = self._output_state(operation, token)
            if (self.require_output and remote["status"] == "pending" and
                    state["state"] in ("unavailable", "gap")):
                raise Error("output_unavailable")
            claim = self.journal.claim(snapshot(operation), cancel=token)
            if claim.receipt is not None:
                validate_receipt(operation, claim.receipt)
                if remote["receipt"] is not None and not equal(remote["receipt"], claim.receipt):
                    raise Error("conflict", "remote and local receipt disagree")
                confirmed = state is None or state["state"] in ("complete", "truncated", "unavailable")
                return ExecutionOutcome(claim.receipt, state, confirmed)
            if not claim.claimed:
                return self._persist(operation, remote["receipt"] or
                                     receipt_for(operation, "unknown", "execution_outcome_unknown"), state)
            if remote["receipt"] is not None:
                return self._persist(operation, remote["receipt"], state)
            if state is not None and state["state"] in ("receiving", "complete", "truncated", "gap"):
                return self._persist(operation, receipt_for(operation, "unknown", "remote_execution_not_pending"), state)
            try:
                self._check(operation, token)
                latest = self._read_status(operation, token)
                if latest["receipt"] is not None:
                    return self._persist(operation, latest["receipt"], state)
                if latest["status"] != "pending":
                    return self._persist(operation, receipt_for(operation, "unknown", "remote_execution_not_pending"), state)
                if state is not None and state["state"] == "available":
                    state = self._output_state(operation, token)
                    if state["state"] != "available":
                        return self._persist(operation, receipt_for(operation,
                                             "failed" if state["state"] == "unavailable" else "unknown",
                                             "output_window_changed"), state)
                self._check(operation, token)
            except Error:
                return self._persist(operation, receipt_for(operation, "failed", "authorization_rejected"), state)
            deadline = original_deadline
            if not monitored:
                deadline = min(deadline, live(self._current()))
            arguments = parse_tool_arguments(operation["request"]["args"]["argsJson"])
            if state is not None and state["state"] == "available":
                current = self._current()
                output = OutputWriter(self.client.api, self.terminal["session"],
                                      {"operationId": operation["operationId"], "requestDigest": operation["digest"]},
                                      current["executorId"], current["connectionId"], self.terminal["limits"],
                                      cancel=token, deadline_ms=deadline)

            def observe() -> None:
                try:
                    while not monitor_cancel.wait(0.1):
                        self._check(operation, monitor_cancel)
                        status = self._read_status(operation, monitor_cancel)
                        if status["status"] != "pending":
                            token.cancel()
                            return
                except BaseException:
                    token.cancel()

            def watch() -> None:
                while not monitor_stop.wait(0.02):
                    try:
                        token.check(deadline)
                        live(self._current())
                    except Error:
                        token.cancel()
                        if output is not None:
                            output.abort()
                        return

            observer = threading.Thread(target=contextvars.copy_context().run, args=(observe,),
                                        name="tansr-execution-status")
            watchdog = threading.Thread(target=watch, name="tansr-execution-deadline")
            observer.start()
            watchdog.start()
            propagate = None  # type: Any
            try:
                token.check(deadline)
                result = self._invoke_callback(tool[1], ToolContext(token, deadline, output), arguments)
                if inspect.isawaitable(result):
                    if inspect.iscoroutine(result):
                        result.close()
                    raise Error("invalid_tool_result", "use adapt_async_handler for async handlers")
                result = verify_tool_result(result)
                receipt = receipt_for(operation, "completed", result={"operation": "tool.invoke",
                                      "args": {"resultJson": strict_json.dumps(result).decode("utf-8")}})
            except asyncio.CancelledError as error:
                receipt = receipt_for(operation, "unknown", "execution_outcome_unknown")
                propagate = error
            except (KeyboardInterrupt, SystemExit) as error:
                receipt = receipt_for(operation, "unknown", "execution_outcome_unknown")
                propagate = error
            except Rejected as error:
                receipt = receipt_for(operation, "failed", error.code)
            except Exception:
                receipt = receipt_for(operation, "unknown", "execution_outcome_unknown")
            outcome = self._persist(operation, receipt)
            # 业务事实先耐久；输出失败只影响第二项完成，不擦掉 receipt。
            if output is not None:
                try:
                    state = output.finish(cancel=token)
                    outcome = ExecutionOutcome(receipt, state, True)
                except Error as error:
                    outcome = ExecutionOutcome(receipt, output.snapshot(), False, error)
            if propagate is not None:
                raise propagate
            return outcome
        finally:
            monitor_stop.cancel()
            for thread in (observer, watchdog):
                if thread is not None and thread.ident is not None:
                    thread.join()
            if output is not None:
                # 即便注入的传输不合作也不能释放资格；close 的调用者可观察 False。
                while not output.close(timeout=0.1):
                    pass
            unlink_parent()
            unlink_stop()
            unlink_monitor()
            unlink_monitor_stop()
            with self._lock:
                self._active = None
                self._executing = False
                if not self._running:
                    self._idle.set()
            self._execute_lock.release()

    def _heartbeat(self, token: CancellationToken, errors: list) -> None:
        try:
            current = self._current()
            next_heartbeat = now_ms() + current["heartbeatAfterMs"]
            while not token.wait(0.02):
                current = self._current()
                if now_ms() < next_heartbeat:
                    continue
                renewed = self.client.heartbeat(current, cancel=token)
                if renewed["connectionRevision"] != current["connectionRevision"]:
                    raise Error("conflict", "connection generation changed")
                with self._lock:
                    self._connection = snapshot(renewed)
                next_heartbeat = now_ms() + renewed["heartbeatAfterMs"]
        except BaseException as error:
            if not token.cancelled:
                errors.append(error if isinstance(error, Error) else Error("lease_expired"))
                token.cancel()

    def run(self, *, cancel: Optional[CancellationToken] = None) -> None:
        self._guard()
        token = CancellationToken()
        unlink_parent = cancellation(cancel).register(token.cancel)
        unlink_stop = self._stop.register(token.cancel)
        with self._lock:
            if self._running or self._executing or self._closed:
                unlink_parent()
                unlink_stop()
                raise Error("already_running" if not self._closed else "closed")
            self._running = True
            self._idle.clear()
        heartbeat = None  # type: Any
        failures = []  # type: list
        try:
            self.connect(cancel=token)
            heartbeat = threading.Thread(target=contextvars.copy_context().run,
                                         args=(self._heartbeat, token, failures), name="tansr-execution-lease")
            heartbeat.start()
            while not token.cancelled:
                batch = self.client.poll(self._current(), cancel=token)
                for operation in batch["operations"]:
                    token.check()
                    outcome = self._execute(operation, token, True)
                    try:
                        self.client.submit(operation, outcome.receipt, cancel=token)
                    except Error as original:
                        status = self._read_status(operation, token)
                        if status["receipt"] is None or not equal(status["receipt"], outcome.receipt):
                            raise original
                    if self.on_receipt is not None:
                        self._invoke_callback(self.on_receipt, snapshot(operation), outcome)
                    if not outcome.output_confirmed:
                        raise Error("output_incomplete", "business receipt durable and submitted",
                                    detail={"receipt": snapshot(outcome.receipt)})
                token.wait(self.poll_interval)
            if failures:
                raise failures[0]
            token.check()
        finally:
            token.cancel()
            if heartbeat is not None and heartbeat.ident is not None:
                heartbeat.join()
            unlink_parent()
            unlink_stop()
            with self._lock:
                self._running = False
                if not self._executing:
                    self._idle.set()

    def close(self, timeout: float = 30.0) -> bool:
        self._guard()
        self._stop.cancel()
        with self._lock:
            active = self._active
            self._closed = True
        if active is not None:
            active.cancel()
        return self._idle.wait(max(0.0, timeout))

    def __enter__(self) -> "Runner":
        if self._closed:
            raise Error("closed")
        return self

    def __exit__(self, *args: Any) -> None:
        if not self.close():
            raise Error("not_quiescent")
