"""执行资格、持久恢复和双完成边界；全部身份为合成测试数据。"""
import asyncio
import base64
import copy
import contextvars
import datetime
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from types import SimpleNamespace

from tansr_sdk import strict_json
from tansr_sdk.errors import Error
from tansr_sdk.executor import (AsyncExecutorClient, AsyncRunner, ExecutorClient, FileJournal, OutputWriter,
                                Rejected, Runner, Tool, adapt_async_handler, current_platform,
                                definition_bytes, definition_digest, operation_digest,
                                validate_output_status)
from tansr_sdk.lifecycle import AsyncBridge, CancellationToken, now_ms


def future(seconds=30):
    return (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=seconds)).isoformat(timespec="milliseconds").replace("+00:00", "Z")


SCOPE = {"applicationScopeId": "app", "endUserId": "user", "authorizationRevision": "1"}
DECLARATION = {"name": "Lookup", "description": "业务查询", "parameters": {"value": {"type": "number"}}}
LIMITS = {"maxControlBytes": 262144, "maxBlockBytes": 16384, "maxBatchBytes": 65536,
          "maxPendingBytes": 2097152, "maxRetainedBytes": 8388608}


def connection():
    return {"protocol": "sdk2-ext-v1", "executorId": "device", "connectionId": "connection",
            "connectionRevision": "1", "expiresAt": future(), "heartbeatAfterMs": 1000}


def operation():
    value = {"protocol": "sdk2-ext-v1", "operationId": "op", "sessionId": "session", "scope": copy.deepcopy(SCOPE),
             "binding": {"bindingId": "binding", "revision": "1", "target": {
                 "executorId": "device", "connectionId": "connection", "connectionRevision": "1",
                 "workspaceId": "workspace", "workspaceRevision": "1"}}, "toolName": "Lookup",
             "request": {"operation": "tool.invoke", "args": {"name": "Lookup",
                         "definitionDigest": definition_digest(DECLARATION), "argsJson": '{"value":-1.5}'}},
             "expiresAt": future()}
    value["digest"] = operation_digest(value)
    return value


def result():
    return {"status": "ok", "content": [{"t": "text", "text": "known-result"}]}


def terminal(op):
    return {"contract": "terminal-services-v1", "requestId": "terminal-1",
            "session": {"sessionContract": "sdk1", "sessionId": "session"}, "scope": copy.deepcopy(SCOPE),
            "executionBinding": copy.deepcopy(op["binding"]), "accepted": ["execution-stream-v1"],
            "unavailable": [], "outputAuthority": "tool-output", "limits": copy.deepcopy(LIMITS)}


class Peer:
    def __init__(self, op):
        self.operation = copy.deepcopy(op)
        self.connection = connection()
        self.receipt = None
        self.calls = []
        self.bytes = bytearray()
        self.blocks = []
        self.first_block = threading.Event()
        self.lose_batch = False
        self.lose_submit = False
        self.fail_seal = False
        self.block_status = False
        self.batch_gate = None
        self.heartbeats = 0
        self.output = {"contract": "terminal-services-v1", "operation": {
            "operationId": op["operationId"], "requestDigest": op["digest"]}, "state": "available",
            "acceptedThrough": None, "durableThrough": None, "retainedFrom": None,
            "nextByteOffset": "0", "seal": None}

    def default_deadline_ms(self):
        return now_ms() + 2000

    def call(self, name, **options):
        token = options.get("cancel") or CancellationToken()
        token.check(options.get("deadline_ms"))
        self.calls.append((name, copy.deepcopy({k: v for k, v in options.items() if k != "cancel"})))
        status = 200
        if name == "executor.register":
            body, status = self.connection, 201
        elif name == "executor.heartbeat":
            self.heartbeats += 1
            self.connection["expiresAt"] = future()
            body = self.connection
        elif name == "executor.operations.poll":
            body = {"protocol": "sdk2-ext-v1", "executorId": "device", "connectionId": "connection",
                    "operations": [self.operation] if self.receipt is None else []}
        elif name in ("execution.status", "terminal.execution.state"):
            if self.block_status and self.receipt is None and sum(n == "execution.status" for n, _ in self.calls) > 2:
                while not token.wait(0.02):
                    token.check(options.get("deadline_ms"))
                token.check()
            body = {"protocol": "sdk2-ext-v1", "operation": self.operation,
                    "status": self.receipt["status"] if self.receipt else "pending", "receipt": self.receipt}
            if name == "terminal.execution.state":
                body = {"contract": "terminal-services-v1", "session": terminal(self.operation)["session"], "execution": body}
        elif name == "executor.receipt.submit":
            self.receipt = copy.deepcopy(options["body"])
            if self.lose_submit:
                self.lose_submit = False
                raise Error("network")
            body = {"protocol": "sdk2-ext-v1", "operation": self.operation,
                    "status": self.receipt["status"], "receipt": self.receipt}
        elif name == "terminal.output.status":
            body = self.output
        elif name == "terminal.output.batch":
            if self.batch_gate is not None:
                while not self.batch_gate.wait(0.01):
                    token.check(options.get("deadline_ms"))
            batch = options["body"]
            if batch["seal"] is not None and self.fail_seal:
                raise Error("forbidden")
            for block in batch["blocks"]:
                index = int(block["seq"])
                if index < len(self.blocks):
                    assert self.blocks[index] == block
                    continue
                assert index == len(self.blocks)
                data = base64.b64decode(block["base64"], validate=True)
                assert hashlib.sha256(data).hexdigest() == block["payloadDigest"]
                assert int(block["byteOffset"]) == len(self.bytes)
                self.bytes.extend(data)
                self.blocks.append(copy.deepcopy(block))
                self.first_block.set()
            self.output["acceptedThrough"] = str(len(self.blocks) - 1) if self.blocks else None
            self.output["durableThrough"] = self.output["acceptedThrough"]
            self.output["nextByteOffset"] = str(len(self.bytes))
            self.output["state"] = "receiving" if self.blocks else "available"
            if batch["seal"] is not None:
                assert batch["seal"]["payloadDigest"] == hashlib.sha256(self.bytes).hexdigest()
                self.output["seal"] = copy.deepcopy(batch["seal"])
                self.output["state"] = "truncated" if batch["seal"]["truncated"] else "complete"
            if self.lose_batch:
                self.lose_batch = False
                raise Error("network")
            body = self.output
        else:
            raise AssertionError(name)
        return SimpleNamespace(body=copy.deepcopy(body), status=status)


class ExecutorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="tansr-python-executor-")
        self.addCleanup(self.temp.cleanup)
        self.op = operation()
        self.peer = Peer(self.op)
        self.client = ExecutorClient(self.peer, SCOPE)
        self.journal = FileJournal(str(Path(self.temp.name) / "journal"))
        self.addCleanup(self.journal.close)

    def runner(self, handler, **kwargs):
        tool = Tool(DECLARATION, handler)
        registration = {"protocol": "sdk2-ext-v1", "executorId": "device", "platform": current_platform(),
                        "workspaces": [{"workspaceId": "workspace", "revision": "1"}],
                        "operations": ["tool.invoke"], "tools": [tool.registration()]}
        runner = Runner(self.client, registration, {"Lookup": tool}, self.journal,
                        kwargs.pop("authorize", lambda op, cancel: None), connection=self.peer.connection, **kwargs)
        self.addCleanup(runner.close)
        return runner

    def test_durable_replay_business_numbers_and_conflicting_identity(self):
        calls = []
        runner = self.runner(lambda ctx, args: calls.append(args["value"]) or result())
        receipt = runner.execute(self.op)
        self.assertEqual(receipt["status"], "completed")
        self.assertEqual(runner.execute(self.op), receipt)
        self.assertEqual(calls, [-1.5])
        changed = copy.deepcopy(self.op)
        changed["request"]["args"]["argsJson"] = "{}"
        changed["digest"] = operation_digest(changed)
        self.peer.operation = changed
        with self.assertRaises(Error):
            runner.execute(changed)
        self.assertEqual(calls, [-1.5])

    def test_only_claim_survives_new_process_without_handler(self):
        path = str(Path(self.temp.name) / "child-journal")
        script = "from tansr_sdk.executor import FileJournal; import json,sys; j=FileJournal(sys.argv[1]); j.claim(json.loads(sys.argv[2])); j.close()"
        subprocess.run([sys.executable, "-c", script, path, json.dumps(self.op)], check=True,
                       env=dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1] / "src")),
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        reopened = FileJournal(path)
        self.addCleanup(reopened.close)
        self.journal = reopened
        calls = []
        runner = self.runner(lambda ctx, args: calls.append(True) or result())
        self.assertEqual(runner.execute(self.op)["status"], "unknown")
        self.assertEqual(calls, [])
        self.assertEqual(reopened.claim(self.op).receipt["status"], "unknown")

    def test_scope_generation_workspace_and_authorization_refuse_before_effect(self):
        for field, changed in (("scope", "other"), ("connectionRevision", "2"), ("workspaceRevision", "2")):
            op = copy.deepcopy(self.op)
            if field == "scope":
                op["scope"]["endUserId"] = changed
            else:
                op["binding"]["target"][field] = changed
            op["digest"] = operation_digest(op)
            calls = []
            runner = self.runner(lambda ctx, args: calls.append(True) or result())
            with self.assertRaises(Error):
                runner.execute(op)
            self.assertEqual(calls, [])

    def test_second_authorization_cannot_mutate_original_or_grant_execution(self):
        authorizations, calls = [], []
        def authorize(op, cancel):
            authorizations.append(True)
            op["request"]["args"]["argsJson"] = "{}"
            return len(authorizations) < 2
        runner = self.runner(lambda ctx, args: calls.append(True) or result(), authorize=authorize)
        self.assertEqual(runner.execute(self.op)["status"], "failed")
        self.assertEqual(calls, [])
        self.assertEqual(self.op["request"]["args"]["argsJson"], '{"value":-1.5}')

    def test_business_error_rejection_exception_and_cancelled_error(self):
        handlers = [(lambda ctx, args: {"status": "error", "message": "business decline"}, "completed"),
                    (lambda ctx, args: (_ for _ in ()).throw(Rejected("no_effect")), "failed"),
                    (lambda ctx, args: (_ for _ in ()).throw(RuntimeError("secret")), "unknown"),
                    (lambda ctx, args: {"status": "wrong"}, "unknown")]
        for index, (handler, status) in enumerate(handlers):
            op = copy.deepcopy(self.op)
            op["operationId"] += str(index)
            op["digest"] = operation_digest(op)
            self.peer.operation = op
            self.assertEqual(self.runner(handler).execute(op)["status"], status)
        op = copy.deepcopy(self.op)
        op["operationId"] = "cancel-error"
        op["digest"] = operation_digest(op)
        self.peer.operation = op
        runner = self.runner(lambda ctx, args: (_ for _ in ()).throw(asyncio.CancelledError()))
        with self.assertRaises(asyncio.CancelledError):
            runner.execute(op)
        self.assertEqual(self.journal.claim(op).receipt["status"], "unknown")

    def test_close_timeout_keeps_noncooperative_handler_execution_qualification(self):
        entered, release = threading.Event(), threading.Event()
        def handler(ctx, args):
            entered.set()
            release.wait(3)
            return result()
        runner = self.runner(handler)
        outcomes = []
        worker = threading.Thread(target=lambda: outcomes.append(runner.execute(self.op)))
        worker.start()
        self.assertTrue(entered.wait(2))
        self.assertFalse(runner.close(timeout=0.02))
        with self.assertRaises(Error):
            runner.execute(self.op)
        release.set()
        worker.join(3)
        self.assertFalse(worker.is_alive())
        self.assertTrue(runner.close(timeout=0.1))
        self.assertEqual(outcomes[0]["status"], "completed")

    def test_incremental_output_lost_ack_and_seal_failure_preserve_business_receipt(self):
        self.peer.lose_batch = True
        self.peer.fail_seal = True
        def handler(ctx, args):
            ctx.output.stdout.write(b"first\xe4")
            self.assertTrue(self.peer.first_block.wait(2))
            ctx.output.stderr.write(b"\xb8\xad-last")
            return result()
        runner = self.runner(handler, terminal=terminal(self.op), require_output=True)
        outcome = runner.execute_with_output(self.op)
        self.assertFalse(outcome.output_confirmed)
        self.assertEqual(outcome.receipt["status"], "completed")
        self.assertEqual(self.journal.claim(self.op).receipt, outcome.receipt)
        self.assertEqual(bytes(self.peer.bytes), b"first\xe4\xb8\xad-last")
        self.assertGreaterEqual(sum(name == "terminal.output.status" for name, _ in self.peer.calls), 3)

    def test_run_renews_while_handler_and_status_read_are_blocked(self):
        self.peer.block_status = True
        stop = CancellationToken()
        self.peer.lose_submit = True
        def handler(ctx, args):
            end = time.monotonic() + 1.25
            while time.monotonic() < end:
                ctx.check()
                time.sleep(0.01)
            return result()
        runner = self.runner(handler, on_receipt=lambda op, outcome: stop.cancel())
        with self.assertRaises(Error) as raised:
            runner.run(cancel=stop)
        self.assertEqual(raised.exception.code, "cancelled")
        self.assertGreaterEqual(self.peer.heartbeats, 1)
        self.assertEqual(self.peer.receipt["status"], "completed")

    def test_runner_observer_and_heartbeat_preserve_owner_context(self):
        scope = contextvars.ContextVar("runner_owner", default="missing-owner")
        owner = scope.set("original-owner")
        heartbeat_seen = threading.Event()
        stopped = CancellationToken()
        observed = []
        original = self.peer.call

        def call(name, **options):
            observed.append((threading.current_thread().name, name, scope.get()))
            if name == "executor.heartbeat":
                heartbeat_seen.set()
            return original(name, **options)

        def authorize(operation, cancel):
            observed.append((threading.current_thread().name, "authorize", scope.get()))

        def handler(context, arguments):
            end = time.monotonic() + 2
            while not heartbeat_seen.wait(0.01):
                context.check()
                self.assertLess(time.monotonic(), end)
            return result()

        self.peer.call = call
        runner = self.runner(handler, authorize=authorize, on_receipt=lambda operation, outcome: stopped.cancel())
        try:
            with self.assertRaises(Error) as done:
                runner.run(cancel=stopped)
            self.assertEqual(done.exception.code, "cancelled")
            self.assertEqual(self.peer.receipt["status"], "completed")
            background = [item for item in observed if item[0].startswith("tansr-execution-")]
            self.assertTrue(any(item[1] == "executor.heartbeat" for item in background))
            self.assertTrue(any(item[1] == "execution.status" for item in background))
            self.assertTrue(any(item[1] == "authorize" for item in background))
            self.assertEqual({item[2] for item in background}, {"original-owner"})
        finally:
            stopped.cancel()
            self.assertTrue(runner.close(timeout=1))
            scope.reset(owner)

    def test_async_handler_and_async_runner_share_durable_path(self):
        async def handler(ctx, args):
            await asyncio.sleep(0.01)
            return result()
        runner = self.runner(adapt_async_handler(handler))
        async def consume():
            async with AsyncRunner(runner) as async_runner:
                return await async_runner.execute(self.op)
        self.assertEqual(asyncio.run(consume())["status"], "completed")

    def test_async_queue_freezes_operation_and_preserves_borrowed_bridge(self):
        runner = self.runner(lambda ctx, args: result())
        original = copy.deepcopy(self.op)
        async def consume():
            bridge = AsyncBridge(workers=1, max_pending=1)
            release, entered = threading.Event(), threading.Event()
            def occupy():
                entered.set()
                release.wait(3)
            blocker = asyncio.create_task(bridge.run(occupy))
            facade = AsyncRunner(runner, bridge=bridge)
            try:
                while not entered.is_set():
                    await asyncio.sleep(0.01)
                pending = asyncio.create_task(facade.execute(self.op))
                await asyncio.sleep(0.03)
                self.op["scope"]["endUserId"] = "mutated-after-enqueue"
                release.set()
                await blocker
                receipt = await pending
                self.assertEqual(receipt["digest"], original["digest"])
                self.assertTrue(await facade.aclose())
                self.assertEqual(await bridge.run(lambda: "still-owned-by-host"), "still-owned-by-host")
                with self.assertRaises(Error) as closed:
                    await facade.execute(original)
                self.assertEqual(closed.exception.code, "closed")
            finally:
                release.set()
                await bridge.aclose()
        asyncio.run(consume())

    def test_async_queue_uses_original_default_deadline_without_late_request(self):
        async def consume():
            bridge = AsyncBridge(workers=1, max_pending=1)
            release, entered = threading.Event(), threading.Event()
            def occupy():
                entered.set()
                release.wait(3)
            blocker = asyncio.create_task(bridge.run(occupy))
            facade = AsyncExecutorClient(self.client, bridge=bridge)
            self.peer.default_deadline_ms = lambda: now_ms() + 80
            try:
                while not entered.is_set():
                    await asyncio.sleep(0.01)
                pending = asyncio.create_task(facade.heartbeat(self.peer.connection))
                await asyncio.sleep(0.13)
                with self.assertRaises(Error) as expired:
                    await pending
                self.assertEqual(expired.exception.code, "timeout")
                release.set()
                await blocker
                self.assertEqual(self.peer.calls, [])
                self.assertTrue(await facade.aclose())
            finally:
                release.set()
                await bridge.aclose()
        asyncio.run(consume())

    def test_async_client_close_tracks_worker_when_bridge_is_borrowed(self):
        release, entered = threading.Event(), threading.Event()
        original = self.peer.call
        def slow(*args, **kwargs):
            entered.set()
            release.wait(3)
            return original(*args, **kwargs)
        self.peer.call = slow
        async def consume():
            bridge = AsyncBridge(workers=1, max_pending=1)
            facade = AsyncExecutorClient(self.client, bridge=bridge)
            pending = asyncio.create_task(facade.status("session", "op"))
            try:
                while not entered.is_set():
                    await asyncio.sleep(0.01)
                self.assertFalse(await facade.aclose(timeout=0.01))
                release.set()
                with self.assertRaises(Error):
                    await pending
                self.assertTrue(await facade.aclose())
                self.assertEqual(await bridge.run(lambda: 7), 7)
            finally:
                release.set()
                await bridge.aclose()
        asyncio.run(consume())

    def test_restricted_client_does_not_fallback(self):
        client = ExecutorClient(self.peer, SCOPE, restricted=True)
        with self.assertRaises(Error):
            client.status("session", "op")
        self.assertEqual(self.peer.calls, [])
        self.assertEqual(client.executor_status(terminal(self.op)["session"], self.peer.connection, self.op)["status"], "pending")
        self.assertEqual(self.peer.calls[0][0], "terminal.execution.state")
        refused = []
        def refuse(name, **options):
            refused.append(name)
            raise Error("forbidden", http_status=403)
        self.peer.call = refuse
        with self.assertRaises(Error):
            client.executor_status(terminal(self.op)["session"], self.peer.connection, self.op)
        self.assertEqual(refused, ["terminal.execution.state"])


class OutputTests(unittest.TestCase):
    def test_output_pump_uses_owner_context_not_producer_context(self):
        scope = contextvars.ContextVar("output_owner", default="missing-owner")
        owner = scope.set("output-owner")
        op = operation()
        peer = Peer(op)
        observed = []
        original = peer.call

        def call(name, **options):
            observed.append((threading.current_thread().name, scope.get()))
            return original(name, **options)

        peer.call = call
        writer = OutputWriter(peer, terminal(op)["session"], peer.output["operation"], "device", "connection", LIMITS)

        def produce():
            scope.set("unrelated-producer")
            writer.stdout.write(b"original bytes")

        producer = threading.Thread(target=produce)
        try:
            producer.start()
            producer.join(1)
            self.assertFalse(producer.is_alive())
            self.assertTrue(peer.first_block.wait(1))
            self.assertEqual(writer.finish()["state"], "complete")
            self.assertEqual(bytes(peer.bytes), b"original bytes")
            pumps = [item for item in observed if item[0] == "tansr-output"]
            self.assertTrue(pumps)
            self.assertEqual({item[1] for item in pumps}, {"output-owner"})
        finally:
            producer.join(1)
            self.assertTrue(writer.close(timeout=1))
            scope.reset(owner)

    def test_reconcile_cancellation_and_close_wait_for_actual_io(self):
        op = operation()
        peer = Peer(op)
        writer = OutputWriter(peer, terminal(op)["session"], peer.output["operation"], "device", "connection", LIMITS)
        release, entered, cancelled = threading.Event(), threading.Event(), threading.Event()
        token = CancellationToken()
        original = peer.call
        def blocked(name, **options):
            unlink = options["cancel"].register(cancelled.set)
            try:
                entered.set()
                release.wait(3)
                return original(name, **options)
            finally:
                unlink()
        peer.call = blocked
        errors = []
        def reconcile():
            try:
                writer.reconcile(cancel=token)
            except Error as error:
                errors.append(error.code)
        thread = threading.Thread(target=reconcile)
        thread.start()
        try:
            self.assertTrue(entered.wait(2))
            token.cancel()
            self.assertTrue(cancelled.wait(1))
            self.assertFalse(writer.close(timeout=0.01))
        finally:
            release.set()
            thread.join(3)
            self.assertTrue(writer.close(timeout=1))
        self.assertEqual(errors, ["cancelled"])

    def test_bounded_prefix_drains_both_channels_and_seals_original_bytes(self):
        op = operation()
        peer = Peer(op)
        gate = threading.Event()
        peer.batch_gate = gate
        limits = dict(LIMITS, maxBlockBytes=64, maxPendingBytes=2048)
        writer = OutputWriter(peer, terminal(op)["session"], peer.output["operation"], "device", "connection", limits)
        try:
            self.assertEqual(writer.stdout.write(b"x" * 4096), 4096)
            self.assertEqual(writer.stderr.write(b"y" * 50), 50)
            state = writer.snapshot()
            self.assertTrue(state["truncated"])
            self.assertLessEqual(state["pendingBytes"], limits["maxPendingBytes"])
            gate.set()
            sealed = writer.finish()
            self.assertEqual(sealed["state"], "truncated")
            self.assertEqual(bytes(peer.bytes), b"x" * int(state["capturedBytes"]))
            with self.assertRaises(Error):
                writer.capture("stdout", b"late")
        finally:
            gate.set()
            self.assertTrue(writer.close())

    def test_empty_seal_and_impossible_watermark(self):
        op = operation()
        peer = Peer(op)
        writer = OutputWriter(peer, terminal(op)["session"], peer.output["operation"], "device", "connection", LIMITS)
        try:
            self.assertEqual(writer.finish()["state"], "complete")
            wrong = copy.deepcopy(peer.output)
            wrong["durableThrough"] = "10"
            with self.assertRaises(Error):
                validate_output_status(wrong)
        finally:
            writer.close()


class DefinitionTests(unittest.TestCase):
    def test_js_utf16_numeric_keys_missing_false_and_proto(self):
        decl = {"name": "Echo", "description": "emoji😀", "parameters": {
            "10": {"type": "number"}, "2": {"type": "string"},
            "\ue000": {"type": "string"}, "\U00010000": {"type": "string"},
            "__proto__": {"type": "object"}}, "timeoutMs": 1000.0}
        expected = '{"description":"emoji😀","name":"Echo","parameters":{"2":{"type":"string"},"10":{"type":"number"},"𐀀":{"type":"string"},"\ue000":{"type":"string"}},"timeoutMs":1000}'
        self.assertEqual(definition_bytes(decl), expected.encode("utf-8"))
        self.assertNotEqual(definition_digest(decl), definition_digest(dict(decl, readOnly=False)))
        lexical = strict_json.loads('{"name":"Echo","description":"x","timeoutMs":1e3}')
        self.assertEqual(definition_digest(lexical), definition_digest({"name": "Echo", "description": "x", "timeoutMs": 1000}))
        for invalid in (dict(decl, timeoutMs=True), dict(decl, readOnly=0), dict(decl, description="😀" * 1025)):
            with self.assertRaises(Error):
                definition_digest(invalid)


if __name__ == "__main__":
    unittest.main()
