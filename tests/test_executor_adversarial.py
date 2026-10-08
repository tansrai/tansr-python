"""PY-A18—24 集中的身份、进程窗口、并发输出与当前权限反例。"""
import asyncio
import base64
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest

from tansr_sdk.errors import Error
from tansr_sdk.executor import (AsyncRunner, ExecutorClient, FileJournal, OutputWriter, Runner,
                                Tool, adapt_async_handler, current_platform, operation_digest)
from tansr_sdk.lifecycle import CancellationToken
from test_executor import DECLARATION, LIMITS, SCOPE, Peer, future, operation, result, terminal


class ExecutorAdversarialTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="tansr-executor-boundaries-")
        self.addCleanup(self.temp.cleanup)
        self.op = operation()
        self.peer = Peer(self.op)
        self.journal = FileJournal(str(Path(self.temp.name) / "journal"))
        self.addCleanup(self.journal.close)

    def runner(self, handler, **options):
        tool = Tool(DECLARATION, handler)
        registration = {"protocol": "sdk2-ext-v1", "executorId": "device", "platform": current_platform(),
                        "workspaces": [{"workspaceId": "workspace", "revision": "1"}],
                        "operations": ["tool.invoke"], "tools": [tool.registration()]}
        value = Runner(ExecutorClient(self.peer, SCOPE), registration, {"Lookup": tool}, self.journal,
                       options.pop("authorize", lambda op, token: None), connection=self.peer.connection,
                       **options)
        self.addCleanup(value.close)
        return value

    def test_every_identity_substitution_and_expiry_has_zero_handler(self):
        paths = [("scope", "applicationScopeId"), ("scope", "endUserId"), ("scope", "authorizationRevision"),
                 ("sessionId",), ("binding", "bindingId"), ("binding", "revision"),
                 ("binding", "target", "executorId"), ("binding", "target", "connectionId"),
                 ("binding", "target", "connectionRevision"), ("binding", "target", "workspaceId"),
                 ("binding", "target", "workspaceRevision"), ("expiresAt",)]
        calls = []
        for path in paths:
            with self.subTest(path=path):
                changed = copy.deepcopy(self.op)
                target = changed
                for key in path[:-1]:
                    target = target[key]
                target[path[-1]] = future(-1) if path[-1] == "expiresAt" else "2"
                changed["digest"] = operation_digest(changed)
                self.peer.operation = changed
                runner = self.runner(lambda ctx, args: calls.append(True) or result(), terminal=terminal(self.op))
                with self.assertRaises(Error):
                    runner.execute(changed)
                self.assertEqual(calls, [])
        self.peer.connection["expiresAt"] = future(-1)
        with self.assertRaises(Error):
            self.runner(lambda ctx, args: calls.append(True) or result())
        self.assertEqual(calls, [])
        self.assertFalse(any(name == "executor.receipt.submit" for name, _ in self.peer.calls))

    def test_missing_installed_tool_never_dispatches_or_claims(self):
        changed = copy.deepcopy(self.op)
        changed["toolName"] = changed["request"]["args"]["name"] = "MissingBusinessTool"
        changed["digest"] = operation_digest(changed)
        runner = self.runner(lambda ctx, args: self.fail("unexpected handler"))
        with self.assertRaises(Error) as unavailable:
            runner.execute(changed)
        self.assertEqual(unavailable.exception.code, "unsupported")
        self.assertEqual(self.peer.calls, [])
        self.assertTrue(self.journal.claim(changed).claimed)

    def test_crash_after_claim_effect_or_receipt_never_repeats_effect(self):
        script = """import json,os,sys
from tansr_sdk.executor import FileJournal,Runner,Tool,ExecutorClient,current_platform
from test_executor import Peer,DECLARATION,SCOPE,result
directory,mode,raw=sys.argv[1:]
op=json.loads(raw)
class CrashJournal(FileJournal):
 def claim(self,operation,cancel=None):
  claimed=super().claim(operation,cancel)
  assert claimed.claimed
  if mode=='claim': os._exit(71)
  return claimed
 def complete(self,operation,receipt,cancel=None):
  super().complete(operation,receipt,cancel)
  os._exit(73)
journal=CrashJournal(directory)
def handler(context,arguments):
 with FileJournal(directory) as observer:
  assert not observer.claim(op).claimed
 descriptor=os.open(os.path.join(directory,'effect.txt'),os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
 with os.fdopen(descriptor,'wb') as effect:
  effect.write(b'one effect');effect.flush();os.fsync(effect.fileno())
 if mode=='effect': os._exit(72)
 return result()
tool=Tool(DECLARATION,handler)
peer=Peer(op)
registration={'protocol':'sdk2-ext-v1','executorId':'device','platform':current_platform(),
 'workspaces':[{'workspaceId':'workspace','revision':'1'}],'operations':['tool.invoke'],'tools':[tool.registration()]}
runner=Runner(ExecutorClient(peer,SCOPE),registration,{'Lookup':tool},journal,lambda op,token:None,connection=peer.connection)
runner.execute(op)
raise AssertionError('crash hook not reached')
"""
        for index, mode in enumerate(("claim", "effect", "receipt")):
            with self.subTest(window=mode):
                path = Path(self.temp.name) / mode
                completed = subprocess.run([sys.executable, "-c", script, str(path), mode, json.dumps(self.op)],
                    env=dict(os.environ, PYTHONPATH=os.pathsep.join((str(Path(__file__).resolve().parents[1] / "src"),
                                                                   str(Path(__file__).resolve().parent)))),
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=20)
                self.assertEqual(completed.returncode, 71 + index, completed.stderr.decode("utf-8", "replace"))
                reopened = FileJournal(str(path))
                try:
                    self.journal = reopened
                    runner = self.runner(lambda ctx, args: self.fail("crashed operation re-executed"))
                    receipt = runner.execute(self.op)
                    self.assertEqual(receipt["status"], "completed" if mode == "receipt" else "unknown")
                    self.assertEqual(runner.execute(self.op), receipt)
                    self.assertEqual(reopened.claim(self.op).receipt, receipt)
                    output_runner = self.runner(lambda ctx, args: self.fail("cold output caused re-execution"),
                                                terminal=terminal(self.op))
                    unresolved = output_runner.execute_with_output(self.op)
                    self.assertEqual(unresolved.receipt, receipt)
                    self.assertFalse(unresolved.output_confirmed)
                    effect = path / "effect.txt"
                    self.assertEqual(effect.read_bytes() if effect.exists() else b"", b"" if mode == "claim" else b"one effect")
                finally:
                    reopened.close()

    def test_current_authorization_generation_binding_and_lease_cancel_running_handler(self):
        for index, change in enumerate(("authority", "connection", "binding", "lease")):
            with self.subTest(change=change):
                self.op = operation()
                self.op["operationId"] = "changing-" + str(index)
                self.op["digest"] = operation_digest(self.op)
                self.peer = Peer(self.op)
                allowed = [True]
                entered = threading.Event()
                outcomes = []
                def authorize(op, token):
                    return allowed[0]
                def handler(ctx, args):
                    entered.set()
                    while not ctx.cancel.wait(0.01):
                        ctx.check()
                    ctx.check()
                runner = self.runner(handler, terminal=terminal(self.op), authorize=authorize)
                worker = threading.Thread(target=lambda: outcomes.append(runner.execute_with_output(self.op)))
                worker.start()
                try:
                    self.assertTrue(entered.wait(2))
                    if change == "authority":
                        allowed[0] = False
                    elif change == "connection":
                        with runner._lock:
                            runner._connection["connectionRevision"] = "2"
                    elif change == "binding":
                        self.peer.operation["binding"]["revision"] = "2"
                        self.peer.operation["digest"] = operation_digest(self.peer.operation)
                    else:
                        with runner._lock:
                            runner._connection["expiresAt"] = future(-1)
                    worker.join(3)
                    self.assertFalse(worker.is_alive())
                    self.assertEqual(outcomes[0].receipt["status"], "unknown")
                    self.assertEqual(self.journal.claim(self.op).receipt["status"], "unknown")
                finally:
                    runner.close(0)
                    worker.join(3)

    def test_keyboard_interrupt_is_durable_unknown_and_rethrown(self):
        runner = self.runner(lambda ctx, args: (_ for _ in ()).throw(KeyboardInterrupt()))
        with self.assertRaises(KeyboardInterrupt):
            runner.execute(self.op)
        self.assertEqual(self.journal.claim(self.op).receipt["status"], "unknown")
        self.assertEqual(runner.execute(self.op)["status"], "unknown")

    def test_late_output_window_never_grants_permission_and_disappearing_window_refuses(self):
        for available_first in (False, True):
            with self.subTest(available_first=available_first):
                op = operation()
                op["operationId"] = "output-window-" + str(available_first)
                op["digest"] = operation_digest(op)
                self.peer = Peer(op)
                calls, states, authorizations = [], [], []
                real_call = self.peer.call
                def call(name, **options):
                    if name == "terminal.output.status":
                        states.append(True)
                        self.peer.output["state"] = "available" if available_first and len(states) == 1 else "unavailable"
                        self.peer.output["nextByteOffset"] = "0" if self.peer.output["state"] == "available" else None
                    return real_call(name, **options)
                self.peer.call = call
                def authorize(operation, cancel):
                    authorizations.append(True)
                    if not available_first and len(authorizations) == 3:
                        self.peer.output.update(state="available", nextByteOffset="0")
                def handler(ctx, args):
                    calls.append(ctx.output)
                    # 原 unavailable 不得因后来出现的窗口增加 output 能力。
                    self.peer.output.update(state="available", nextByteOffset="0")
                    return result()
                runner = self.runner(handler, terminal=terminal(op), require_output=available_first, authorize=authorize)
                receipt = runner.execute(op)
                self.assertEqual(receipt["status"], "failed" if available_first else "completed")
                self.assertEqual(calls, [] if available_first else [None])
                self.assertEqual(len(states), 2 if available_first else 1)

    def test_explicit_async_handler_streams_before_return_and_keeps_output_separate(self):
        async def handler(ctx, args):
            ctx.output.stdout.write(b"async-first\xe4")
            deadline = time.monotonic() + 2
            while not self.peer.first_block.is_set():
                self.assertLess(time.monotonic(), deadline)
                await asyncio.sleep(0.01)
            ctx.output.stderr.write(b"\xb8\xad")
            return result()
        runner = self.runner(adapt_async_handler(handler), terminal=terminal(self.op), require_output=True)
        async def consume():
            async with AsyncRunner(runner) as facade:
                return await facade.execute_with_output(self.op)
        outcome = asyncio.run(consume())
        self.assertEqual(outcome.receipt["status"], "completed")
        self.assertTrue(outcome.output_confirmed)
        self.assertEqual(bytes(self.peer.bytes), b"async-first\xe4\xb8\xad")

    def test_cancelled_async_await_keeps_handler_cleanup_and_durable_unknown(self):
        entered, cleaning, release = threading.Event(), threading.Event(), threading.Event()
        async def handler(ctx, args):
            entered.set()
            try:
                await asyncio.sleep(30)
            finally:
                cleaning.set()
                while not release.is_set():
                    await asyncio.sleep(0.01)
        runner = self.runner(adapt_async_handler(handler))
        async def consume():
            facade = AsyncRunner(runner)
            pending = asyncio.create_task(facade.execute(self.op))
            try:
                while not entered.is_set():
                    await asyncio.sleep(0.01)
                pending.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await pending
                while not cleaning.is_set():
                    await asyncio.sleep(0.01)
                self.assertFalse(await facade.aclose(timeout=0.02))
                with self.assertRaises(Error):
                    runner.execute(self.op)
                self.assertIsNone(self.journal.claim(self.op).receipt)
            finally:
                release.set()
                self.assertTrue(await facade.aclose(timeout=3))
            self.assertEqual(self.journal.claim(self.op).receipt["status"], "unknown")
        asyncio.run(consume())


class OutputAdversarialTests(unittest.TestCase):
    def test_many_producers_slow_ack_bound_encoded_inflight_and_host_stdout(self):
        op = operation()
        peer = Peer(op)
        gate = threading.Event()
        peer.batch_gate = gate
        limits = dict(LIMITS, maxBlockBytes=64, maxPendingBytes=4096)
        writer = OutputWriter(peer, terminal(op)["session"], peer.output["operation"], "device", "connection", limits)
        before = sys.stdout
        host_output = io.StringIO()
        samples, failures = [], []
        start = threading.Barrier(5)
        def produce(channel, byte):
            try:
                start.wait()
                for _ in range(40):
                    self.assertEqual(getattr(writer, channel).write(byte * 64), 64)
                    samples.append(writer.snapshot())
            except BaseException as error:
                failures.append(error)
        workers = [threading.Thread(target=produce, args=("stdout" if i % 2 else "stderr", bytes([65 + i]))) for i in range(4)]
        writer.stdout.write(b"first")
        try:
            end = time.monotonic() + 2
            while not writer.snapshot()["inflightBytes"]:
                self.assertLess(time.monotonic(), end)
                time.sleep(0.01)
            for worker in workers:
                worker.start()
            start.wait()
            for i in range(20):
                print("independent host " + str(i), file=host_output)
            for worker in workers:
                worker.join(3)
                self.assertFalse(worker.is_alive())
            self.assertEqual(failures, [])
            self.assertTrue(any(item["inflightBytes"] > 0 for item in samples))
            self.assertTrue(all(item["pendingBytes"] <= limits["maxPendingBytes"] for item in samples))
            state = writer.snapshot()
            self.assertEqual(int(state["capturedBytes"]) + int(state["droppedBytes"]), 5 + 4 * 40 * 64)
            self.assertTrue(state["truncated"])
            gate.set()
            sealed = writer.finish()
            self.assertEqual(sealed["state"], "truncated")
            self.assertEqual(sealed["seal"]["payloadDigest"], hashlib.sha256(peer.bytes).hexdigest())
            self.assertEqual([block["seq"] for block in peer.blocks], [str(i) for i in range(len(peer.blocks))])
            for block in peer.blocks[1:]:
                raw = base64.b64decode(block["base64"])
                self.assertIn(raw[:1], (b"B", b"D") if block["channel"] == "stdout" else (b"A", b"C"))
                self.assertEqual(raw, raw[:1] * len(raw))
            self.assertEqual(len(peer.bytes), int(state["capturedBytes"]))
            self.assertIs(sys.stdout, before)
            self.assertEqual(len(host_output.getvalue().splitlines()), 20)
        finally:
            gate.set()
            writer.close()

    def test_cancelled_output_continues_draining_and_never_claims_seal_ack(self):
        op = operation()
        peer = Peer(op)
        token = CancellationToken()
        writer = OutputWriter(peer, terminal(op)["session"], peer.output["operation"], "device", "connection", LIMITS, cancel=token)
        try:
            writer.stdout.write(b"prefix")
            self.assertTrue(peer.first_block.wait(2))
            token.cancel()
            self.assertEqual(writer.stdout.write(b"drain-out"), 9)
            self.assertEqual(writer.stderr.write(b"drain-err"), 9)
            self.assertEqual(writer.snapshot()["droppedBytes"], "18")
            with self.assertRaises(Error):
                writer.finish()
            self.assertFalse(writer.snapshot()["sealed"])
            self.assertEqual(bytes(peer.bytes), b"prefix")
        finally:
            self.assertTrue(writer.close())

    def test_wrong_ack_digest_gap_and_duplicate_success_lost(self):
        for attack in ("ack", "digest", "gap", "seal", "lost"):
            with self.subTest(attack=attack):
                op = operation()
                peer = Peer(op)
                peer.lose_batch = attack == "lost"
                real_call = peer.call
                def call(name, **options):
                    response = real_call(name, **options)
                    if name == "terminal.output.batch":
                        if attack == "ack":
                            response.body.update(acceptedThrough="99", durableThrough="99", nextByteOffset="100")
                        elif attack == "digest":
                            response.body["operation"]["requestDigest"] = "0" * 64
                        elif attack == "gap":
                            response.body["state"] = "gap"
                        elif attack == "seal" and response.body["seal"] is not None:
                            response.body["seal"]["payloadDigest"] = "0" * 64
                    return response
                peer.call = call
                writer = OutputWriter(peer, terminal(op)["session"], peer.output["operation"], "device", "connection", LIMITS)
                try:
                    writer.stdout.write(b"original")
                    if attack == "lost":
                        sealed = writer.finish()
                        self.assertEqual(sealed["state"], "complete")
                        self.assertEqual(bytes(peer.bytes), b"original")
                        self.assertEqual(len(peer.blocks), 1)
                        self.assertTrue(any(name == "terminal.output.status" for name, _ in peer.calls))
                    else:
                        with self.assertRaises(Error):
                            writer.finish()
                        self.assertFalse(writer.snapshot()["sealed"])
                finally:
                    writer.close()
