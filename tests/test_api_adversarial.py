"""A07—10/A39：错误映射、重试等待与现有宿主资源的集中边界。"""
import asyncio
import copy
import contextvars
from concurrent.futures import ThreadPoolExecutor
import dataclasses
import threading
import time
import traceback
import unittest

from tansr_sdk import AsyncClient, AuthToken, CallOptions, Client, Error, strict_json
from tansr_sdk.api.operations import manifest
from tansr_sdk.lifecycle import AsyncBridge, CancellationToken, now_ms
from tansr_sdk.transport import HttpResponse


def response_headers(**extra):
    result = {"tansr-contract": "unified-v1", "tansr-manifest-revision": "7",
              "tansr-schema-hash": "sha256:" + manifest()["schemaHash"], "tansr-domain": "session",
              "content-type": "application/json"}
    result.update(extra)
    return result


def failure(code="conflict", status=409, action="same-request", **extra):
    body = {"contract": "unified-v1", "traceId": "trace", "requestId": "original",
            "code": code, "status": status, "retryAction": action, "message": "synthetic failure"}
    body.update(extra)
    return body


class Port:
    def __init__(self, handler=None, stream_factory=None):
        self.handler = handler or (lambda request: HttpResponse(200, response_headers(), b"{}"))
        self.stream_factory = stream_factory
        self.requests, self.opened = [], []
        self.closed = False

    def request(self, request):
        self.requests.append(request)
        return self.handler(request)

    def stream(self, request):
        self.requests.append(request)
        stream = self.stream_factory()
        self.opened.append(stream)
        return stream

    def close(self, timeout=30):
        self.closed = True
        for stream in self.opened:
            stream.close()
        return True


def api_for(port, provider=None):
    return Client("https://serve.example.test", provider or (lambda token: AuthToken("token", "principal")), transport=port)


def frame(number):
    cursor = str(number)
    value = {"contract": "unified-v1", "eventId": cursor, "domain": "session", "type": "future.event",
             "cursorSet": {"eventCursor": cursor, "archiveCoverage": None, "outputWatermark": None,
                           "materialConsumed": None, "ackReceipt": None},
             "terminalStatus": None, "raw": {"type": "future.event", "seq": number, "sessionId": "s", "ts": 1}}
    return b"id: " + cursor.encode("ascii") + b"\ndata: " + strict_json.dumps(value) + b"\n\n"


class CountedStream:
    status = 200

    def __init__(self, count=256, gate=None, combined=False):
        self.headers = response_headers(**{"content-type": "text/event-stream", "tansr-event-envelope": "unified-v1"})
        self.count, self.gate, self.combined = count, gate, combined
        self.reads = 0
        self.entered = threading.Event()
        self.closed = threading.Event()

    def iter_bytes(self):
        for index in range(self.count):
            self.entered.set()
            if self.gate is not None:
                self.gate.wait(3)  # 模拟注入的慢读取；close 只发合作信号，不能伪造线程静止。
            self.reads += 1
            yield frame(index + 1) + (frame(index + 2) if self.combined else b"")

    def close(self):
        self.closed.set()


class ErrorBoundaryTests(unittest.TestCase):
    def test_invalid_domain_and_retry_after_headers_reject_before_error_delivery(self):
        # 冻结 ResponseHeaders 限定九域，Retry-After 只接受正整数字符串；不是HTTP通用宽松解析。
        cases = [{"tansr-domain": "made-up"}, {"retry-after": "0"}, {"retry-after": "1e3"},
                 {"retry-after": "1.5"}, {"retry-after": "Wed, 21 Oct 2099 07:28:00 GMT"}]
        for extra in cases:
            with self.subTest(headers=extra):
                response = HttpResponse(409, response_headers(**extra), strict_json.dumps(failure()))
                with api_for(Port(lambda request: response)) as api:
                    with self.assertRaises(Error) as refused:
                        api.call("session.get", parameters={"id": "s"})
                    self.assertEqual(refused.exception.code, "contract")

    def test_code_http_action_combinations_and_status_mismatch_are_rejected(self):
        cases = [(503, failure("result_unknown", 503, "same-request")),
                 (412, failure("precondition_failed", 412, "none")),
                 (400, failure("not_canonical", 400, "refresh")),
                 (401, failure("forbidden", 401, "none")),
                 (409, failure("forbidden", 403, "none"))]
        for status, body in cases:
            with self.subTest(status=status, code=body["code"], action=body["retryAction"]):
                with api_for(Port(lambda request: HttpResponse(status, response_headers(), strict_json.dumps(body)))) as api:
                    with self.assertRaises(Error) as refused:
                        api.call("session.get", parameters={"id": "s"})
                    self.assertEqual(refused.exception.code, "contract")

    def test_secret_probes_never_appear_in_default_error_formatting_but_detail_survives(self):
        secret = "PRIVATE-PROBE-TOKEN-BODY-DETAIL"
        body = failure("result_unknown", 503, "query-status", message=secret, traceId=secret,
                       requestId=secret, detail={"nested": {"original": secret}, "domainCode": "commit_unknown"})
        port = Port(lambda request: HttpResponse(503, response_headers(), strict_json.dumps(body)))
        with api_for(port, lambda cancel: AuthToken(secret, "principal")) as api:
            with self.assertRaises(Error) as delivered:
                api.call("session.message.send", parameters={"id": "s"}, body={"text": secret}, request_key=secret)
            error = delivered.exception
            self.assertEqual((error.code, error.http_status, error.wire_code, error.retry_action),
                             ("http", 503, "result_unknown", "query-status"))
            self.assertEqual(error.detail, body["detail"])
            self.assertEqual(error.request_id, secret)
            for text in (str(error), repr(error), "".join(traceback.format_exception_only(type(error), error))):
                self.assertNotIn(secret, text)
            with self.assertRaises(Error):
                api.retry_same_request("session.message.send", CallOptions(parameters={"id": "s"},
                                       body={"text": secret}, request_key=secret), error)
            self.assertEqual(len(port.requests), 1)
        local = Error("permission", secret, detail={"secret": secret})
        self.assertNotIn(secret, str(local) + repr(local))


class RetryWaitTests(unittest.TestCase):
    def prepare(self, provider=None, delay=300):
        port = Port(lambda request: HttpResponse(409, response_headers(), strict_json.dumps(failure(retryAfterMs=delay))))
        api = api_for(port, provider)
        self.addCleanup(api.close, 0)
        options = CallOptions(parameters={"id": "s"}, body={"text": "original body"},
                              request_key="original", deadline_ms=now_ms() + 5000)
        with self.assertRaises(Error) as original:
            api.call("session.message.send", options)
        self.assertEqual(original.exception.code, "http")
        return api, port, options, original.exception

    def test_sync_retry_after_cancellation_has_no_second_auth_or_write(self):
        credentials = []
        api, port, options, original = self.prepare(lambda token: credentials.append(1) or AuthToken("t", "p"))
        token = CancellationToken()
        errors, started = [], threading.Event()
        def retry():
            started.set()
            try:
                api.retry_same_request("session.message.send", dataclasses.replace(options, cancel=token), original)
            except Error as error:
                errors.append(error.code)
        worker = threading.Thread(target=retry)
        worker.start()
        try:
            self.assertTrue(started.wait(1))
            time.sleep(0.03)
            token.cancel()
            worker.join(1)
            self.assertFalse(worker.is_alive())
            self.assertEqual(errors, ["cancelled"])
            self.assertEqual(len(port.requests), 1)
            self.assertEqual(credentials, [1])
        finally:
            token.cancel()
            worker.join(2)

    def test_principal_change_during_original_wait_has_no_second_write(self):
        principal = ["before"]
        reads = []
        def provider(token):
            reads.append(principal[0])
            return AuthToken("same-format-token", principal[0])
        api, port, options, original = self.prepare(provider, delay=120)
        original_bytes = port.requests[0].body
        changed = threading.Timer(0.02, lambda: principal.__setitem__(0, "after"))
        changed.start()
        try:
            with self.assertRaises(Error) as rejected:
                api.retry_same_request("session.message.send", options, original)
            self.assertEqual(rejected.exception.code, "permission")
            self.assertEqual(reads, ["before", "after"])
            self.assertEqual(len(port.requests), 1)
            self.assertEqual(port.requests[0].body, original_bytes)
            self.assertEqual(port.requests[0].deadline_ms, options.deadline_ms)
        finally:
            changed.cancel()
            changed.join()

    def test_async_retry_wait_cancel_retains_original_evidence_and_borrowed_client(self):
        api, port, options, original = self.prepare()
        evidence = copy.deepcopy(original.detail)
        async def host():
            facade = AsyncClient(client=api)
            pending = asyncio.create_task(facade.retry_same_request("session.message.send", options, original))
            await asyncio.sleep(0.03)
            pending.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await pending
            self.assertTrue(await facade.aclose(1))
            self.assertEqual(len(port.requests), 1)
            self.assertEqual(original.detail, evidence)
            port.handler = lambda request: HttpResponse(200, response_headers(), b"{}")
            self.assertEqual(api.call("session.get", parameters={"id": "s"}).status, 200)
        asyncio.run(host())


class StreamAndHostTests(unittest.TestCase):
    def test_async_provider_preserves_calling_task_context_and_principal(self):
        principal = contextvars.ContextVar("provider_principal", default="missing-principal")

        async def host():
            principal.set("same-host-principal")
            port = Port()
            sync = api_for(port, lambda cancel: AuthToken(principal.get(), principal.get()))
            facade = AsyncClient(client=sync)
            try:
                self.assertEqual(sync.call("session.get", parameters={"id": "s"}).status, 200)
                self.assertEqual((await facade.call("session.get", parameters={"id": "s"})).status, 200)
                self.assertEqual([request.headers["authorization"] for request in port.requests],
                                 ["Bearer same-host-principal", "Bearer same-host-principal"])
                self.assertEqual(principal.get(), "same-host-principal")
            finally:
                self.assertTrue(await facade.aclose(1))
                self.assertTrue(sync.close(1))
        asyncio.run(host())

    def test_bridge_worker_context_does_not_leak_into_next_request(self):
        value = contextvars.ContextVar("worker_context", default="no-calling-context")

        async def host():
            bridge = AsyncBridge(workers=1, max_pending=2)
            value.set("request-one")

            def callback():
                seen = value.get()
                value.set("callback-local-value")
                return seen

            try:
                self.assertEqual(await bridge.run(callback), "request-one")
                self.assertEqual(value.get(), "request-one")
                value.set("request-two")
                self.assertEqual(await bridge.run(value.get), "request-two")
            finally:
                self.assertTrue(await bridge.aclose(1))
        asyncio.run(host())

    def test_slow_async_consumer_is_pull_based_and_cancel_discards_cached_frame(self):
        async def host():
            port = Port(stream_factory=lambda: CountedStream(combined=True))
            sync = api_for(port)
            facade = AsyncClient(client=sync)
            token = CancellationToken()
            try:
                stream = await facade.events(parameters={"id": "s"}, cancel=token)
                raw = port.opened[0]
                await asyncio.sleep(0.02)
                self.assertEqual(raw.reads, 0)
                self.assertEqual((await stream.__anext__()).id, "1")
                await asyncio.sleep(0.04)
                self.assertEqual(raw.reads, 1)
                token.cancel()
                with self.assertRaises(Error) as cancelled:
                    await stream.__anext__()
                self.assertEqual(cancelled.exception.code, "cancelled")
                self.assertEqual(raw.reads, 1)
                self.assertTrue(await stream.aclose(1))
                self.assertTrue(raw.closed.is_set())
            finally:
                self.assertTrue(await facade.aclose(1))
                self.assertTrue(sync.close(1))
        asyncio.run(host())

    def test_four_blocked_stream_workers_do_not_starve_control_or_host_executor(self):
        async def host():
            loop = asyncio.get_running_loop()
            loop.set_debug(True)
            unhandled = []
            loop.set_exception_handler(lambda loop, context: unhandled.append(context))
            release = threading.Event()
            port = Port(stream_factory=lambda: CountedStream(gate=release))
            sync = api_for(port)
            facade = AsyncClient(client=sync)
            tasks = []
            pool = ThreadPoolExecutor(max_workers=1)
            try:
                streams = [await facade.events(parameters={"id": "s"}) for _ in range(4)]
                tasks = [asyncio.create_task(stream.__anext__()) for stream in streams]
                end = time.monotonic() + 2
                while not all(raw.entered.is_set() for raw in port.opened):
                    self.assertLess(time.monotonic(), end)
                    await asyncio.sleep(0.005)
                response = await asyncio.wait_for(facade.call("session.get", parameters={"id": "s"}), 0.5)
                self.assertEqual(response.status, 200)
                self.assertEqual(await loop.run_in_executor(pool, lambda: "host-alive"), "host-alive")
                self.assertFalse(await facade.aclose(0.02))
                self.assertTrue(all(raw.closed.is_set() for raw in port.opened))
                release.set()
                values = await asyncio.gather(*tasks, return_exceptions=True)
                self.assertTrue(all(isinstance(value, Error) for value in values))
                self.assertTrue(await facade.aclose(1))
                self.assertEqual(await loop.run_in_executor(pool, lambda: 9), 9)
                self.assertEqual(sync.call("session.get", parameters={"id": "s"}).status, 200)
                self.assertEqual(unhandled, [])
            finally:
                release.set()
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
                await facade.aclose(1)
                sync.close(1)
                pool.shutdown(wait=True)
        asyncio.run(host())

    def test_two_async_clients_share_host_client_without_false_worker_quiescence(self):
        async def host():
            loop = asyncio.get_running_loop()
            loop.set_debug(True)
            unhandled = []
            loop.set_exception_handler(lambda loop, context: unhandled.append(context))
            baseline = {thread.ident for thread in threading.enumerate() if thread.name.startswith("tansr")}
            entered, release = threading.Event(), threading.Event()
            def handle(request):
                if request.url.endswith("/blocked"):
                    entered.set()
                    release.wait(3)
                return HttpResponse(200, response_headers(), b"{}")
            port = Port(handle)
            sync = api_for(port)
            first, second = AsyncClient(client=sync), AsyncClient(client=sync)
            task = asyncio.create_task(first.call("session.get", parameters={"id": "blocked"}))
            try:
                end = time.monotonic() + 2
                while not entered.is_set():
                    self.assertLess(time.monotonic(), end)
                    await asyncio.sleep(0.005)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                self.assertFalse(await first.aclose(0.01))
                self.assertEqual((await second.call("session.get", parameters={"id": "control"})).status, 200)
                self.assertFalse(port.closed)
                release.set()
                self.assertTrue(await first.aclose(1))
                self.assertEqual((await second.call("session.get", parameters={"id": "after"})).status, 200)
                self.assertTrue(await second.aclose(1))
                self.assertEqual(sync.call("session.list").status, 200)
            finally:
                release.set()
                await first.aclose(1)
                await second.aclose(1)
                sync.close(1)
            end = time.monotonic() + 1
            while any(thread.ident not in baseline and thread.name.startswith("tansr")
                      for thread in threading.enumerate()):
                self.assertLess(time.monotonic(), end, "owned SDK worker survived quiescent close")
                await asyncio.sleep(0.005)
            self.assertEqual(asyncio.all_tasks(loop), {asyncio.current_task()})
            self.assertEqual(unhandled, [])
        asyncio.run(host())

    def test_sync_client_close_does_not_claim_blocked_stream_reader_is_quiet(self):
        release = threading.Event()
        port = Port(stream_factory=lambda: CountedStream(gate=release))
        api = api_for(port)
        stream = api.events(parameters={"id": "s"})
        errors = []
        def read():
            try:
                next(stream)
            except Error as error:
                errors.append(error.code)
        worker = threading.Thread(target=read)
        worker.start()
        try:
            self.assertTrue(port.opened[0].entered.wait(1))
            self.assertFalse(api.close(0.01))
        finally:
            release.set()
            worker.join(2)
            self.assertTrue(api.close(1))
        self.assertFalse(worker.is_alive())
        self.assertEqual(errors, ["cancelled"])
