import asyncio
import dataclasses
import threading

import pytest

from tansr_sdk import AuthToken, CallOptions, Client, AsyncClient, Error
from tansr_sdk import strict_json
from tansr_sdk.api.operations import manifest
from tansr_sdk.lifecycle import AsyncBridge, CancellationToken, now_ms
from tansr_sdk.transport import HttpResponse


def headers(**extra):
    value = {
        "tansr-contract": "unified-v1",
        "tansr-manifest-revision": "7",
        "tansr-schema-hash": "sha256:" + manifest()["schemaHash"],
        "tansr-domain": "session",
        "content-type": "application/json",
    }
    value.update(extra)
    return value


class Transport:
    def __init__(self, handler=None):
        self.requests = []
        self.handler = handler or (lambda request: HttpResponse(200, headers(), b"{}"))

    def request(self, request):
        self.requests.append(request)
        return self.handler(request)

    def close(self, timeout=30):
        return True


def client(transport, provider=None):
    return Client(
        "https://serve.example.test", provider or (lambda cancel: AuthToken("test-token", "one")), transport=transport
    )


def test_path_utf8_original_body_header_identity_and_no_legacy_routes():
    transport = Transport()
    with client(transport) as api:
        api.call(
            "session.message.send",
            parameters={"id": "测试 界"},
            body={"text": "hello"},
            request_key="original",
            capability_closure="a" * 64,
        )
    sent = transport.requests[0]
    assert "/api/sessions/%E6%B5%8B%E8%AF%95%20%E7%95%8C/messages" in sent.url
    assert sent.headers["idempotency-key"] == "original"
    assert sent.headers["tansr-closure-id"] == "a" * 64
    assert strict_json.loads(sent.body) == {"text": "hello"}


def test_wire_limit_integer_normalized_only_at_transport_configuration_boundary():
    transport = Transport()
    limits = strict_json.loads(b'{"capacity":1048576}')
    with client(transport) as api:
        api.call(
            "session.get",
            parameters={"id": "s"},
            max_response_bytes=limits["capacity"],
            deadline_ms=strict_json.JsonInt(str(now_ms() + 30000)),
        )
    assert type(transport.requests[0].max_response_bytes) is int
    assert type(transport.requests[0].deadline_ms) is int


@pytest.mark.parametrize("bad", ["https://user:pass@a", "https://a/path", "https://a?", "ftp://a", "https://a\\evil"])
def test_bad_origin(bad):
    with pytest.raises(Error):
        Client(bad, lambda cancel: ("token", "p"))


@pytest.mark.parametrize(
    "kwargs",
    [
        {"parameters": {"id": "../x"}},
        {"parameters": {"id": "ok", "extra": "x"}},
        {"parameters": {"id": "ok"}, "query": {"unexpected": "x"}},
        {"parameters": {"id": "ok"}, "request_key": "bad\r\nheader"},
        {"parameters": {"id": "ok"}, "if_match": "1"},
        {"parameters": {"id": "ok"}, "raw_body": b"undeclared"},
    ],
)
def test_invalid_request_never_reads_credentials(kwargs):
    calls = []
    with client(Transport(), lambda token: calls.append(1)) as api:
        with pytest.raises(Error):
            api.call("session.message.send", **kwargs)
    assert calls == []


def test_missing_control_body_is_rejected_before_credentials():
    calls = []
    with client(Transport(), lambda token: calls.append(1)) as api:
        with pytest.raises(Error) as error:
            api.call("executor.heartbeat", parameters={"id": "executor"})
        assert error.value.code == "invalid_input"
    assert calls == []


def test_invalid_control_lexemes_and_types_never_reach_credentials_or_transport():
    # 公开入口的控制字段；不可用codec单测代替零凭据、零网络证明。
    for bad in (True, False, None, -1, 1.5, strict_json.JsonFloat("1e0"),
                strict_json.JsonInt("-0"), "1", 9007199254740992):
        calls = []
        transport = Transport()
        body = {
            "protocol": "sdk2-ext-v1", "bindingId": "binding",
            "generations": {"historyEpoch": "epoch", "deletionGeneration": "0", "projectionRevision": "0"},
            "afterSequence": None, "limit": bad, "maxBytes": 1024,
        }
        with client(transport, lambda cancel: calls.append(1)) as api:
            with pytest.raises(Error):
                api.call("archive.records.read", parameters={"id": "binding"}, body=body)
        assert calls == [] and transport.requests == []


def test_declared_query_schema_does_not_require_a_get_body():
    calls = []

    def sent(request):
        assert request.method == "GET" and request.body == b""
        assert "protocol=sdk2-ext-v1" in request.url
        raise Error("observed_transport")

    transport = Transport(sent)

    def provider(cancel):
        calls.append(1)
        return AuthToken("t", "p")

    with client(transport, provider) as api:
        with pytest.raises(Error) as error:
            api.call("archive.binding.target", parameters={"id": "s"})
        assert error.value.code == "observed_transport"
    assert calls == [1] and len(transport.requests) == 1


def test_freeze_before_provider_and_principal_cannot_change():
    body = {"text": "before"}
    count = [0]

    def provider(cancel):
        count[0] += 1
        body["text"] = "after"
        return AuthToken("test", str(count[0]))

    transport = Transport()
    with client(transport, provider) as api:
        api.call("session.message.send", parameters={"id": "s"}, body=body)
        assert strict_json.loads(transport.requests[0].body)["text"] == "before"
        with pytest.raises(Error) as error:
            api.call("session.get", parameters={"id": "s"})
        assert error.value.code == "permission"
    assert len(transport.requests) == 1


@pytest.mark.parametrize(
    "response",
    [
        HttpResponse(302, headers(location="https://elsewhere"), b""),
        HttpResponse(200, {}, b"{}"),
        HttpResponse(200, headers(**{"tansr-contract": "other"}), b"{}"),
        HttpResponse(200, headers(), b'{"a":1,"a":2}'),
        HttpResponse(200, list(headers().items()) + [("Tansr-Contract", "unified-v1")], b"{}"),
        HttpResponse(200, headers(**{"content-type": "text/html"}), b"{}"),
    ],
)
def test_response_contract_and_redirect_rejected(response):
    with client(Transport(lambda request: response)) as api:
        with pytest.raises(Error):
            api.call("session.get", parameters={"id": "s"})


def error_response(code="conflict", action="same-request"):
    return HttpResponse(
        409,
        headers(),
        strict_json.dumps(
            {
                "contract": "unified-v1",
                "code": code,
                "status": 409,
                "retryAction": action,
                "requestId": "original",
                "traceId": "test-trace",
                "message": "synthetic conflict",
            }
        ),
    )


def test_explicit_retry_keeps_bytes_deadline_and_requires_authentic_evidence():
    transport = Transport(lambda request: error_response())
    with client(transport) as api:
        options = CallOptions(parameters={"id": "s"}, body={"text": "a"}, request_key="original")
        with pytest.raises(Error) as first:
            api.call("session.message.send", options)
        # Frozen wire's code names are schema constrained; this test requires a valid error.
        assert first.value.code == "http", first.value.message
        transport.handler = lambda request: HttpResponse(202, headers(), b'{"accepted":true}')
        reply = api.retry_same_request("session.message.send", options, first.value)
        assert reply.status == 202
        assert transport.requests[0].body == transport.requests[1].body
        assert transport.requests[0].headers["deadline"] == transport.requests[1].headers["deadline"]
        changed = dataclasses.replace(options, body={"text": "b"})
        with pytest.raises(Error):
            api.retry_same_request("session.message.send", changed, first.value)
        first.value.retry_action = "none"
        with pytest.raises(Error):
            api.retry_same_request("session.message.send", options, first.value)
        assert len(transport.requests) == 2


def test_close_reports_pending_provider_and_cancels_cooperatively():
    entered, release = threading.Event(), threading.Event()

    def provider(cancel):
        entered.set()
        release.wait(2)
        return AuthToken("t", "p")

    api = client(Transport(), provider)
    errors = []

    def work():
        try:
            api.call("session.list")
        except Error as error:
            errors.append(error.code)

    thread = threading.Thread(target=work)
    thread.start()
    assert entered.wait(1)
    assert api.close(0) is False
    release.set()
    thread.join(2)
    assert errors == ["cancelled"]
    assert api.close(0) is True


def test_provider_reentrant_close_does_not_wait_for_itself():
    results = []

    def provider(cancel):
        results.append(api.close(30))
        return AuthToken("t", "p")

    api = client(Transport(), provider)
    with pytest.raises(Error) as error:
        api.call("session.list", deadline_ms=now_ms() + 1000)
    assert error.value.code == "cancelled"
    assert results == [False]
    assert api.close(0)


def test_sync_pending_capacity_includes_blocked_provider():
    entered, release = threading.Event(), threading.Event()
    errors = []

    def provider(cancel):
        entered.set()
        release.wait(2)
        return AuthToken("t", "p")

    api = Client("https://serve.example.test", provider, transport=Transport(), max_pending=1)

    def work():
        try:
            api.call("session.list")
        except Error as error:
            errors.append(error.code)

    thread = threading.Thread(target=work)
    thread.start()
    assert entered.wait(1)
    try:
        with pytest.raises(Error) as error:
            api.call("session.list")
        assert error.value.code == "capacity"
        assert api.close(0) is False
    finally:
        release.set()
        thread.join(2)
        api.close(0)
    assert errors == ["cancelled"]


def test_client_close_owns_an_open_stream_even_without_an_active_reader():
    class Stream:
        status = 200
        headers = dict(headers(), **{"content-type": "text/event-stream", "tansr-event-envelope": "unified-v1"})
        closed = False

        def iter_bytes(self):
            return iter([])

        def close(self):
            self.closed = True

    class Streaming(Transport):
        def stream(self, request):
            self.response = Stream()
            return self.response

    transport = Streaming()
    api = client(transport)
    stream = api.events(parameters={"id": "s"})
    assert api.close(timeout=0)
    assert transport.response.closed
    with pytest.raises(Error) as error:
        next(stream)
    assert error.value.code == "cancelled"


def test_async_cancel_does_not_claim_uncooperative_worker_stopped_or_block_loop():
    async def scenario():
        bridge = AsyncBridge(workers=1, max_pending=1)
        started, release = threading.Event(), threading.Event()
        token = CancellationToken()

        def blocking():
            started.set()
            release.wait(2)

        task = asyncio.ensure_future(bridge.run(blocking, cancel=token))
        while not started.is_set():
            await asyncio.sleep(0.001)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert token.cancelled
        assert await bridge.aclose(0) is False
        release.set()
        assert await bridge.aclose(2) is True

    asyncio.run(scenario())


def test_async_client_uses_same_protocol_engine_and_rejects_cross_loop():
    async_api = AsyncClient(client=client(Transport()))

    async def first():
        value = await async_api.call("session.get", parameters={"id": "s"})
        assert value.status == 200

    asyncio.run(first())

    async def second():
        with pytest.raises(Error):
            await async_api.call("session.get", parameters={"id": "s"})

    asyncio.run(second())
    async_api._calls.close(0)
    async_api._streams.close(0)
    async_api.sync.close(0)


def test_async_client_closes_only_its_own_sync_client():
    sync = client(Transport())

    async def scenario():
        async with AsyncClient(client=sync) as borrowed:
            assert (await borrowed.call("session.list")).status == 200
        assert sync.call("session.list").status == 200
        owned = AsyncClient("https://serve.example.test", lambda cancel: AuthToken("t", "p"), transport=Transport())
        assert await owned.aclose()
        with pytest.raises(Error) as error:
            owned.sync.call("session.list")
        assert error.value.code == "closed"

    try:
        asyncio.run(scenario())
    finally:
        sync.close(0)


def test_async_deadline_in_queue_never_starts_new_work():
    async def scenario():
        bridge = AsyncBridge(workers=1, max_pending=1)
        started, release = threading.Event(), threading.Event()

        def block():
            started.set()
            release.wait(2)

        task = asyncio.ensure_future(bridge.run(block))
        while not started.is_set():
            await asyncio.sleep(0.001)
        invoked = []
        with pytest.raises(Error) as error:
            await bridge.run(lambda: invoked.append(1), _deadline_ms=now_ms() + 30)
        assert error.value.code == "timeout" and invoked == []
        release.set()
        await task
        assert await bridge.aclose(1)

    asyncio.run(scenario())


def test_async_unclaimed_resource_is_disposed_before_bridge_becomes_quiet():
    async def scenario():
        bridge = AsyncBridge(workers=1, max_pending=1)
        started, release, disposed = threading.Event(), threading.Event(), threading.Event()
        resource = object()

        def produce():
            started.set()
            release.wait(2)
            return resource

        def abandon(value):
            assert value is resource
            disposed.set()

        task = asyncio.ensure_future(bridge.run(produce, _abandon=abandon))
        while not started.is_set():
            await asyncio.sleep(0.001)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not await bridge.aclose(0)
        release.set()
        assert await bridge.aclose(1)
        assert disposed.is_set()

    asyncio.run(scenario())
