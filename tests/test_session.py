import asyncio
import threading
from collections import deque

import pytest

from tansr_sdk.api import ApiResponse
from tansr_sdk.errors import Error
from tansr_sdk.lifecycle import CancellationToken, now_ms
from tansr_sdk.session import (
    Answer,
    AsyncSessionClient,
    Budget,
    CompactOptions,
    CreateOptions,
    Created,
    ForkReference,
    ImageBlock,
    Input,
    InputContent,
    InputTarget,
    LabeledCheckpoint,
    Session,
    SessionClient,
    SessionEvent,
    SpeechRequest,
    TextBlock,
    TranscriptionRequest,
    TurnTracker,
    WriteOptions,
)
from tansr_sdk.sse import Frame
from tansr_sdk.strict_json import dumps


def response(body, status=200, closure=None, raw=b"", mime="application/json"):
    return ApiResponse(
        status=status,
        body=body,
        raw_body=raw,
        headers={"content-type": mime},
        capability_closure=closure,
        content_type=mime,
    )


def discovery():
    return response(
        {
            "protocol": "sdk2-ext-v1",
            "contracts": [
                {"contract": "sdk1", "availability": "legacy-complete"},
                {"contract": "sdk2-offload-v1", "availability": "source-required"},
            ],
        }
    )


def closure(operation, state="enabled"):
    return response({"closureId": "a" * 64, "operations": {operation: state}}, closure="a" * 64)


class FakeApi:
    def __init__(self, entries=(), family="sdk1"):
        self.family = family
        self.entries = deque(entries)
        self.calls = []
        self.stream = None
        self.deadline = now_ms() + 30000

    def default_deadline_ms(self):
        return self.deadline

    def call(self, operation, options):
        options.cancel.check(options.deadline_ms)
        self.calls.append((operation, options))
        expected, value = self.entries.popleft()
        assert operation == expected
        if callable(value):
            return value(options)
        if isinstance(value, Exception):
            raise value
        return value

    def events(self, operation, options):
        assert operation == "session.events.observe"
        self.calls.append((operation, options))
        if callable(self.stream):
            return self.stream(options)
        return self.stream


def session(entries=(), family="sdk1"):
    api = FakeApi(entries, family)
    return Session(SessionClient(api), Created("s1")), api


def accepted(with_session=True):
    body = {"accepted": True}
    if with_session:
        body["sessionId"] = "s1"
    return response(body, 202 if with_session else 200)


@pytest.mark.parametrize("family", ["sdk1", "sdk2-offload-v1"])
def test_create_family_and_shared_original_deadline(family):
    raw = {"sessionId": "s1", "resumed": False, "lastSeq": 0}
    if family != "sdk1":
        raw.update(contract=family, availability="source-required")
    api = FakeApi([("session.capabilities", discovery()), ("session.create", response(raw, 201))], family)
    options = CreateOptions(request_id="retained_1" if family != "sdk1" else None, budget=Budget(0, 0))
    result = SessionClient(api).create(options)
    assert result.id == "s1"
    assert [item[1].deadline_ms for item in api.calls] == [api.deadline, api.deadline]
    assert api.calls[-1][1].body["budget"] == {"maxUsd": 0, "maxTokens": 0}
    assert options.write.deadline_ms is None


@pytest.mark.parametrize(
    "family,options",
    [
        ("sdk1", CreateOptions(request_id="bad")),
        ("sdk2-offload-v1", CreateOptions()),
        ("sdk2-offload-v1", CreateOptions(request_id="bad id")),
        ("sdk2-offload-v1", CreateOptions(request_id="okay", fork=ForkReference("s1", "c1"))),
        ("sdk1", CreateOptions(budget=Budget(max_tokens=True))),
        ("sdk1", CreateOptions(budget=Budget(max_usd=float("nan")))),
    ],
)
def test_invalid_create_has_no_network(family, options):
    api = FakeApi(family=family)
    with pytest.raises(Error):
        SessionClient(api).create(options)
    assert api.calls == []


def test_create_snapshot_cannot_be_mutated_by_discovery_callback():
    tools = [{"name": "lookup", "inputSchema": {"type": "object"}}]
    options = CreateOptions(client_tools=tools)

    def mutate(call):
        tools[0]["name"] = "changed"
        return discovery()

    api = FakeApi(
        [
            ("session.capabilities", mutate),
            ("session.create", response({"sessionId": "s1", "resumed": False, "lastSeq": 0}, 201)),
        ]
    )
    SessionClient(api).create(options)
    assert api.calls[-1][1].body["clientTools"][0]["name"] == "lookup"


def test_resume_rejects_changed_identity_without_creating_replacement():
    api = FakeApi(
        [
            ("session.capabilities", discovery()),
            ("session.create", response({"sessionId": "other", "resumed": True, "lastSeq": 1})),
        ]
    )
    with pytest.raises(Error):
        SessionClient(api).resume("s1")
    assert len(api.calls) == 2


def test_offload_requires_source_lifecycle_metadata():
    api = FakeApi(
        [
            ("session.capabilities", discovery()),
            ("session.create", response({"sessionId": "s1", "resumed": False, "lastSeq": 0}, 201)),
        ],
        "sdk2-offload-v1",
    )
    with pytest.raises(Error):
        SessionClient(api).create(request_id="original")


def test_discovery_disabled_rejects_before_write():
    value, api = session([("discovery.session.capabilities", closure("session.message.send", "disabled"))])
    with pytest.raises(Error):
        value.send("hello")
    assert len(api.calls) == 1


def test_closure_header_mismatch_rejects_write():
    bad = closure("session.message.send")
    bad.capability_closure = "b" * 64
    value, api = session([("discovery.session.capabilities", bad)])
    with pytest.raises(Error):
        value.send("hello")
    assert len(api.calls) == 1


def test_message_blocks_and_request_identity_preserved():
    value, api = session(
        [("discovery.session.capabilities", closure("session.message.send")), ("session.message.send", accepted())]
    )
    write = WriteOptions(request_key="original-key", deadline_ms=now_ms() + 5000)
    result = value.send_blocks([TextBlock("你好"), ImageBlock("image/png", "aGVsbG8=")], write)
    assert result.accepted
    call = api.calls[-1][1]
    assert call.body == {
        "blocks": [{"t": "text", "text": "你好"}, {"t": "image", "mime": "image/png", "data": "aGVsbG8="}]
    }
    assert call.request_key == "original-key" and call.capability_closure == "a" * 64
    assert api.calls[0][1].deadline_ms == call.deadline_ms == write.deadline_ms


def test_request_failure_is_not_retried_or_rekeyed():
    value, api = session(
        [("discovery.session.capabilities", closure("session.interrupt")), ("session.interrupt", Error("network"))]
    )
    with pytest.raises(Error):
        value.interrupt(WriteOptions(request_key="keep"))
    assert [call[0] for call in api.calls] == ["discovery.session.capabilities", "session.interrupt"]
    assert api.calls[-1][1].request_key == "keep"


def test_history_zero_and_explicit_close():
    value, api = session(
        [("session.history.read", response({"messages": [], "total": 3})), ("session.close", accepted())]
    )
    assert value.history(limit=0)["total"] == 3
    assert api.calls[-1][1].query["limit"] == "0"
    assert value.close().accepted
    assert api.calls[-1][0] == "session.close"


def test_permission_and_question_explicit_ticket_digest():
    value, api = session(
        [
            ("discovery.session.capabilities", closure("session.permission.decide")),
            ("session.permission.decide", accepted(False)),
            ("discovery.session.capabilities", closure("session.question.answer")),
            ("session.question.answer", accepted(False)),
        ]
    )
    value.permission("ticket", "digest", "deny")
    assert api.calls[-1][1].body == {"digest": "digest", "verdict": "deny"}
    assert api.calls[-1][1].parameters["ticketId"] == "ticket"
    value.answer("question", [Answer("q1", ["choice1"], "typed")])
    assert api.calls[-1][1].body["answers"] == [
        {"questionId": "q1", "selectedOptionIds": ["choice1"], "freeText": "typed"}
    ]


def test_durable_input_does_not_degrade_or_change_turn():
    value, api = session([("session.input.capabilities", response({"durableAck": False}))])
    request = Input("input1", InputTarget("epoch", "turn"), InputContent(text="next"), "durable")
    with pytest.raises(Error):
        value.submit_input(request)
    assert len(api.calls) == 1


def test_input_receipt_preserves_identity_and_consumed_distinction():
    receipt = {
        "sessionId": "s1",
        "inputId": "input1",
        "turnId": "turn",
        "historyEpoch": "epoch",
        "source": "strict",
        "state": "accepted",
        "durability": "durable",
        "ordinal": 0,
        "revision": 0,
    }
    value, api = session(
        [
            ("session.input.capabilities", response({"durableAck": True})),
            ("discovery.session.capabilities", closure("session.input.submit")),
            ("session.input.submit", response({"outcome": "accepted", "receipt": receipt}, 202)),
        ]
    )
    request = Input("input1", InputTarget("epoch", "turn"), InputContent(blocks=[TextBlock("next")]), "durable")
    result = value.submit_input(request)
    assert result["receipt"]["state"] == "accepted"
    assert len({call[1].deadline_ms for call in api.calls}) == 1


@pytest.mark.parametrize(
    "key,value", [("historyEpoch", "other"), ("turnId", "old"), ("durability", "memory"), ("ordinal", True)]
)
def test_input_receipt_drift_rejected(key, value):
    receipt = {
        "sessionId": "s1",
        "inputId": "input1",
        "turnId": "turn",
        "historyEpoch": "epoch",
        "source": "strict",
        "state": "accepted",
        "durability": "durable",
        "ordinal": 0,
        "revision": 0,
    }
    receipt[key] = value
    current, api = session(
        [
            ("session.input.capabilities", response({"durableAck": True})),
            ("discovery.session.capabilities", closure("session.input.submit")),
            ("session.input.submit", response({"outcome": "accepted", "receipt": receipt}, 202)),
        ]
    )
    with pytest.raises(Error):
        current.submit_input(Input("input1", InputTarget("epoch", "turn"), InputContent(text="next"), "durable"))


def test_checkpoint_original_bytes_restore_and_compact_status():
    checkpoint = {"checkpointId": "c1", "sessionId": "s1", "messageCount": 2}
    original = b"\x00\xff\r\ncheckpoint"
    value, api = session(
        [
            ("session.checkpoint.export", response(None, raw=original, mime="application/octet-stream")),
            ("discovery.session.capabilities", closure("session.checkpoint.import")),
            ("session.checkpoint.import", response(checkpoint, 201)),
            ("discovery.session.capabilities", closure("session.checkpoint.restore")),
            (
                "session.checkpoint.restore",
                response({"status": "restored", "checkpointId": "c1", "fromMessages": 3, "toMessages": 2}),
            ),
            ("discovery.session.capabilities", closure("session.compact")),
            ("session.compact", response({"status": "rejected", "reason": "empty_history"})),
        ]
    )
    assert value.export_checkpoint("c1") == original
    assert value.import_checkpoint(original, "标签").message_count == 2
    assert api.calls[-1][1].raw_body == original
    assert value.restore("c1", False)["toMessages"] == 2
    assert value.compact(CompactOptions(checkpoint=LabeledCheckpoint("saved")))["status"] == "rejected"


def test_transcription_and_speech_payloads():
    value, api = session(
        [
            ("discovery.session.capabilities", closure("session.audio.transcribe")),
            ("session.audio.transcribe", response({"text": "heard"})),
            ("discovery.session.capabilities", closure("session.audio.speak")),
            ("session.audio.speak", response({"audio": "bytes"})),
        ]
    )
    assert value.transcribe(TranscriptionRequest("base64", language="zh", diarize=False))["text"] == "heard"
    assert api.calls[-1][1].body["diarize"] is False
    assert value.speak(SpeechRequest("hello", format="wav", speed=1))["audio"] == "bytes"
    assert api.calls[-1][1].max_response_bytes == 32 * 1024 * 1024


def event(seq, kind, turn=None, terminal=None, **raw_fields):
    raw = {"type": kind, "sessionId": "s1", "seq": seq, "ts": 1}
    if turn is not None:
        raw["turnId"] = turn
    raw.update(raw_fields)
    return {
        "contract": "unified-v1",
        "eventId": str(seq),
        "domain": "session",
        "type": kind,
        "cursorSet": {
            "eventCursor": str(seq),
            "archiveCoverage": None,
            "outputWatermark": None,
            "materialConsumed": None,
            "ackReceipt": None,
        },
        "terminalStatus": terminal,
        "raw": raw,
    }


class Frames:
    def __init__(self, values):
        self.values = iter(values)
        self.closed = False

    def __iter__(self):
        return self

    def __next__(self):
        envelope = next(self.values)
        return Frame(envelope["type"], dumps(envelope).decode("utf8"), envelope["eventId"], None)

    def close(self):
        self.closed = True


def test_events_deduplicate_and_deliver_additive_without_remote_interrupt():
    value, api = session()
    api.stream = Frames(
        [
            event(1, "msg.text.delta", text="old"),
            event(2, "custom.additive", field="kept"),
            event(2, "custom.additive"),
            event(3, "turn.completed", "t", "completed"),
        ]
    )
    with value.events("1") as stream:
        received = list(stream)
        assert stream.last_event_id == "3"
    assert [item.kind for item in received] == ["custom.additive", "turn.completed"]
    assert received[0].raw["field"] == "kept"
    assert api.stream.closed
    assert [call[0] for call in api.calls] == ["session.events.observe"]


def test_stream_identity_rejected_and_no_cursor_advance():
    value, api = session()
    bad = event(2, "turn.completed", "t", "completed", sessionId="other")
    api.stream = Frames([bad])
    stream = value.events("1")
    with pytest.raises(Error):
        next(stream)
    assert stream.last_event_id == "1" and api.stream.closed


def test_tracker_ignores_old_turn_and_eof_is_not_a_result():
    tracker = TurnTracker(5)
    assert tracker.observe(SessionEvent(event(4, "turn.completed", "old", "completed"))) is None
    assert tracker.observe(SessionEvent(event(6, "turn.completed", "old", "completed"))) is None
    assert tracker.observe(SessionEvent(event(7, "turn.started", "new"))) is None
    assert tracker.observe(SessionEvent(event(8, "turn.completed", "old", "completed"))) is None
    result = tracker.observe(SessionEvent(event(9, "turn.completed", "new", "completed")))
    assert result.status == "completed" and result.turn_id == "new"
    assert tracker.observe(SessionEvent(event(10, "turn.aborted", "new", "aborted"))) is None


def test_replay_and_gap_invalidate_turn_inference():
    tracker = TurnTracker.from_replay(5)
    assert tracker.observe(SessionEvent(event(2, "turn.started", "resumed"))) is None
    assert tracker.active_turn_id == "resumed"
    assert tracker.observe(SessionEvent(event(6, "turn.completed", "resumed", "completed"))).status == "completed"
    tracker = TurnTracker.resume(5, "resumed")
    assert tracker.observe(SessionEvent({"type": "server.replay.gap", "raw": {}})) is None
    assert tracker.needs_reconciliation
    assert tracker.observe(SessionEvent(event(7, "turn.completed", "resumed", "completed"))) is None


def test_gap_ahead_of_log_resets_stream_delivery_cursor():
    value, api = session()
    gap = event(0, "server.replay.gap", reason="ahead_of_log")
    gap["eventId"] = gap["cursorSet"]["eventCursor"] = None
    api.stream = Frames([gap, event(1, "turn.started", "new")])
    with value.events("99") as stream:
        assert next(stream).kind == "server.replay.gap"
        assert stream.last_event_id is None
        assert next(stream).kind == "turn.started"


def test_parent_cancellation_stops_observer_locally():
    value, api = session()
    api.stream = Frames([event(1, "turn.started", "t")])
    cancel = CancellationToken()
    stream = value.events(cancel=cancel)
    cancel.cancel()
    with pytest.raises(Error) as failure:
        next(stream)
    assert failure.value.code == "cancelled"
    assert api.stream.closed and len(api.calls) == 1


def test_async_facade_matches_sync_without_closing_borrowed_api():
    async def run():
        api = FakeApi(
            [
                ("session.capabilities", discovery()),
                ("session.create", response({"sessionId": "s1", "resumed": False, "lastSeq": 0}, 201)),
                ("discovery.session.capabilities", closure("session.message.send")),
                ("session.message.send", accepted()),
                ("session.history.read", response({"messages": []})),
            ]
        )
        api.stream = Frames([event(1, "turn.started", "t"), event(2, "turn.completed", "t", "completed")])
        async with AsyncSessionClient(api) as client:
            value = await client.create()
            assert (await value.send("hello")).accepted
            assert (await value.history(limit=0))["messages"] == []
            async with await value.events() as stream:
                seen = [item.kind async for item in stream]
            assert seen == ["turn.started", "turn.completed"]
        assert "session.close" not in [item[0] for item in api.calls]

    asyncio.run(run())


def test_async_long_stream_keeps_control_channel_and_cancellation():
    class BlockingFrames:
        def __init__(self, options):
            self.cancel = options.cancel
            self.reading = threading.Event()
            self.stopped = threading.Event()

        def __iter__(self):
            return self

        def __next__(self):
            self.reading.set()
            self.cancel.wait(5)
            self.stopped.set()
            self.cancel.check()
            raise StopIteration

        def close(self):
            self.cancel.cancel()

    async def run():
        api = FakeApi(
            [
                ("session.get", response({"sessionId": "s1", "status": "running", "live": True, "lastSeq": 0})),
                ("discovery.session.capabilities", closure("session.interrupt")),
                ("session.interrupt", accepted()),
            ]
        )
        created = []

        def stream_factory(options):
            result = BlockingFrames(options)
            created.append(result)
            return result

        api.stream = stream_factory
        client = AsyncSessionClient(api, workers=1, stream_workers=1)
        value = await client.attach("s1")
        stream = await value.events()
        waiting = asyncio.ensure_future(stream.__anext__())
        for _ in range(100):
            if created[0].reading.is_set():
                break
            await asyncio.sleep(0.005)
        assert (await asyncio.wait_for(value.interrupt(), 1)).accepted
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting
        assert await client.aclose()
        assert created[0].stopped.is_set()

    asyncio.run(run())


def test_async_queue_time_uses_original_deadline_without_any_write():
    async def run():
        value, api = session()
        api.default_deadline_ms = lambda: now_ms() + 100
        client = AsyncSessionClient(api, workers=1, max_pending=1)
        started = threading.Event()
        release = threading.Event()

        def occupy():
            started.set()
            release.wait(3)

        busy = asyncio.ensure_future(client._control.run(occupy))
        try:
            while not started.is_set():
                await asyncio.sleep(0.005)
            from tansr_sdk.session import AsyncSession

            wrapped = AsyncSession(client, value)
            with pytest.raises(Error) as failure:
                await asyncio.wait_for(wrapped.send("must-not-reach-serve"), 1)
            assert failure.value.code == "timeout"
            assert api.calls == []
        finally:
            release.set()
            await busy
            assert await client.aclose()

    asyncio.run(run())


def test_async_wrong_loop_close_rejects_before_mutating_owner():
    client = AsyncSessionClient(FakeApi())
    first = asyncio.new_event_loop()
    second = asyncio.new_event_loop()
    try:
        first.run_until_complete(client.__aenter__())
        with pytest.raises(Error) as failure:
            second.run_until_complete(client.aclose())
        assert failure.value.code == "invalid_input"
        assert client._closed is False
        assert first.run_until_complete(client.aclose()) is True
    finally:
        first.close()
        second.close()
