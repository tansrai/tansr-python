"""真实Serve会话驱动。由共有宿主传入合成fixture信息，产品调用全部使用公开SDK。"""

import asyncio
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import threading
import time

from tansr_sdk.api import AuthToken, Client
from tansr_sdk.errors import Error
from tansr_sdk.lifecycle import CancellationToken, now_ms
from tansr_sdk.session import (
    Answer,
    AsyncSessionClient,
    CompactOptions,
    CreateOptions,
    ImageBlock,
    Input,
    InputContent,
    InputTarget,
    SessionClient,
    SpeechRequest,
    TextBlock,
    TranscriptionRequest,
    TurnTracker,
)
from tansr_sdk.strict_json import dumps, loads


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def rejected(fn, statuses=()):
    try:
        fn()
    except Error as error:
        if statuses:
            require(error.http_status in statuses, "expected real Serve HTTP rejection: " + str(error))
        return error
    raise AssertionError("operation unexpectedly succeeded")


def wait_idle(session):
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        meta = session.meta()
        if meta.status == "idle":
            return
        require(meta.status == "running", "session ended while waiting for idle")
        time.sleep(0.01)
    raise AssertionError("kernel did not settle after terminal event")


def wait_kind(stream, kind):
    for event in stream:
        if event.kind == kind:
            return event.raw
        require(event.turn_outcome() is None, "turn ended before requested event")
    raise AssertionError("EOF is not the requested event")


def finish(stream, tracker, expected="completed"):
    answer = []
    for event in stream:
        require(event.kind != "server.replay.gap", "turn needs explicit replay-gap reconciliation")
        require(len(event.envelope) == 7, "event lost exact seven-key envelope")
        if event.kind == "msg.text.delta" and isinstance(event.raw.get("text"), str):
            answer.append(event.raw["text"])
        outcome = tracker.observe(event)
        if outcome is not None:
            require(outcome.status == expected, "unexpected current-turn outcome: " + outcome.status)
            require(
                outcome.turn_id is not None and outcome.turn_id == tracker.active_turn_id,
                "terminal changed current turn identity",
            )
            return "".join(answer)
    raise AssertionError("EOF is not turn completion")


def complete(session, prompt):
    floor = session.meta().last_seq
    with session.events(str(floor), deadline_ms=now_ms() + 25000) as stream:
        tracker = TurnTracker(floor)
        require(session.send(prompt).accepted, "message not accepted")
        answer = finish(stream, tracker)
        require(stream.last_event_id is not None, "processed watermark missing")
    wait_idle(session)
    return answer


def run(info, output_dir, command=None):
    """普通session/归档offload宿主；可选受控指令只用于合成模型放行及撤权。"""
    require(info.get("manifestRevision") == 7, "fixture differs from frozen revision")
    family = "sdk2-offload-v1" if info["mode"].startswith("archive-offload") else "sdk1"
    result = {
        "suite": "python-session-real-serve",
        "family": family,
        "mode": info["mode"],
        "phases": [],
        "status": "running",
        "paidModel": False,
    }
    clients = []

    def api(other=False, selected=None):
        token = info["otherToken" if other else "token"]
        principal = "fixture-other" if other else "fixture-owner"
        value = Client(info["baseURL"], lambda cancel: AuthToken(token, principal), family=selected or family)
        clients.append(value)
        return value

    def phase(name):
        result["phases"].append(name)

    def make(client, identity):
        return client.create(CreateOptions(request_id=identity if client.api.family == "sdk2-offload-v1" else None))

    try:
        sessions = SessionClient(api())
        current = make(sessions, "python-session-main")
        require(sessions.resume(current.id).id == current.id, "live resume changed identity")
        require(not sessions.resume(current.id).created.resumed, "live resume invented a cold restore")
        phase("create-live-resume-original-identity")
        if family == "sdk1":
            require(current.compact()["status"] == "rejected", "empty compaction reported success")
        first = complete(current, "PY-FIRST")
        second = complete(current, "PY-SECOND")
        expected = "go-real-serve-answer" if family == "sdk1" else "go-archive-answer"
        require(expected in first and bool(second), "fixture model answer missing")
        phase("two-turn-current-terminal-with-original-turn-id")
        attached = sessions.attach(current.id)
        count = attached.history(limit=0)
        history = attached.history(limit=50)
        require(count["messages"] == [] and count["total"] >= 4, "history count-only lost zero semantics")
        require(
            count["total"] == history["total"] and history["sessionId"] == current.id,
            "history count or identity changed",
        )
        phase("attach-history-zero-and-content")
        local = CancellationToken()
        stream = attached.events(str(attached.meta().last_seq), cancel=local)
        local.cancel()
        require(rejected(lambda: next(stream)).code == "cancelled", "local cancellation not observed")
        require(attached.meta().status == "idle", "local cancellation altered remote state")
        stream.close()
        phase("local-stream-cancel-keeps-remote-idle")
        other = SessionClient(api(other=True))
        rejected(lambda: other.attach(current.id), (403, 404))
        rejected(lambda: other.resume(current.id), (403, 404))
        require(other.list().total == 0, "foreign resume created substitute session")
        opposite = "sdk2-offload-v1" if family == "sdk1" else "sdk1"
        wrong = SessionClient(api(selected=opposite))
        before = sessions.list().total
        rejected(lambda: wrong.attach(current.id))
        rejected(lambda: wrong.resume(current.id))
        require(sessions.list().total == before, "wrong family created replacement")
        phase("cross-owner-cross-family-attach-resume-no-replacement")

        if family == "sdk1":
            floor = current.meta().last_seq
            png = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+y9k8AAAAASUVORK5CYII="
            with current.events(str(floor), deadline_ms=now_ms() + 25000) as stream:
                current.send_blocks([TextBlock("PY-SNAPSHOT"), ImageBlock("image/png", png)])
                require("go-real-serve-answer" in finish(stream, TurnTracker(floor)), "image model turn failed")
            wait_idle(current)
            require("image/png" in dumps(current.history(limit=50)).decode(), "image history missing")
            before = current.history(limit=0)
            checkpoint = current.checkpoint("原始快照")
            require(checkpoint.message_count == before["total"], "checkpoint count changed history")
            require(
                checkpoint.checkpoint_id in [entry.checkpoint_id for entry in current.checkpoints()],
                "created checkpoint not listed",
            )
            original_bytes = current.export_checkpoint(checkpoint.checkpoint_id)
            imported = current.import_checkpoint(original_bytes, "导入副本")
            exported = loads(current.export_checkpoint(imported.checkpoint_id))
            require(
                exported["checkpoint"]["messages"] == loads(original_bytes)["checkpoint"]["messages"],
                "checkpoint import changed original history",
            )
            require(current.history(limit=0)["total"] == before["total"], "import altered live history")
            require(
                current.restore(imported.checkpoint_id, False)["toMessages"] == before["total"], "restore count changed"
            )
            current.delete_checkpoint(imported.checkpoint_id)
            rejected(lambda: current.export_checkpoint(imported.checkpoint_id))
            rejected(lambda: current.import_checkpoint(b"invalid-snapshot"))
            phase("image-checkpoint-export-import-restore-delete")
            history_before_media = current.history(limit=0)["total"]
            audio = "data:audio/wav;base64,UklGRi1ydXN0LXN5bnRoZXRpYy1hdWRpbw=="
            require(
                current.transcribe(TranscriptionRequest(audio, language="en"))["text"] == "rust synthetic transcript",
                "transcription differs from synthetic fixture",
            )
            speech = current.speak(SpeechRequest("one", format="wav"))
            require(speech["audio"]["mime"] == "audio/wav", "speech request did not return audio")
            require(current.history(limit=0)["total"] == history_before_media, "speech changed live history")
            compact = current.compact(CompactOptions(checkpoint=True))
            require(compact["status"] in ("compacted", "rejected", "failed"), "compaction outcome was guessed")
            phase("synthetic-stt-tts-and-compaction-status")
            original_history = current.history(limit=50)
            floor = current.meta().last_seq
            with current.events(str(floor), deadline_ms=now_ms() + 25000) as ending:
                current.close()
                require(wait_kind(ending, "session.ended")["sessionId"] == current.id, "session end identity drift")
            resumed = sessions.resume(current.id)
            require(
                resumed.id == current.id and resumed.created.resumed,
                "dormant resume did not reconstruct original session",
            )
            require(
                resumed.history(limit=50)["messages"] == original_history["messages"],
                "persisted resume changed history",
            )
            with resumed.events("9007199254740991", deadline_ms=now_ms() + 25000) as observing:
                gap = next(observing)
                require(
                    gap.kind == "server.replay.gap" and gap.raw["reason"] == "ahead_of_log",
                    "missing original replay gap",
                )
                tracker = TurnTracker.from_replay(floor)
                require(
                    tracker.observe(gap) is None and tracker.needs_reconciliation, "gap failed to invalidate tracker"
                )
                require(observing.last_event_id is None, "ahead gap retained invalid delivery cursor")
            current = resumed
            phase("dormant-resume-and-ahead-gap-explicit-reconciliation")
            blocked = make(sessions, "unused")
            floor = blocked.meta().last_seq
            with blocked.events(str(floor), deadline_ms=now_ms() + 25000) as observing:
                blocked.send("GO-BLOCK")
                wait_kind(observing, "msg.text.delta")
                target = blocked.input_capabilities()["target"]
                blocked.interrupt()
                finish(observing, TurnTracker.resume(floor, target["turnId"]), "aborted")
            blocked.close()
            phase("explicit-remote-interrupt-aborted-terminal")
            if command is not None:
                _same_turn(sessions, make, command)
                phase("concurrent-same-turn-input-idempotency-consumption-epoch")
                _questions(sessions, other, command)
                phase("question-original-ticket-options-revocation-expiry")
        current.close()
        phase("explicit-close")

        async def asynchronous():
            sync_api = api()
            async with AsyncSessionClient(sync_api) as facade:
                created = await facade.create(
                    CreateOptions(request_id="python-session-async" if family != "sdk1" else None)
                )
                floor = (await created.meta()).last_seq
                tracker = TurnTracker(floor)
                async with await created.events(str(floor), deadline_ms=now_ms() + 25000) as stream:
                    require((await created.send("PY-ASYNC-TURN")).accepted, "async message not accepted")
                    terminal = None
                    async for event in stream:
                        terminal = tracker.observe(event)
                        if terminal is not None:
                            break
                    require(terminal is not None and terminal.status == "completed", "async current turn incomplete")
                require((await created.history(limit=0))["total"] >= 2, "async history missing")
                await created.close()

        asyncio.run(asynchronous())
        phase("async-create-stream-current-turn-history-close")
        result["status"] = "passed-covered-paths"
    except BaseException as error:
        result["status"] = "failed"
        result["error"] = str(error)
        result["errorType"] = type(error).__name__
        raise
    finally:
        stopped = all([client.close(10) for client in clients])
        result["allClientResourcesStopped"] = stopped
        require(stopped, "client resource shutdown remains pending")
        path = Path(output_dir)
        path.mkdir(parents=True, exist_ok=True)
        (path / ("session-" + family + ".json")).write_text(
            json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    return result


def _same_turn(sessions, make, command):
    current = make(sessions, "unused-input")
    complete(current, "PY-BEFORE-INPUT")
    checkpoint = current.checkpoint("pre-input-generation")
    floor = current.meta().last_seq
    with current.events(str(floor), deadline_ms=now_ms() + 25000) as observing:
        current.send("GO-BLOCK")
        wait_kind(observing, "msg.text.delta")
        capabilities = current.input_capabilities()
        target = InputTarget(capabilities["target"]["historyEpoch"], capabilities["target"]["turnId"])
        original = Input(
            "python-same-turn-original", target, InputContent(text="PY-INSERTED actual consumption"), "memory"
        )
        require(capabilities["durableAck"] is False, "fixture durable setting changed")
        rejected(
            lambda: current.submit_input(Input("python-no-downgrade", target, InputContent(text="next"), "durable"))
        )
        barrier = threading.Barrier(2)

        def submit():
            barrier.wait(timeout=5)
            return current.submit_input(original)

        with ThreadPoolExecutor(max_workers=2) as pool:
            one = pool.submit(submit)
            two = pool.submit(submit)
            first, second = one.result(10), two.result(10)
        require(first == second and first["receipt"]["state"] == "accepted", "idempotent input receipt changed")
        conflict = rejected(
            lambda: current.submit_input(Input(original.input_id, target, InputContent(text="changed"), "memory"))
        )
        require(
            isinstance(conflict.detail, dict) and conflict.detail.get("domainCode") == "input_conflict",
            "input conflict lost domain detail",
        )
        command({"command": "release-model", "requestId": "python-release-input"})
        finish(observing, TurnTracker.resume(floor, target.turn_id))
    wait_idle(current)
    consumed = current.input_status(original.input_id, target)
    require(consumed["receipt"]["state"] == "consumed", "accepted input was not actually consumed")
    require(
        "PY-INSERTED actual consumption" in dumps(current.history(limit=50)).decode(),
        "consumed input missing from history",
    )
    require(
        current.submit_input(original)["receipt"] == consumed["receipt"], "ended duplicate changed original receipt"
    )
    rejected(lambda: current.submit_input(Input("new-after-end", target, InputContent(text="late"), "memory")))
    current.restore(checkpoint.checkpoint_id, False)
    rejected(lambda: current.input_status(original.input_id, target))
    rejected(lambda: current.submit_input(original))
    require(current.meta().status == "idle", "old target unexpectedly started a new turn")
    current.close()


def _questions(sessions, other, command):
    current = sessions.create()
    floor = current.meta().last_seq
    with current.events(str(floor), deadline_ms=now_ms() + 25000) as stream:
        current.send("GO-QUESTION")
        request = wait_kind(stream, "server.question.request")
        question = request["questions"][0]
        answer = Answer(question["id"], [question["options"][0]["id"]])
        ticket = request["requestId"]
        rejected(lambda: current.answer("wrong-ticket", [answer]), (404,))
        rejected(lambda: current.answer(ticket, [Answer(question["id"], ["invented-option"])]), (400,))
        command({"command": "set-auth", "requestId": "python-question-revoke", "allowed": False})
        try:
            rejected(lambda: current.answer(ticket, [answer]), (401, 403))
        finally:
            command({"command": "set-auth", "requestId": "python-question-restore", "allowed": True})
        require(current.answer(ticket, [answer]).accepted, "original question answer rejected")
    with current.events(str(floor), deadline_ms=now_ms() + 25000) as replay:
        finish(replay, TurnTracker(floor))
    wait_idle(current)
    rejected(lambda: current.answer(ticket, [answer]), (409,))
    current.close()
    expired = sessions.create()
    with expired.events(deadline_ms=now_ms() + 25000) as stream:
        expired.send("GO-QUESTION")
        request = wait_kind(stream, "server.question.request")
    time.sleep(0.75)
    question = request["questions"][0]
    rejected(
        lambda: expired.answer(request["requestId"], [Answer(question["id"], [question["options"][0]["id"]])]), (410,)
    )
    expired.close()
