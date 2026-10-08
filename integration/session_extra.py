"""现有真实 Serve 的会话差量；只验证本批未跑分支，不复制已成功全池。"""

import argparse
import asyncio
import hashlib
import json
from pathlib import Path
import sys
import threading
import time
from urllib.parse import urlsplit

from tansr_sdk import AuthToken, Client, Error
from tansr_sdk.executor import ExecutorClient, FileJournal, Runner, Tool, current_platform
from tansr_sdk.lifecycle import CancellationToken, now_ms
from tansr_sdk.session import (
    AsyncSessionClient,
    CreateOptions,
    ImageBlock,
    Input,
    InputContent,
    InputTarget,
    SessionClient,
    SpeechRequest,
    TranscriptionRequest,
    TurnTracker,
)
from tansr_sdk.strict_json import loads
from tansr_sdk.transport import HttpTransport

from integration.host import Fixture
from integration.session import _same_turn, complete, finish, rejected, require, wait_idle, wait_kind


class RecordedTransport:
    """请求计数只记录方法与路由，不记录认证或合成正文。"""

    def __init__(self):
        self.real = HttpTransport()
        self.requests = []

    def request(self, request):
        self.requests.append((request.method, urlsplit(request.url).path))
        return self.real.request(request)

    def stream(self, request):
        self.requests.append((request.method, urlsplit(request.url).path))
        return self.real.stream(request)

    def close(self, timeout=30):
        return self.real.close(timeout)


def _api(info, family="sdk1", other=False, transport=None):
    token = info["otherToken" if other else "token"]
    return Client(
        info["baseURL"],
        lambda cancel: AuthToken(token, "other" if other else "owner"),
        family=family,
        timeout=20,
        transport=transport,
    )


async def _async_turn(session, prompt):
    floor = (await session.meta()).last_seq
    tracker = TurnTracker(floor)
    async with await session.events(str(floor), deadline_ms=now_ms() + 15000) as events:
        require((await session.send(prompt)).accepted, "async turn not accepted")
        async for event in events:
            outcome = tracker.observe(event)
            if outcome is not None:
                require(outcome.status == "completed", "async terminal is not completed")
                break
        else:
            raise AssertionError("async EOF is not completion")
    until = time.monotonic() + 5
    while (await session.meta()).status == "running":
        require(time.monotonic() < until, "async turn did not become idle")
        await asyncio.sleep(0.01)


def async_lifecycle(info, report):
    family = "sdk2-offload-v1" if info["mode"] == "archive-offload" else "sdk1"
    with _api(info, family) as api:

        async def scenario():
            async with AsyncSessionClient(api) as client:
                session = await client.create(CreateOptions(request_id="py-extra-async" if family != "sdk1" else None))
                try:
                    await _async_turn(session, "PY-ASYNC-CHECKPOINT-BEFORE")
                    original = await session.history(limit=50)
                    attached = await client.attach(session.id)
                    resumed = await client.resume(session.id)
                    require(
                        attached.id == resumed.id == session.id and not resumed.created.resumed,
                        "async live attach/resume replaced session",
                    )
                    require(
                        (await attached.history(limit=50))["messages"] == original["messages"],
                        "async attachment changed original history",
                    )
                    report["phases"].append("async-attach-live-resume-original-history")
                    closure = await session.capabilities()
                    report["checkpointOperation"] = closure.operations.get("session.checkpoint.create")
                    if closure.operations.get("session.checkpoint.create") != "enabled":
                        report["notCovered"].append("offload checkpoint is not enabled by this real closure")
                    else:
                        checkpoint = await session.checkpoint("异步原始记录")
                        require(
                            checkpoint.checkpoint_id in [c.checkpoint_id for c in await session.checkpoints()],
                            "async checkpoint not listed",
                        )
                        raw = await session.export_checkpoint(checkpoint.checkpoint_id)
                        imported = await session.import_checkpoint(raw, "异步原字节导入")
                        require(
                            loads(await session.export_checkpoint(imported.checkpoint_id))["checkpoint"]["messages"]
                            == loads(raw)["checkpoint"]["messages"],
                            "async checkpoint messages changed",
                        )
                        await _async_turn(session, "PY-ASYNC-CHECKPOINT-AFTER")
                        require((await session.history(limit=0))["total"] > original["total"], "second turn absent")
                        restored = await session.restore(imported.checkpoint_id, False)
                        require(restored["toMessages"] == original["total"], "async restore changed count")
                        require(
                            (await session.history(limit=50))["messages"] == original["messages"],
                            "async restore changed history",
                        )
                        await session.delete_checkpoint(imported.checkpoint_id)
                        try:
                            await session.export_checkpoint(imported.checkpoint_id)
                        except Error as error:
                            require(error.http_status == 404, "async deleted export wrong rejection")
                        else:
                            raise AssertionError("deleted checkpoint still exports")
                        report["phases"].append("async-checkpoint-create-list-export-import-restore-delete")
                    if family == "sdk1":
                        await session.close()
                        dormant = await client.resume(session.id)
                        require(
                            dormant.id == session.id and dormant.created.resumed, "async dormant resume did not restore"
                        )
                        require(
                            (await dormant.history(limit=50))["messages"] == original["messages"],
                            "async dormant history changed",
                        )
                        session = dormant
                        report["phases"].append("async-dormant-resume-history-same-identity")
                finally:
                    await session.close()

        asyncio.run(scenario())
        require(SessionClient(api).list().total >= 1, "async closure destroyed borrowed API")


def media_disabled(info, report):
    require(info["mode"] == "session-media-disabled", "disabled fixture required")
    transport = RecordedTransport()
    with _api(info, transport=transport) as api:
        session = SessionClient(api).create()
        try:
            complete(session, "PY-TEXT-WHILE-MEDIA-DISABLED")
            before = session.history(limit=0)["total"]
            start = len(transport.requests)
            transcribe = rejected(lambda: session.transcribe(TranscriptionRequest("data:audio/wav;base64,UklGRg==")))
            speak = rejected(lambda: session.speak(SpeechRequest("synthetic")))
            writes = [path for method, path in transport.requests[start:] if method == "POST"]
            require(not writes, "disabled media issued business POST: " + repr(writes))
            require(session.history(limit=0)["total"] == before, "disabled speech inserted history")
            report["disabledMedia"] = {
                "transcribeError": transcribe.code,
                "speakError": speak.code,
                "businessPostCount": len(writes),
            }
            report["phases"].append("disabled-stt-tts-zero-business-writes-history-unchanged")
            floor = session.meta().last_seq
            with session.events(str(floor), deadline_ms=now_ms() + 15000) as events:
                png = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+y9k8AAAAASUVORK5CYII="
                require(session.send_blocks([ImageBlock("image/png", png)]).accepted, "image ingress not accepted")
                finish(events, TurnTracker(floor), "failed")
            report["phases"].append("disabled-image-202-ingress-is-failed-turn-not-success")
        finally:
            session.close()


def configuration(info, report):
    """只验现成默认配置，不宣称覆盖夹具未提供的非空平台提示词策略。"""
    missing_transport = RecordedTransport()
    with _api(info, "sdk2-offload-v1", transport=missing_transport) as offload:
        error = rejected(
            lambda: SessionClient(offload).create(CreateOptions(request_id="py-unavailable-family")), (404,)
        )
        require(
            not any(method == "POST" for method, path in missing_transport.requests),
            "unavailable family attempted create or sdk1 fallback",
        )
        report["unavailableFamilyDiscoveryStatus"] = error.http_status
        report["phases"].append("unavailable-offload-family-discovery-zero-create-no-sdk1-fallback")
    with _api(info) as api:
        session = SessionClient(api).create(CreateOptions(model="fake-main"))
        try:
            require("applicationPrompt" not in session.meta().raw, "ordinary metadata leaked explicit prompt detail")
            prompt = session.application_prompt_meta().raw["applicationPrompt"]
            require(prompt == {"policy": "fallback", "source": "none"}, "default prompt facts differ")
            complete(session, "PY-USER-TEXT-DOES-NOT-REPLACE-APPLICATION-PROMPT")
            require(
                session.application_prompt_meta().raw["applicationPrompt"] == prompt,
                "user turn altered application prompt facts",
            )
            report["applicationPrompt"] = prompt
            report["phases"].append("model-alias-create-explicit-default-prompt-readback-no-client-override")
            report["notCovered"].append(
                "nonempty platform/Serve prompt fallback and prepend: clean existing fixture has no configurable modes"
            )
        finally:
            session.close()


def offload_input(info, report):
    require(info["mode"] == "archive-offload", "offload fixture required")
    with _api(info, "sdk2-offload-v1") as api:
        session = SessionClient(api).create(CreateOptions(request_id="py-extra-input"))
        try:
            complete(session, "PY-OFFLOAD-ENDED-INPUT")
            capabilities = session.input_capabilities()
            report["inputCapabilities"] = capabilities
            # 现成宿主只有立即完成文本模型；不靠抢时间假造稳定的活动轮窗口。
            target = capabilities.get("target")
            if target:
                before = session.history(limit=0)["total"]
                error = rejected(
                    lambda: session.submit_input(
                        Input(
                            "py-offload-ended",
                            InputTarget(target["historyEpoch"], target["turnId"]),
                            InputContent(text="late"),
                            "memory",
                        )
                    )
                )
                require(session.history(limit=0)["total"] == before, "ended input altered history")
                report["endedInputError"] = error.code
                report["phases"].append("offload-ended-input-rejected-without-history-growth")
            else:
                report["phases"].append("offload-ended-session-exposes-no-active-input-target")
            report["notCovered"].append(
                "active offload insertion: existing archive-offload MockModelClient has no controlled running window; "
                "GO-BLOCK/release-model exists only in session/execution fixtures"
            )
        finally:
            session.close()


def offload_active_input(info, report, command):
    """复用原同轮断言；仅合成模型由归档派生夹具延长第二轮活动窗口。"""
    require(info["mode"] == "archive-offload" and command is not None, "controlled offload fixture required")
    evidence = []
    evidence_lock = threading.Lock()

    class InputTransport(RecordedTransport):
        def request(self, request):
            response = super().request(request)
            path = urlsplit(request.url).path
            if "/inputs" in path or path.endswith("/history"):
                item = {
                    "method": request.method,
                    "path": path,
                    "status": response.status,
                    "requestBody": loads(request.body) if request.body else None,
                    "requestBodySha256": hashlib.sha256(request.body).hexdigest() if request.body else None,
                    "response": loads(response.body),
                }
                with evidence_lock:
                    evidence.append(item)
            return response

    transport = InputTransport()
    holder = []
    report["inputEvidence"] = evidence
    with _api(info, "sdk2-offload-v1", transport=transport) as api:

        def make(sessions, unused):
            current = sessions.create(CreateOptions(request_id="py-offload-active-input"))
            holder.append(current)
            report["sessionId"] = current.id
            return current

        try:
            _same_turn(SessionClient(api), make, command)
        finally:
            for current in holder:
                current.close()

    accepted = [
        item
        for item in evidence
        if item["method"] == "POST" and item["status"] == 202 and item["response"]["receipt"]["state"] == "accepted"
    ]
    require(len(accepted) == 2, "concurrent duplicate did not have exactly two accepted receipts")
    require(accepted[0]["requestBodySha256"] == accepted[1]["requestBodySha256"], "duplicate request bytes changed")
    require(accepted[0]["response"] == accepted[1]["response"], "duplicate response changed")
    histories = [item["response"] for item in evidence if item["path"].endswith("/history") and item["status"] == 200]
    consumption = [item for item in histories if "PY-INSERTED actual consumption" in json.dumps(item)]
    require(consumption, "missing real post-consumption history")
    for history in consumption:
        serialized = json.dumps(history)
        require(serialized.count("PY-INSERTED actual consumption") == 1, "duplicate inserted history")
    require(
        not any((item["requestBody"] or {}).get("inputId") == "python-no-downgrade" for item in evidence),
        "disabled durable ACK issued a write or downgraded",
    )
    report["consumptionCount"] = 1
    report["concurrentAcceptedRequests"] = len(accepted)
    report["phases"].extend(
        [
            "offload-concurrent-same-id-same-body-identical-accepted-receipt",
            "offload-same-id-different-body-conflict-no-second-input",
            "offload-consumed-single-history-in-original-turn",
            "offload-ended-duplicate-same-consumed-receipt-new-id-rejected",
            "offload-restored-history-epoch-old-input-status-and-submit-rejected-no-new-turn",
            "offload-disabled-durable-ack-zero-write-no-memory-downgrade",
        ]
    )


def permission(info, output, report, command):
    require(info["mode"] == "execution", "real executor permission fixture required")
    scope = {key: info[key] for key in ("applicationScopeId", "endUserId", "authorizationRevision")}
    with _api(info) as api, _api(info, other=True) as foreign:
        sessions = SessionClient(api)
        old = None
        for index in range(2):
            current = sessions.create(CreateOptions(client_tools=[info["declaration"]]))
            executor = ExecutorClient(api, scope)
            calls = []

            def handler(context, arguments):
                calls.append(True)
                return {"status": "ok", "content": [{"t": "text", "text": "go-terminal-fact"}]}

            tool = Tool(info["declaration"], handler)
            workspace = {"workspaceId": "py-permission", "revision": "1"}
            registration = {
                "protocol": "sdk2-ext-v1",
                "executorId": info["executorId"],
                "platform": current_platform(),
                "workspaces": [workspace],
                "operations": ["tool.invoke"],
                "tools": [tool.registration()],
            }
            connection = executor.register(registration)
            initialized = executor.initialize(
                current.id, registration["platform"], [tool.name], current.capabilities().closure_id
            )
            binding = executor.bind(
                current.id, connection, workspace, initialized["capabilityRevision"], current.capabilities().closure_id
            )["binding"]
            journal = FileJournal(str((output / ("permission-journal-%d" % index)).resolve()))
            runner, worker = None, None
            stop = CancellationToken()
            worker_errors = []
            try:
                floor = current.meta().last_seq
                with current.events(str(floor), deadline_ms=now_ms() + 20000) as events:
                    current.send("GO-TOOL")
                    ticket = wait_kind(events, "server.permission.request")
                    require(ticket["name"] == tool.name, "question is not permission")
                    ticket_id, digest = ticket["requestId"], ticket["digest"]
                    turn_id = current.input_capabilities()["target"]["turnId"]

                    def no_execution():
                        require(not calls, "invalid permission invoked handler")
                        require(not executor.poll(connection)["operations"], "invalid permission dispatched operation")
                        require(
                            not list((output / ("permission-journal-%d" % index)).glob("*.execution.json")),
                            "invalid permission wrote durable execution",
                        )

                    if index == 0:
                        for event in events:
                            if event.kind == "tool.permission.decided":
                                require(event.raw.get("decision") != "allow", "expiry approved by default")
                            if event.kind == "server.permission.closed" and event.raw["requestId"] == ticket_id:
                                break
                        else:
                            raise AssertionError("permission expiry did not close original ticket")
                        rejected(lambda: current.permission(ticket_id, digest, "allow"), (409, 410))
                        no_execution()
                        old = (ticket_id, digest)
                        report["phases"].append("real-permission-expiry-default-deny-zero-dispatch-handler-journal")
                    else:
                        require(ticket_id != old[0], "permission ticket reused")

                        async def negatives_and_allow():
                            async with AsyncSessionClient(api) as client:
                                session = await client.attach(current.id)

                                async def denied(ticket_value, digest_value, statuses):
                                    try:
                                        await session.permission(ticket_value, digest_value, "allow")
                                    except Error as error:
                                        require(error.http_status in statuses, "unexpected permission status")
                                    else:
                                        raise AssertionError("invalid async permission allowed")

                                await asyncio.gather(
                                    denied(ticket_id, "0" * 64, (409,)), denied(old[0], old[1], (404, 409, 410))
                                )
                                rejected(
                                    lambda: foreign.call(
                                        "session.permission.decide",
                                        parameters={"id": current.id, "ticketId": ticket_id},
                                        body={"digest": digest, "verdict": "allow"},
                                    ),
                                    (403, 404),
                                )
                                no_execution()
                                command("set-auth", allowed=False)
                                try:
                                    await denied(ticket_id, digest, (401, 403))
                                finally:
                                    command("set-auth", allowed=True)
                                no_execution()
                                require(
                                    (await session.permission(ticket_id, digest, "allow")).accepted,
                                    "valid async permission not accepted",
                                )

                        asyncio.run(negatives_and_allow())
                        report["phases"].append(
                            "async-real-permission-wrong-digest-old-ticket-foreign-revoked-zero-effect"
                        )

                        def authorize(operation, cancel):
                            cancel.check()
                            require(
                                operation["sessionId"] == current.id and operation["scope"] == scope,
                                "permission runner identity drift",
                            )
                            require(operation["binding"] == binding, "permission runner binding drift")

                        runner = Runner(
                            executor,
                            registration,
                            {tool.name: tool},
                            journal,
                            authorize,
                            connection=connection,
                            on_receipt=lambda op, outcome: stop.cancel(),
                        )

                        def execute():
                            try:
                                runner.run(cancel=stop)
                            except Error as error:
                                if error.code != "cancelled":
                                    worker_errors.append(error)
                            except BaseException as error:
                                worker_errors.append(error)

                        worker = threading.Thread(target=execute, name="python-permission-once")
                        worker.start()
                        finish(events, TurnTracker.resume(floor, turn_id))
                        stop.cancel()
                        worker.join(5)
                        require(not worker.is_alive() and not worker_errors, "permission worker did not stop")
                        require(len(calls) == 1, "valid permission did not execute exactly once")
                        report["phases"].append("valid-current-async-permission-single-business-execution-terminal")
                        report["handlerCalls"] = len(calls)
                if index != 0:
                    wait_idle(current)
            finally:
                stop.cancel()
                if worker is not None:
                    worker.join(5)
                    require(not worker.is_alive(), "permission worker remains active")
                if runner is not None:
                    require(runner.close(5), "permission runner remains active")
                journal.close()
                current.close()


def run(info, output_dir, command=None, variant="async"):
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    report = {
        "suite": "python-session-remaining-real-serve",
        "mode": info["mode"],
        "variant": variant,
        "status": "running",
        "phases": [],
        "notCovered": [],
        "paidModel": False,
        "python": sys.version,
        "synthetic": ["authentication", "platform", "model"],
    }
    try:
        if variant == "async":
            async_lifecycle(info, report)
        elif variant == "media":
            media_disabled(info, report)
        elif variant == "input":
            offload_input(info, report)
        elif variant == "input-active":
            offload_active_input(info, report, command)
        elif variant == "permission":
            permission(info, output, report, command)
        elif variant == "configuration":
            configuration(info, report)
        else:
            raise ValueError("unknown session extra variant")
        report["status"] = "passed-covered-paths"
    except BaseException as error:
        report.update(status="failed", errorType=type(error).__name__, error=str(error))
        raise
    finally:
        (output / "session-extra.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--node", required=True)
    parser.add_argument("--fixture", required=True)
    parser.add_argument("--cli", required=True)
    parser.add_argument("--evidence", required=True)
    parser.add_argument("--mode", required=True)
    parser.add_argument(
        "--variant", required=True, choices=("async", "media", "input", "input-active", "permission", "configuration")
    )
    args = parser.parse_args()
    fixture = Path(args.fixture)
    provenance = json.loads(fixture.with_name("serve-fixture.provenance.json").read_text(encoding="utf-8"))
    require(hashlib.sha256(fixture.read_bytes()).hexdigest() == provenance["output"]["sha256"], "fixture hash mismatch")
    destination = Path(args.evidence)
    with Fixture(args.node, fixture, args.cli, destination / "host", args.mode) as host:
        report = run(host.info, destination / "results", host.command, args.variant)
    report["hostExited"] = True
    (destination / "results" / "session-extra.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps({"status": report["status"], "phases": report["phases"], "notCovered": report["notCovered"]}))


if __name__ == "__main__":
    main()
