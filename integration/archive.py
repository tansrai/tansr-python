"""PY-04 真实 Serve 合成场景；无生产身份、付费模型或内核替代物。"""

import hashlib
import json
import os
from pathlib import Path
import tempfile
import subprocess
import sys
import time
import uuid
from urllib.parse import urlsplit

from tansr_sdk import strict_json
from tansr_sdk.api import AuthToken, Client
from tansr_sdk.archive import ArchiveClient, FileStore, SavedIntent, identity_from_binding
from tansr_sdk.errors import Error
from tansr_sdk.lifecycle import now_ms
from tansr_sdk.session import SessionClient, CreateOptions, WriteOptions, TurnTracker
from tansr_sdk.storage import PrivateDirectory
from tansr_sdk.transport import HttpTransport


def _id(prefix):
    return "py-archive-" + prefix + "-" + uuid.uuid4().hex


def _check(value, message):
    if not value:
        raise AssertionError(message)


class LostResponse:
    """在真实服务器成功响应之后丢回；真实 body/receipt 不作伪造。"""

    def __init__(self):
        self.real = HttpTransport()
        self.suffix = None
        self.discarded = 0
        self.body = None
        self.requests = []

    def request(self, request):
        if request.body:
            value = strict_json.loads(request.body)
            if isinstance(value, dict) and "request" in value:
                self.requests.append(
                    dict(
                        method=request.method,
                        path=urlsplit(request.url).path,
                        bodySha256=hashlib.sha256(request.body).hexdigest(),
                        bodyBytes=len(request.body),
                        deadlineMs=request.deadline_ms,
                        deadlineHeader=request.headers.get("deadline"),
                        requestKey=request.headers.get("idempotency-key"),
                        ifMatch=request.headers.get("if-match"),
                        identity=value["request"],
                    )
                )
        response = self.real.request(request)
        if self.suffix and self.suffix in request.url and 200 <= response.status < 300:
            self.suffix = None
            self.discarded += 1
            self.body = request.body
            raise Error("network", "controlled loss after actual successful response")
        return response

    def stream(self, request):
        return self.real.stream(request)

    def close(self, timeout=30):
        return self.real.close(timeout)


def _private(parent, leaf):
    path = os.path.join(parent, leaf)
    with PrivateDirectory(path, create=True):
        pass
    return path


def _open(path, identity):
    def authorize(actual):
        _check(
            actual == identity and actual["applicationScopeId"] == "go-app" and actual["endUserId"] == "go-user",
            "synthetic current principal mismatch",
        )

    return FileStore(path, b"q" * 32, "py-integration-key", identity, authorize)


def _file_hashes(paths):
    return {path: hashlib.sha256(Path(path).read_bytes()).hexdigest() for path in paths}


def _body_witness(store, records):
    refs = {}
    for record in records:
        for ref in [record["payload"]] + record["attachments"]:
            refs[ref["artifactId"]] = ref
    witness = {}
    for artifact, ref in refs.items():
        body = store.body(ref)
        digest = hashlib.sha256(body).hexdigest()
        _check(len(body) == ref["bytes"] and digest == ref["sha256"], "cold artifact bytes changed")
        witness[artifact] = dict(bytes=len(body), sha256=digest)
    return witness


def _recover_new_process(action, info, family, original, files, **facts):
    """新解释器只从原私有文件恢复；凭据经stdin传递，不进argv或证据。"""
    payload = dict(
        action=action,
        info={key: info[key] for key in ("baseURL", "token")},
        family=family,
        parentPid=os.getpid(),
        original=original,
        files=_file_hashes(files),
        facts=facts,
    )
    result = subprocess.run(
        [sys.executable, str(Path(__file__).resolve()), "--recover"],
        input=strict_json.dumps(payload),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=60,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    if result.returncode != 0:
        raise AssertionError("archive recovery child failed: " + result.stderr.decode("utf-8", "replace"))
    evidence = strict_json.loads(result.stdout)
    _check(evidence["pid"] != os.getpid() and evidence["parentPid"] == os.getpid(), "recovery reused parent")
    _check(evidence["original"] == original and evidence["replay"] == original, "child changed original HTTP request")
    _check(evidence["filesBefore"] == payload["files"], "child did not open original persisted bytes")
    evidence["exitCode"] = result.returncode
    evidence["stderr"] = result.stderr.decode("utf-8", "replace")
    return evidence


def _recover_main():
    payload = strict_json.loads(sys.stdin.buffer.read())
    _check(payload["action"] in ("creation", "ack", "material", "rebase"), "unknown child recovery action")
    original, facts = payload["original"], payload["facts"]
    files_before = _file_hashes(payload["files"])
    _check(files_before == payload["files"], "persisted bytes changed before child opened them")
    transport = LostResponse()
    info = payload["info"]
    api = Client(
        info["baseURL"],
        lambda cancel: AuthToken(info["token"], "go-app/go-user"),
        payload["family"],
        transport=transport,
    )
    client, store = ArchiveClient(api), None
    deadline = int(original["deadlineMs"])
    details = {}
    try:
        if payload["action"] == "creation":
            with PrivateDirectory(facts["root"]) as directory:
                saved = SavedIntent.load(directory, "creation.json")
                _check(saved.deadline_ms == deadline, "creation deadline replaced")
                _check(saved.body["request"] == original["identity"], "creation request identity replaced")
                receipt = client.creation_operation(facts["sessionId"], saved.body["request"], deadline_ms=deadline)
                _check(receipt["state"] == "completed", "creation operation unconfirmed")
                binding = client.binding(receipt["bindingId"], deadline_ms=deadline)
                _check(
                    binding["sourceId"] == facts["sourceId"] and binding["target"] == saved.body["target"],
                    "creation identity mismatch",
                )
                replay = client.create_binding(saved, deadline_ms=deadline)
                _check(replay["bindingId"] == binding["bindingId"], "creation replay replaced binding")
                try:
                    client.prepare_create(facts["sessionId"], facts["sourceId"], _id("must-not-create"))
                except Error:
                    pass
                else:
                    raise AssertionError("new creation replaced existing binding")
                details.update(bindingId=binding["bindingId"], savedDeadlineMs=saved.deadline_ms)
        else:
            store = _open(facts["path"], facts["identity"])
            details.update(
                storageIdentity=store.identity(),
                keyId="py-integration-key",
                keySha256=hashlib.sha256(b"q" * 32).hexdigest(),
                rawBodies=_body_witness(store, facts["records"]),
            )
            if payload["action"] == "material":
                with PrivateDirectory(facts["intentRoot"]) as directory:
                    incoming = SavedIntent.load(directory, "request.json")
                    saved = SavedIntent.load(directory, "response.json")
                    identity = strict_json.loads(directory.read("response-identity.json"))
                    _check(saved.body["request"] == identity == original["identity"], "material identity replaced")
                    _check(saved.deadline_ms == incoming.deadline_ms == deadline, "material TTL extended")
                    _check(
                        client.material_status(facts["bindingId"], facts["materialId"])["state"] == "received",
                        "ingress unexpectedly consumed",
                    )
                    _check(store.coverage() == facts["coverage"], "material ingress advanced coverage")
                    _check(client.submit_materials(saved, deadline_ms=deadline)["state"] == "received", "replay failed")
                    _check(store.coverage() == facts["coverage"], "material replay advanced coverage")
                    details.update(savedDeadlineMs=saved.deadline_ms, state="received", coverage=store.coverage())
            else:
                saved = store.pending_rebase() if payload["action"] == "rebase" else store.pending()
                _check(saved["request"] == original["identity"], "pending request identity replaced")
                _check(store.pending_deadline() == deadline, "pending deadline replaced")
                _check(saved == facts["pending"], "pending intent changed on cold open")
                operation = client.recover_pending if payload["action"] == "rebase" else client.sync_once
                recovered = operation(store, _id("unused-child-request"), deadline_ms=deadline)
                _check(
                    recovered.recovered and recovered.receipt["request"] == original["identity"],
                    "original operation was not recovered",
                )
                _check(store.pending() is None and store.pending_rebase() is None, "pending recovery not confirmed")
                _check(store.coverage() == facts["coverage"], "recovered coverage changed")
                status = client.status(facts["identity"]["bindingId"], deadline_ms=deadline)
                _check(store.coverage() == status["acknowledgedCoverage"], "coverage differs from real Serve")
                details.update(
                    savedDeadlineMs=deadline, receiptIdentity=recovered.receipt["request"], coverage=store.coverage()
                )
            _check(details["rawBodies"] == facts["rawBodies"], "cold raw byte witnesses differ")
        matching = [
            row for row in transport.requests if row["path"] == original["path"] and row["method"] == original["method"]
        ]
        _check(len(matching) == 1 and matching[0] == original, "replay changed request bytes, identity, or deadline")
        evidence = dict(
            action=payload["action"],
            pid=os.getpid(),
            # Windows venv redirector可能额外产生launcher；分别保留调用者和OS父PID。
            parentPid=payload["parentPid"],
            osParentPid=os.getppid(),
            executable=sys.executable,
            python=sys.version,
            original=original,
            replay=matching[0],
            filesBefore=files_before,
            details=details,
        )
    finally:
        if store is not None:
            store.close()
        _check(api.close(), "child API resources did not quiesce")
    sys.stdout.buffer.write(strict_json.dumps(evidence))


def _turn(session):
    before = session.meta()
    tracker = TurnTracker(before.last_seq)
    deadline = now_ms() + 25000
    texts = []
    with session.events(str(before.last_seq), deadline_ms=deadline) as stream:
        session.send(
            "PY-ARCHIVE synthetic retained material", WriteOptions(request_key=_id("turn"), deadline_ms=deadline)
        )
        for event in stream:
            if event.kind == "msg.text.delta":
                texts.append(event.raw.get("text", ""))
            outcome = tracker.observe(event)
            if outcome is not None:
                _check(outcome.status == "completed", "turn was not completed")
                break
        else:
            raise AssertionError("event EOF before current turn completion")
    until = now_ms() + 10000
    while True:
        meta = session.meta()
        if meta.status == "idle":
            break
        _check(meta.status == "running" and now_ms() < until, "archive settle deadline")
        time.sleep(0.01)
    return "".join(texts)


def _capture(client, session, path):
    target = client.binding_target(session.id)
    _check(target["bindingId"] is not None, "archive host did not bind Source")
    binding = client.binding(target["bindingId"])
    status = client.status(binding["bindingId"])
    identity = identity_from_binding(binding, status)
    page = client.records(binding)
    _check(bool(page["records"]), "turn did not publish archive records")
    bodies = {}
    for record in page["records"]:
        for ref in [record["payload"]] + record["attachments"]:
            if ref["artifactId"] not in bodies:
                bodies[ref["artifactId"]] = client.artifact(binding, ref)
    store = _open(path, identity)
    try:
        request = dict(requestId=_id("ack"), operationEpoch=binding["operationEpoch"]["id"])
        ack = store.receive(binding, status, page, bodies, request, now_ms() + 60000)
        _check(store.coverage() is None, "durable receive invented accepted coverage")
        for record in page["records"]:
            _check(store.body(record["payload"]) == bodies[record["payload"]["artifactId"]], "original bytes changed")
        return store, dict(binding=binding, identity=identity, page=page, ack=ack)
    except BaseException:
        store.close()
        raise


def _manual(client, transport, info, root):
    deadline = now_ms() + 60000
    body = client.prepare_create(info["sessionId"], info["sourceId"], _id("binding"), deadline_ms=deadline)
    with PrivateDirectory(root) as directory:
        intent = SavedIntent.save(directory, "creation.json", "binding-create", body, deadline)
        transport.suffix = "/api/archive/bindings"
        try:
            client.create_binding(intent, deadline_ms=deadline)
        except Error as error:
            _check(error.code == "network" and transport.discarded == 1, "creation loss was not after success")
        else:
            raise AssertionError("creation response loss not injected")
        _check(strict_json.loads(transport.body) == body, "creation differs from persisted body")
    evidence = _recover_new_process(
        "creation",
        info,
        "sdk1",
        transport.requests[-1],
        [os.path.join(root, "creation.json")],
        root=root,
        sessionId=info["sessionId"],
        sourceId=info["sourceId"],
    )
    return dict(creationLostResponse=True, sameCreationRecovered=True, childRecoveries=[evidence])


def _ordinary(client, transport, sessions, family, root, command, info):
    def create():
        return sessions.create(CreateOptions(request_id=_id("create") if family == "sdk2-offload-v1" else None))

    session = create()
    store = None
    stale_session = None
    child_recoveries = []
    try:
        _check("go-archive-answer" in _turn(session), "unexpected real archive model output")
        path = os.path.join(_private(root, "first"), "archive.bin")
        store, data = _capture(client, session, path)
        transport.suffix = "/archive/acks"
        try:
            client.acknowledge(data["ack"], deadline_ms=store.pending_deadline())
        except Error as error:
            _check(error.code == "network" and transport.discarded == 1, "ACK loss was not actual success")
        else:
            raise AssertionError("ACK loss not injected")
        _check(strict_json.loads(transport.body) == data["ack"], "ACK request changed")
        raw_bodies = _body_witness(store, data["page"]["records"])
        store.close()
        store = None
        child_recoveries.append(
            _recover_new_process(
                "ack",
                info,
                family,
                transport.requests[-1],
                [path],
                path=path,
                identity=data["identity"],
                pending=data["ack"],
                coverage=data["ack"]["coverage"],
                records=data["page"]["records"],
                rawBodies=raw_bodies,
            )
        )
        store = _open(path, data["identity"])
        _check(
            store.coverage() == client.status(data["binding"]["bindingId"])["acknowledgedCoverage"], "coverage differs"
        )
        store.close()
        store = _open(path, data["identity"])
        _check(store.coverage() == data["ack"]["coverage"], "confirmed cold reopen differs")
        if command is None:
            raise AssertionError("real material control callback required")
        binding = client.binding(data["binding"]["bindingId"])
        subject = dict(endUserId="go-user", sessionId=session.id)
        material_id = _id("material")
        with client.events(binding, deadline_ms=now_ms() + 20000) as events:
            command(
                dict(
                    command="request-materials",
                    subject=subject,
                    request=dict(
                        materialRequestId=material_id,
                        recordIds=[record["recordId"] for record in data["page"]["records"]],
                        purpose="context-recall",
                    ),
                )
            )
            for frame in events:
                raw = strict_json.loads(frame.data)["raw"]
                if raw["eventType"] == "material.request":
                    material = raw["payload"]
                    break
            else:
                raise AssertionError("material event missing")
        deadline = now_ms() + material["remainingTtlMs"]
        intent_root = _private(root, "intents")
        with PrivateDirectory(intent_root) as directory:
            incoming = SavedIntent.save(directory, "request.json", "material-request", material, deadline)
            identity = dict(requestId=_id("response"), operationEpoch=binding["operationEpoch"]["id"])
            directory.write("response-identity.json", strict_json.dumps(identity), replace=False)
            response = client.prepare_materials(store, incoming, identity)
            outgoing = SavedIntent.save(directory, "response.json", "material-response", response, incoming.deadline_ms)
            before = store.coverage()
            transport.suffix = "/material-responses"
            try:
                client.submit_materials(outgoing, deadline_ms=outgoing.deadline_ms)
            except Error as error:
                _check(error.code == "network" and transport.discarded == 2, "material loss not real success")
            else:
                raise AssertionError("material loss not injected")
        store.close()
        store = None
        child_recoveries.append(
            _recover_new_process(
                "material",
                info,
                family,
                transport.requests[-1],
                [path]
                + [
                    os.path.join(intent_root, name)
                    for name in ("request.json", "response.json", "response-identity.json")
                ],
                path=path,
                identity=data["identity"],
                intentRoot=intent_root,
                bindingId=binding["bindingId"],
                materialId=material_id,
                coverage=before,
                records=data["page"]["records"],
                rawBodies=raw_bodies,
            )
        )
        store = _open(path, data["identity"])
        _check(store.coverage() == before, "cold material replay advanced coverage")
        command(
            dict(
                command="enqueue-materials",
                subject=subject,
                request=dict(materialRequestId=material_id, leaseId=_id("consumer")),
            )
        )
        _check(bool(_turn(session)), "material consumption turn empty")
        until = now_ms() + 10000
        while True:
            state = client.material_status(binding["bindingId"], material_id)["state"]
            if state == "core-consumed":
                break
            _check(state in ("received", "verified") and now_ms() < until, "core consumption not observed")
            time.sleep(0.01)
        _check(client.sync_once(store, _id("after-material")).records > 0, "new material turn page missing")
        store.close()
        store = None
        session.close()
        session = None

        stale_session = create()
        _turn(stale_session)
        path = os.path.join(_private(root, "stale"), "archive.bin")
        store, data = _capture(client, stale_session, path)
        _turn(stale_session)
        try:
            client.sync_once(store, _id("must-not-rebase"))
        except Error as error:
            _check(error.wire_code == "precondition_failed", "stale rejection not observed")
        else:
            raise AssertionError("stale ACK unexpectedly accepted")
        _check(store.pending() == data["ack"] and store.pending_rebase() is None, "ordinary sync rewrote stale intent")
        transport.suffix = "/archive/ack-rebases"
        try:
            client.recover_pending(store, _id("recovery"))
        except Error as error:
            _check(error.code == "network" and transport.discarded == 3, "rebase loss not actual success")
        else:
            raise AssertionError("rebase response not discarded")
        saved_rebase = store.pending_rebase()
        _check(saved_rebase["previous"] == data["ack"], "rebase changed old ACK")
        raw_bodies = _body_witness(store, data["page"]["records"])
        store.close()
        store = None
        child_recoveries.append(
            _recover_new_process(
                "rebase",
                info,
                family,
                transport.requests[-1],
                [path],
                path=path,
                identity=data["identity"],
                pending=saved_rebase,
                coverage=data["ack"]["coverage"],
                records=data["page"]["records"],
                rawBodies=raw_bodies,
            )
        )
        store = _open(path, data["identity"])
        _check(store.coverage() == data["ack"]["coverage"] and store.pending_rebase() is None, "rebase not confirmed")
        _check(client.sync_once(store, _id("after-rebase")).records > 0, "recovery was mistaken for complete archive")
        return dict(
            ackLostColdReopen=True,
            materialReceivedThenCoreConsumed=True,
            explicitRebaseLostColdReopen=True,
            sourceRawBytesVerified=True,
            childRecoveries=child_recoveries,
        )
    finally:
        if store is not None:
            store.close()
        if session is not None:
            session.close()
        if stale_session is not None:
            stale_session.close()


def run(info, output_dir, command=None):
    """由统一宿主运行 archive-manual/archive/archive-offload；临时介质在 OS temp。"""
    mode = info["mode"]
    _check(mode in ("archive-manual", "archive", "archive-offload"), "wrong archive fixture mode")
    family = "sdk2-offload-v1" if mode == "archive-offload" else "sdk1"
    transport = LostResponse()
    api = Client(
        info["baseURL"], lambda cancel: AuthToken(info["token"], "go-app/go-user"), family, transport=transport
    )
    try:
        archive = ArchiveClient(api)
        with tempfile.TemporaryDirectory(prefix="tansr-py-real-archive-") as temp:
            root = _private(temp, "private")
            if mode == "archive-manual":
                result = _manual(archive, transport, info, root)
            else:
                result = _ordinary(archive, transport, SessionClient(api), family, root, command, info)
        result.update(
            status="passed",
            mode=mode,
            family=family,
            discardedActualResponses=transport.discarded,
            realCore=True,
            synthetic=["authentication", "model"],
        )
        output = Path(output_dir)
        output.mkdir(parents=True, exist_ok=True)
        (output / (mode + ".json")).write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        return result
    finally:
        _check(api.close(), "archive API resources did not quiesce")


if __name__ == "__main__":
    _check(sys.argv[1:] == ["--recover"], "archive child requires explicit recovery mode")
    _recover_main()
