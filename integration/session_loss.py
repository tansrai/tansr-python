"""真实受理失回及独立Serve重启。代理只丢回执，不伪造成功或改变原请求。"""

import hashlib
import http.client
import http.server
import json
from pathlib import Path
import socket
import threading
from urllib.parse import urlsplit

from tansr_sdk.api import AuthToken, Client
from tansr_sdk.lifecycle import now_ms
from tansr_sdk.session import CreateOptions, SessionClient, TurnTracker, WriteOptions

from .session import complete, finish, rejected, require, wait_idle, wait_kind


class AcceptedLossProxy:
    """只允许本测试拥有的回环Serve，完整读取真实成功后断开下游一次。"""

    def __init__(self, origin, operation):
        target = urlsplit(origin)
        if target.scheme != "http" or target.hostname != "127.0.0.1" or not target.port:
            raise ValueError("loss proxy requires owned loopback Serve")
        if operation not in ("create", "interrupt", "archive-create", "archive-ack", "archive-rebase"):
            raise ValueError("unsupported loss injection")
        self.records = []
        self.dropped = 0
        self._lock = threading.Lock()
        owner = self

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *_):
                pass

            def forward(self):
                self.connection.settimeout(10)
                if not self.path.startswith("/api/") or self.headers.get("transfer-encoding"):
                    self.send_error(400)
                    return
                try:
                    length = int(self.headers.get("content-length", "0"))
                except ValueError:
                    self.send_error(400)
                    return
                if not 0 <= length <= 8 * 1024 * 1024:
                    self.send_error(413)
                    return
                body = self.rfile.read(length)
                if len(body) != length:
                    self.close_connection = True
                    return
                excluded = {
                    "host",
                    "connection",
                    "content-length",
                    "transfer-encoding",
                    "proxy-authorization",
                    "proxy-connection",
                    "keep-alive",
                    "upgrade",
                }
                headers = {key: value for key, value in self.headers.items() if key.lower() not in excluded}
                headers["connection"] = "close"
                record = {
                    "method": self.command,
                    "path": self.path,
                    "requestBytes": len(body),
                    "requestSha256": hashlib.sha256(body).hexdigest(),
                    "requestKey": self.headers.get("idempotency-key"),
                    "deadline": self.headers.get("deadline"),
                }
                upstream = http.client.HTTPConnection(target.hostname, target.port, timeout=15)
                try:
                    upstream.request(self.command, self.path, body=body, headers=headers)
                    response = upstream.getresponse()
                    response_body = response.read(8 * 1024 * 1024 + 1)
                    if len(response_body) > 8 * 1024 * 1024:
                        raise RuntimeError("proxy response capacity exceeded")
                    record.update(
                        upstreamStatus=response.status,
                        responseBytes=len(response_body),
                        responseSha256=hashlib.sha256(response_body).hexdigest(),
                    )
                    create = self.command == "POST" and self.path == "/api/sessions" and response.status in (200, 201)
                    interrupt = self.command == "POST" and self.path.endswith("/interrupt") and response.status == 202
                    archive_create = (self.command == "POST" and self.path == "/api/archive/bindings"
                                      and response.status == 201)
                    archive_ack = (self.command == "POST" and self.path.endswith("/archive/acks")
                                   and response.status == 200)
                    archive_rebase = (self.command == "POST" and self.path.endswith("/archive/ack-rebases")
                                      and response.status == 200)
                    if create:
                        identity = json.loads(response_body).get("sessionId")
                        require(isinstance(identity, str) and bool(identity), "real create lacked session identity")
                        record["sessionIdSha256"] = hashlib.sha256(identity.encode("utf-8")).hexdigest()
                    if interrupt:
                        require(json.loads(response_body).get("accepted") is True, "real interrupt not accepted")
                        record["accepted"] = True
                    if archive_create:
                        require(bool(json.loads(response_body).get("bindingId")), "real binding create lacked identity")
                    if archive_ack:
                        require(json.loads(response_body).get("state") == "completed", "real ACK not completed")
                    if archive_rebase:
                        require(json.loads(response_body).get("receipt", {}).get("state") == "completed",
                                "real rebase not completed")
                    with owner._lock:
                        drop = owner.dropped == 0 and (
                            operation == "create"
                            and create
                            and response.status == 201
                            or operation == "interrupt"
                            and interrupt
                            or operation == "archive-create"
                            and archive_create
                            or operation == "archive-ack"
                            and archive_ack
                            or operation == "archive-rebase"
                            and archive_rebase
                        )
                        if drop:
                            owner.dropped += 1
                        record["responseDropped"] = drop
                        owner.records.append(record)
                    if drop:
                        self.close_connection = True
                        self.connection.shutdown(socket.SHUT_RDWR)
                        return
                    self.send_response(response.status)
                    for key, value in response.getheaders():
                        if key.lower() not in {"connection", "content-length", "transfer-encoding"}:
                            self.send_header(key, value)
                    self.send_header("content-length", str(len(response_body)))
                    self.send_header("connection", "close")
                    self.end_headers()
                    self.wfile.write(response_body)
                    self.wfile.flush()
                    self.close_connection = True
                except Exception as error:
                    record["proxyError"] = type(error).__name__
                    with owner._lock:
                        if record not in owner.records:
                            owner.records.append(record)
                    self.close_connection = True
                finally:
                    upstream.close()

            do_GET = forward
            do_POST = forward
            do_DELETE = forward

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = False
        self.base_url = "http://127.0.0.1:" + str(self.server.server_address[1])
        self.thread = threading.Thread(target=self.server.serve_forever, kwargs={"poll_interval": 0.05})
        self.thread.start()
        self._stopped = False

    def close(self):
        if not self._stopped:
            self._stopped = True
            self.server.shutdown()
            self.server.server_close()
            self.thread.join(5)
        require(not self.thread.is_alive(), "loss proxy listener leaked")
        require(not any("proxyError" in row for row in self.records), "proxy error invalidated acceptance proof")
        return {"records": self.records, "responsesDropped": self.dropped, "listenerStopped": True}

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


def _api(info, family, base_url=None):
    token = info["token"]
    return Client(base_url or info["baseURL"], lambda cancel: AuthToken(token, "fixture-owner"), family=family)


def _write_receipt(output_dir, name, result):
    directory = Path(output_dir)
    directory.mkdir(parents=True, exist_ok=True)
    (directory / (name + ".json")).write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def run_loss(info, output_dir, offload_header=False):
    """session跑SDK1创建及中断失回；archive-offload跑原创建身份恢复。"""
    family = "sdk2-offload-v1" if info["mode"].startswith("archive-offload") else "sdk1"
    result = {
        "suite": "python-session-real-acceptance-loss",
        "family": family,
        "phases": [],
        "status": "running",
        "paidModel": False,
        "proxies": [],
        "offloadHeaderReceiptReplay": offload_header,
    }
    clients = []
    try:
        direct = _api(info, family)
        clients.append(direct)
        sessions = SessionClient(direct)
        before = sessions.list().total
        with AcceptedLossProxy(info["baseURL"], "create") as proxy:
            proxy_receipt = {"operation": "create", "records": proxy.records}
            result["proxies"].append(proxy_receipt)
            losing = _api(info, family, proxy.base_url)
            clients.append(losing)
            creating = SessionClient(losing)
            original = CreateOptions(
                request_id="python-original-create-loss" if family != "sdk1" else None,
                prompt="PY-ORIGINAL-LOST-CREATE",
                write=WriteOptions(
                    request_key="python-retained-create" if family == "sdk1" or offload_header else None,
                    deadline_ms=now_ms() + 25000,
                ),
            )
            failure = rejected(lambda: creating.create(original))
            require(failure.code in ("network", "timeout"), "accepted create loss lost unknown boundary")
            result["initialCreateOutcome"] = "unknown"
            writes = [row for row in proxy.records if row["method"] == "POST"]
            require(
                len(writes) == 1 and writes[0]["upstreamStatus"] == 201 and writes[0]["responseDropped"],
                "SDK retried or guessed identity after lost create",
            )
            if family == "sdk1":
                # 这份旁路只属合成宿主审计；丢回执的SDK没有获得identity，不拿列表猜作恢复。
                require(sessions.list().total == before + 1, "lost SDK1 creation duplicated or vanished")
                result["sdk1ClientRecovery"] = "unknown-without-session-id-no-automatic-list-or-create"
                result["phases"].append("sdk1-real-201-lost-no-retry-no-list-no-replacement")
            else:
                # 同一绝对截止、requestId、key及原体，宿主显式对账；未获得任何新身份提示。
                recovered = creating.create(original)
                changed = CreateOptions(
                    request_id=original.request_id, prompt="PY-CHANGED-INTENT", write=original.write
                )
                rejected(lambda: creating.create(changed), (409,))
                writes = [row for row in proxy.records if row["method"] == "POST"]
                replay_status = 201 if offload_header else 200
                require(
                    [row["upstreamStatus"] for row in writes] == [201, replay_status, 409],
                    "original create replay/conflict drift: " + str([row["upstreamStatus"] for row in writes]),
                )
                require(
                    writes[0]["requestSha256"] == writes[1]["requestSha256"], "offload replay changed original bytes"
                )
                for key in ("requestKey", "deadline", "sessionIdSha256"):
                    require(writes[0][key] == writes[1][key], "offload replay changed " + key)
                if offload_header:
                    # 统一层键重放保原201回执；无header时才走领域requestId的200恢复。
                    require(
                        writes[0]["responseSha256"] == writes[1]["responseSha256"],
                        "keyed replay did not preserve the accepted original receipt",
                    )
                require(
                    all(
                        row["path"].startswith("/api/capabilities/sessions?")
                        for row in proxy.records
                        if row["method"] == "GET"
                    ),
                    "offload recovery guessed identity or invented a status route",
                )
                attached = sessions.attach(recovered.id)
                wait_idle(attached)
                history = attached.history(limit=50)
                require(
                    history["total"] == 2 and "PY-ORIGINAL-LOST-CREATE" in json.dumps(history),
                    "original turn duplicated or changed",
                )
                require("PY-CHANGED-INTENT" not in json.dumps(history), "conflicting intent reached history")
                complete(attached, "PY-AFTER-CREATE-RECONCILIATION")
                attached.close()
                result["phases"].append(
                    "offload-real-201-lost-original-request-bytes-" + str(replay_status) + "-same-session-409-conflict"
                )
            require(losing.close(5), "losing create client did not stop")
            proxy_receipt.update(proxy.close())

        if family == "sdk1":
            current = sessions.create()
            with current.events(deadline_ms=now_ms() + 25000) as observing:
                current.send("GO-BLOCK")
                wait_kind(observing, "msg.text.delta")
                target = current.input_capabilities()["target"]
            require(current.meta().status == "running", "local observer close interrupted remote session")
            floor = current.meta().last_seq
            with current.events(str(floor), deadline_ms=now_ms() + 25000) as observing:
                tracker = TurnTracker.resume(floor, target["turnId"])
                with AcceptedLossProxy(info["baseURL"], "interrupt") as proxy:
                    proxy_receipt = {"operation": "interrupt", "records": proxy.records}
                    result["proxies"].append(proxy_receipt)
                    losing = _api(info, family, proxy.base_url)
                    clients.append(losing)
                    retained = SessionClient(losing).attach(current.id)
                    write = WriteOptions(request_key="python-original-interrupt", deadline_ms=now_ms() + 25000)
                    failure = rejected(lambda: retained.interrupt(write))
                    require(failure.code in ("network", "timeout"), "lost interrupt response became guessed success")
                    finish(observing, tracker, "aborted")
                    wait_idle(current)
                    require(
                        retained.id == current.id and retained.meta().status == "idle",
                        "interrupt reconciliation changed original session",
                    )
                    writes = [row for row in proxy.records if row["method"] == "POST"]
                    require(
                        len(writes) == 1
                        and writes[0]["path"].endswith("/interrupt")
                        and writes[0]["accepted"] is True
                        and writes[0]["responseDropped"],
                        "interrupt loss retried or created replacement session",
                    )
                    require(writes[0]["requestKey"] == write.request_key, "interrupt changed original request key")
                    require(losing.close(5), "losing interrupt client did not stop")
                    proxy_receipt.update(proxy.close())
            current.close()
            result["phases"].append("interrupt-real-202-lost-original-session-observed-aborted-no-retry")
        result["status"] = "passed"
    except BaseException as error:
        result.update(status="failed", error=str(error), errorType=type(error).__name__)
        raise
    finally:
        result["allClientResourcesStopped"] = all([client.close(10) for client in clients])
        _write_receipt(output_dir, "session-loss-" + family, result)
        require(result["allClientResourcesStopped"], "loss client cleanup still pending")
    return result


def run_restart(host, output_dir):
    """原data目录和独立进程；宿主须提供经确认先停后启的restart入口。"""
    info = dict(host.info)
    family = "sdk2-offload-v1" if info["mode"] == "archive-offload-durable" else "sdk1"
    require(info["mode"] in ("session", "archive-offload-durable"), "restart requires persistent fixture mode")
    require(callable(getattr(host, "restart", None)), "shared fixture has no real restart support")
    result = {
        "suite": "python-session-independent-serve-restart",
        "family": family,
        "status": "running",
        "phases": [],
        "paidModel": False,
    }
    clients = []
    try:
        initial = _api(info, family)
        clients.append(initial)
        original = CreateOptions(request_id="python-offload-cold-restart" if family != "sdk1" else None)
        current = SessionClient(initial).create(original)
        complete(current, "PY-BEFORE-INDEPENDENT-RESTART")
        checkpoint = current.checkpoint("python-independent-restart")
        checkpoint_bytes = current.export_checkpoint(checkpoint.checkpoint_id)
        history = current.history(limit=50)
        processed = str(current.meta().last_seq)
        if family == "sdk1":
            with current.events(processed, deadline_ms=now_ms() + 25000) as stream:
                current.close()
                wait_kind(stream, "session.ended")
                processed = stream.last_event_id
            require(processed is not None, "processed session terminal cursor missing")
        require(initial.close(5), "old API sockets still active before restart")
        old_pid = host.process.pid
        next_info = host.restart()
        require(
            host.process.pid != old_pid and next_info["baseURL"] != info["baseURL"],
            "restart reused old process or endpoint",
        )
        result["processes"] = [old_pid, host.process.pid]
        result["sameDataDirectory"] = True
        restarted = _api(next_info, family)
        clients.append(restarted)
        sessions = SessionClient(restarted)
        if family == "sdk1":
            require(sessions.attach(current.id).meta().live is False, "restarted host retained live old runtime")
            resumed = sessions.resume(current.id)
            require(
                resumed.created.resumed and resumed.created.last_seq > int(processed), "original sequence not resumed"
            )
        else:
            require(next_info.get("persistenceMode") == "reopen", "offload store was recreated")
            for key in ("sourceId", "sourceGeneration", "seedSha256", "coldStore"):
                require(next_info.get(key) == info.get(key), "offload source identity changed: " + key)
            resumed = sessions.create(original)
            require(resumed.created.resumed, "offload create did not resume original identity")
        require(resumed.id == current.id, "restart created replacement session")
        require(resumed.history(limit=50)["messages"] == history["messages"], "restart changed persisted history")
        require(
            resumed.export_checkpoint(checkpoint.checkpoint_id) == checkpoint_bytes,
            "restart changed original checkpoint bytes",
        )
        with resumed.events(processed, deadline_ms=now_ms() + 25000) as replay:
            event = next(replay)
            if family == "sdk1":
                require(
                    event.kind != "server.replay.gap" and int(replay.last_event_id) > int(processed),
                    "SDK1 processed watermark did not advance",
                )
            else:
                tracker = TurnTracker.from_replay(int(processed))
                require(
                    event.kind == "server.replay.gap" and event.raw.get("reason") == "evicted",
                    "offload retention gap was not exposed",
                )
                require(
                    tracker.observe(event) is None and tracker.needs_reconciliation,
                    "offload gap did not invalidate current-turn inference",
                )
                require(replay.last_event_id == processed, "evicted gap guessed a new cursor")
        complete(resumed, "PY-AFTER-INDEPENDENT-RESTART")
        require(resumed.history(limit=0)["total"] == history["total"] + 2, "restart replayed an old model turn")
        require(sessions.list().total == 1, "restart created substitute session")
        resumed.close()
        result["phases"].append("independent-process-original-history-checkpoint-bytes-watermark-next-turn")
        result["status"] = "passed"
    except BaseException as error:
        result.update(status="failed", error=str(error), errorType=type(error).__name__)
        raise
    finally:
        result["allClientResourcesStopped"] = all([client.close(10) for client in clients])
        _write_receipt(output_dir, "session-restart-" + family, result)
        require(result["allClientResourcesStopped"], "restart client cleanup still pending")
    return result
