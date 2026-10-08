"""真实 Serve 的执行器/增量输出消费；私有宿主由集中入口提供。"""
import asyncio
import json
from pathlib import Path
import threading
import time
import hashlib
import os
import queue
import subprocess
import sys
from urllib.parse import urlsplit

from tansr_sdk.api import AuthToken, Client
from tansr_sdk.errors import Error
from tansr_sdk.executor import (AsyncExecutorClient, AsyncRunner, ExecutorClient, FileJournal,
                                Runner, Tool, current_platform, definition_digest, query_output)
from tansr_sdk.lifecycle import CancellationToken, now_ms
from tansr_sdk.session import CreateOptions, SessionClient, TurnTracker
from tansr_sdk import strict_json
from tansr_sdk.transport import HttpTransport
from tansr_sdk.storage import PrivateDirectory


class LostExecutionReplies:
    """只在真实 Serve 成功后丢回各一次，保留原请求字节摘要供对账。"""
    def __init__(self):
        self.real = HttpTransport()
        self.discarded = {}
        self.paths = []
        self.before_submit = None

    def request(self, request):
        body = strict_json.loads(request.body) if request.body else {}
        kind = None
        if isinstance(body, dict) and "blocks" in body and "seal" in body:
            kind = "seal" if body["seal"] is not None else "block"
        elif isinstance(body, dict) and body.get("protocol") == "sdk2-ext-v1" and "result" in body:
            kind = "receipt"
            if self.before_submit is not None:
                self.before_submit(body)
        self.paths.append(urlsplit(request.url).path)
        response = self.real.request(request)
        if kind is not None and kind not in self.discarded and response.status == 200:
            self.discarded[kind] = hashlib.sha256(request.body).hexdigest()
            raise Error("network", "controlled successful execution response loss")
        return response

    def stream(self, request):
        return self.real.stream(request)

    def close(self, timeout=30):
        return self.real.close(timeout)


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def run(info, output_dir, command=None):
    require(info["manifestRevision"] == 7 and info.get("terminalOutput"), "execution fixture required")
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    scope = {key: info[key] for key in ("applicationScopeId", "endUserId", "authorizationRevision")}
    declaration = info["declaration"]
    require(definition_digest(declaration) == info["definitionDigest"], "Python/Serve declaration digest differs")
    report = {"suite": "python-executor-real-serve", "mode": info["mode"], "status": "running",
              "paidModel": False, "phases": [], "handlerCalls": 0, "firstBlockBeforeReturn": False}
    transport = LostExecutionReplies()
    api = Client(info["baseURL"], lambda cancel: AuthToken(info["token"], "executor-fixture-owner"),
                 timeout=20, transport=transport)
    executor = ExecutorClient(api, scope)
    sessions = SessionClient(api)
    journal = FileJournal(str((output_dir / "journal").resolve()))
    runner = None
    thread = None
    stop = CancellationToken()
    receipt_ready = threading.Event()
    stream_cancel = CancellationToken()
    receipts, errors, operation_seen = [], [], []
    session = None
    try:
        session = sessions.create(CreateOptions(client_tools=[declaration]))

        def handler(context, arguments):
            report["handlerCalls"] += 1
            require(context.output is not None, "negotiated output absent")
            context.output.stdout.write(b"python-real-first\xe4")
            observed = None
            end = time.monotonic() + 5
            while time.monotonic() < end:
                context.check()
                observed = query_output(api, terminal["session"], context.output.operation,
                                        cancel=context.cancel, deadline_ms=context.deadline_ms)
                if observed["acceptedThrough"] is not None:
                    break
                time.sleep(0.01)
            require(observed["acceptedThrough"] is not None, "first block never reached Serve during handler")
            report["firstBlockBeforeReturn"] = True
            context.output.stderr.write(b"\xb8\xad-last")
            fact = "awaiting shipment" if info["mode"] == "execution-demo" else "go-terminal-fact"
            return {"status": "ok", "content": [{"t": "text", "text": fact}]}

        tool = Tool(declaration, handler)
        workspace = {"workspaceId": "python-workspace", "revision": "1"}
        registration = {"protocol": "sdk2-ext-v1", "executorId": info["executorId"],
                        "platform": current_platform(), "workspaces": [workspace],
                        "operations": ["tool.invoke"], "tools": [tool.registration()]}
        connection = executor.register(registration)
        original_connection = connection
        connection = executor.heartbeat(connection)
        require(connection["connectionId"] == original_connection["connectionId"] and
                connection["connectionRevision"] == original_connection["connectionRevision"],
                "heartbeat changed connection generation")
        initialized = executor.initialize(session.id, registration["platform"], [tool.name],
                                          session.capabilities().closure_id)
        capabilities = executor.bind(session.id, connection, workspace, initialized["capabilityRevision"],
                                     session.capabilities().closure_id)
        require(any(item["name"] == tool.name and item["available"] for item in capabilities["effectiveTools"]),
                "explicit first binding did not enable business tool")
        terminal = executor.negotiate_output({"sessionContract": "sdk1", "sessionId": session.id},
                                              capabilities["binding"], "python-terminal-first")
        report["phases"].append("declaration-register-renew-initialize-first-binding-output-negotiation")

        def authorize(operation, cancel):
            cancel.check()
            require(operation["sessionId"] == session.id and operation["scope"] == scope,
                    "host identity changed")
            require(operation["binding"] == capabilities["binding"], "host execution binding changed")
            operation_seen[:] = [operation]

        def delivered(operation, outcome):
            receipts.append(outcome)
            receipt_ready.set()
            stop.cancel()

        runner = Runner(executor, registration, {tool.name: tool}, journal, authorize,
                        connection=connection, terminal=terminal, require_output=True, on_receipt=delivered)
        transport.before_submit = lambda body: require(
            journal.claim(operation_seen[0]).receipt == body, "receipt submitted before durable completion")

        def execute():
            try:
                runner.run(cancel=stop)
            except Error as error:
                if error.code != "cancelled":
                    errors.append(error)
                    stream_cancel.cancel()
            except BaseException as error:
                errors.append(error)
                stream_cancel.cancel()

        thread = threading.Thread(target=execute, name="python-real-executor")
        floor = session.meta().last_seq
        with session.events(str(floor), deadline_ms=now_ms() + 25000, cancel=stream_cancel) as events:
            thread.start()
            session.send("GO-TOOL")
            tracker = TurnTracker(floor)
            completed = False
            for event in events:
                if event.kind == "server.permission.request":
                    session.permission(event.raw["requestId"], event.raw["digest"], "allow")
                outcome = tracker.observe(event)
                if outcome is not None:
                    require(outcome.status == "completed", "business turn did not complete")
                    completed = True
                    break
            require(completed, "SSE EOF is not execution completion")
        if not receipt_ready.wait(5):
            if errors:
                raise errors[0]
            raise AssertionError("business receipt callback absent")
        thread.join(5)
        require(not thread.is_alive() and not errors, "execution worker did not finish cleanly")
        outcome = receipts[0]
        require(outcome.receipt["status"] == "completed", "business result was not completed")
        require(outcome.output_confirmed and outcome.output_status["state"] == "complete", "output seal was not confirmed")
        require(report["handlerCalls"] == 1 and report["firstBlockBeforeReturn"], "incremental execution was not observed")
        require(journal.claim(operation_seen[0]).receipt == outcome.receipt, "receipt was not durable")
        require(set(transport.discarded) == {"block", "seal", "receipt"}, "real response losses were not exercised")
        report["lostSuccessfulReplies"] = transport.discarded
        report["phases"].append("real-handler-incremental-first-block-seal-and-business-receipt")
        report["phases"].append("real-block-seal-receipt-success-lost-original-status-reconciliation")

        # 同一个操作通过异步入口重放原耐久事实；不重新调用业务，不新建输出流。
        replay = Runner(executor, registration, {tool.name: tool}, journal, authorize,
                        connection=runner.connection, terminal=terminal, require_output=True)
        async def async_replay():
            async with AsyncExecutorClient(executor) as async_client:
                status = await async_client.status(session.id, operation_seen[0]["operationId"])
                require(status["receipt"] == outcome.receipt, "async status changed original receipt")
            async with AsyncRunner(replay) as async_runner:
                return await async_runner.execute_with_output(operation_seen[0])
        repeated = asyncio.run(async_replay())
        require(repeated.receipt == outcome.receipt and repeated.output_confirmed and report["handlerCalls"] == 1,
                "async replay repeated side effect or lost seal")
        report["phases"].append("async-original-status-and-durable-replay-no-handler-repeat")
        restricted = ExecutorClient(api, scope, restricted=True)
        restricted_status = restricted.executor_status(terminal["session"], runner.connection, operation_seen[0])
        require(restricted_status["receipt"] == outcome.receipt, "restricted route changed original receipt")
        require(any("/api/terminal/executors/" in path for path in transport.paths), "restricted route not used")
        report["phases"].append("explicit-executor-status-route-same-operation")

        # 控制身份失效后，旧本地 journal 不恢复当前授权。
        if command is not None:
            command({"command": "set-auth", "allowed": False})
            try:
                executor.status(session.id, operation_seen[0]["operationId"])
            except Error as error:
                require(error.http_status in (401, 403), "revoked auth did not fail at Serve")
            else:
                raise AssertionError("revoked auth was accepted")
            command({"command": "set-auth", "allowed": True})
            report["phases"].append("revoked-current-identity-rejected")
        report.update(status="passed", businessStatus=outcome.receipt["status"],
                      outputState=outcome.output_status["state"], operationId=outcome.receipt["operationId"])
        return report
    except BaseException:
        report["status"] = "failed"
        if errors:
            raise errors[0]
        raise
    finally:
        stop.cancel()
        if runner is not None:
            require(runner.close(timeout=5), "runner failed to quiesce")
        if thread is not None and thread.ident is not None:
            thread.join(5)
            require(not thread.is_alive(), "executor thread leaked")
        journal.close()
        require(api.close(timeout=5), "API resources did not close")
        require(transport.close(timeout=5), "injected transport resources did not close")
        (output_dir / "executor-receipt.json").write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def run_demo(info, output_dir, command=None, source_root=None):
    """独立 Python 进程调用实际 tools Demo；仅增加 handler 返回标记用于时序观测。"""
    require(info["mode"] == "execution-demo", "execution-demo fixture required")
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    scope = {key: info[key] for key in ("applicationScopeId", "endUserId", "authorizationRevision")}
    credentials = output_dir / "credentials"
    with PrivateDirectory(str(credentials), create=True) as private:
        private.write("token.txt", info["token"].encode("ascii"))
        private.write("scope.json", strict_json.dumps(scope))
    marker = output_dir / "handler-returned.txt"
    wrapper = """import sys,time
from pathlib import Path
from tansr_demo import tools
marker=Path(sys.argv.pop(1))
original=tools.lookup
def observed(context,arguments):
 try:
  return original(context,arguments)
 finally:
  marker.write_text(str(time.monotonic()),encoding='ascii')
tools.lookup=observed
raise SystemExit(tools.main())
"""
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    if source_root is not None:
        project = Path(source_root).resolve()
        env["PYTHONPATH"] = os.pathsep.join((str(project / "src"), str(project / "demo" / "src")))
    process = subprocess.Popen([sys.executable, "-u", "-c", wrapper, str(marker),
        "--base", info["baseURL"], "--token-file", str(credentials / "token.txt"),
        "--scope-file", str(credentials / "scope.json"), "--journal", str(output_dir / "journal"),
        "--executor", info["executorId"], "--require-output", "--run-once", "--timeout", "30"],
        stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env,
        encoding="utf-8", errors="strict", creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    lines = queue.Queue(maxsize=128)
    log = []
    def drain():
        for line in process.stdout:
            log.append(line)
            lines.put_nowait(line.rstrip("\r\n"))
    reader = threading.Thread(target=drain, name="executor-demo-console")
    reader.start()
    api = Client(info["baseURL"], lambda cancel: AuthToken(info["token"], "demo-observer"))
    stop, first, errors, original_operation = threading.Event(), threading.Event(), [], []
    observer = None
    report = {"suite": "python-tools-demo-real-serve", "status": "running", "firstBlockBeforeHandlerReturn": False}
    try:
        session_id = None
        end = time.monotonic() + 15
        while time.monotonic() < end:
            try:
                line = lines.get(timeout=0.1)
            except queue.Empty:
                require(process.poll() is None, "tools Demo exited before ready")
                continue
            if line.startswith("session: "):
                session_id = line[len("session: "):]
            if line.startswith("ready: "):
                break
        require(session_id is not None and any(line.startswith("ready: ") for line in log), "tools Demo readiness absent")
        session = SessionClient(api).attach(session_id)
        executor = ExecutorClient(api, scope)
        reference = {"sessionContract": "sdk1", "sessionId": session_id}
        def observe():
            try:
                while not stop.wait(0.01):
                    response = api.call("terminal.observation.correlations", parameters={"id": session_id},
                        query={"contract": "terminal-observation-v1", "sessionContract": "sdk1"})
                    for correlation in response.body["correlations"]:
                        state = query_output(api, reference, correlation["operation"])
                        if state["acceptedThrough"] is not None and not first.is_set():
                            require(not marker.exists(), "first block arrived after Demo handler returned")
                            current = executor.status(session_id, correlation["operation"]["operationId"])
                            require(current["status"] == "pending" and state["seal"] is None,
                                    "Demo first observation already completed")
                            original_operation[:] = [current["operation"]]
                            report["firstBlockBeforeHandlerReturn"] = True
                            first.set()
            except BaseException as error:
                errors.append(error)
        observer = threading.Thread(target=observe, name="executor-demo-first-block")
        floor = session.meta().last_seq
        with session.events(str(floor), deadline_ms=now_ms() + 20000) as events:
            observer.start()
            session.send("GO-TOOL")
            tracker = TurnTracker(floor)
            for event in events:
                if event.kind == "server.permission.request":
                    session.permission(event.raw["requestId"], event.raw["digest"], "allow")
                outcome = tracker.observe(event)
                if outcome is not None:
                    require(outcome.status == "completed", "Demo business turn failed")
                    break
            else:
                raise AssertionError("Demo SSE EOF is not completion")
        process.wait(timeout=10)
        require(process.returncode == 0, "tools Demo did not exit successfully")
        require(first.is_set() and not errors, "Demo incremental observation failed")
        original = original_operation[0]
        state = query_output(api, reference, {"operationId": original["operationId"], "requestDigest": original["digest"]})
        receipt = executor.status(session_id, original["operationId"])["receipt"]
        require(receipt["status"] == "completed" and state["state"] == "complete", "Demo dual completion absent")
        with FileJournal(str(output_dir / "journal")) as journal:
            require(journal.claim(original).receipt == receipt, "Demo durable receipt differs")
        require(state["seal"]["payloadDigest"] == hashlib.sha256(
            b"order lookup started\norder lookup completed\n").hexdigest(), "Demo output byte identity changed")
        require(any("business receipt: completed; output confirmed=true" in line for line in log),
                "Demo did not present independent receipt and seal confirmation")
        report.update(status="passed", businessStatus=receipt["status"], outputState=state["state"],
                      operationId=original["operationId"], outputDigest=state["seal"]["payloadDigest"])
        return report
    finally:
        stop.set()
        if observer is not None and observer.ident is not None:
            observer.join(5)
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=10)
        reader.join(5)
        process.stdout.close()
        require(api.close(5), "Demo observer resources did not quiesce")
        (output_dir / "tools-console.log").write_text("".join(log), encoding="utf-8")
        (output_dir / "tools-demo-receipt.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    import argparse
    from host import Fixture
    parser = argparse.ArgumentParser()
    parser.add_argument("--node", required=True)
    parser.add_argument("--fixture", required=True)
    parser.add_argument("--cli-root", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--demo", action="store_true")
    parser.add_argument("--source-root", help="explicit source checkout for Demo; omitted means installed packages")
    arguments = parser.parse_args()
    root = Path(arguments.output)
    root.mkdir(parents=True, exist_ok=False)
    mode = "execution-demo" if arguments.demo else "execution"
    with Fixture(arguments.node, arguments.fixture, arguments.cli_root, root / "host", mode) as fixture:
        if arguments.demo:
            result = run_demo(fixture.info, root / "results", fixture.command, source_root=arguments.source_root)
        else:
            result = run(fixture.info, root / "results", fixture.command)
        print(json.dumps(result, ensure_ascii=False))
