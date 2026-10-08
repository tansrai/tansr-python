"""独立 Demo 模块进程消费真实 Serve；凭据、模型及业务材料均为合成数据。"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import threading
import time
import uuid

from integration.host import Fixture


def require(value, message):
    if not value:
        raise AssertionError(message)


def identity(prefix):
    return "py-demo-" + prefix + "-" + uuid.uuid4().hex


class Child:
    """只拥有本次启动的进程；有界日志读者，不通过关闭 stdin 假装业务完成。"""

    def __init__(self, runner, name, module, arguments):
        self.output = runner.output / name
        self.output.mkdir()
        self.condition = threading.Condition()
        self.text = ""
        self.overflow = False
        self.error = None
        self.stderr = (self.output / "stderr.log").open("wb")
        self.command = [runner.python, "-u", "-B", "-m", module] + list(arguments)
        self.process = subprocess.Popen(
            self.command, cwd=str(runner.working), env=runner.environment,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.stderr,
            text=True, encoding="utf-8", errors="strict", bufsize=1,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        self.pid = self.process.pid
        self.forced = False
        runner.children.append(self)

        def read():
            try:
                while True:
                    part = self.process.stdout.read(1)
                    if not part:
                        break
                    with self.condition:
                        if len(self.text) < 2 * 1024 * 1024:
                            self.text += part
                        else:
                            self.overflow = True
                        self.condition.notify_all()
            except BaseException as error:
                self.error = type(error).__name__
            finally:
                with self.condition:
                    self.condition.notify_all()

        self.reader = threading.Thread(target=read, name="python-demo-observer")
        self.reader.start()

    def send(self, line):
        require(self.process.poll() is None, "Demo exited before input")
        self.process.stdin.write(line + "\n")
        self.process.stdin.flush()

    def wait(self, needle, timeout=25):
        end = time.monotonic() + timeout
        with self.condition:
            while needle not in self.text:
                require(not self.overflow and self.error is None, "Demo output collector failed")
                require(self.process.poll() is None, "Demo exited before " + needle)
                remaining = end - time.monotonic()
                require(remaining > 0, "Demo output deadline: " + needle)
                self.condition.wait(min(remaining, 0.1))
        return self.text

    def session_id(self):
        self.wait("session: ")
        end = time.monotonic() + 10
        while True:
            match = re.search(r"session: ([^\r\n]+)\r?\n", self.text)
            if match:
                return match.group(1)
            require(time.monotonic() < end and self.process.poll() is None, "session identity missing")
            time.sleep(0.01)

    def finish(self, expected=0, timeout=35):
        try:
            result = self.process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.forced = True
            self.process.kill()
            self.process.wait(timeout=5)
            raise AssertionError("Demo did not exit after its original operation")
        finally:
            self.close()
        require(result == expected, "Demo exit %d, expected %d: %s" % (result, expected, self.output.name))
        require(not self.overflow and self.error is None, "Demo output collection incomplete")
        return self.text

    def close(self):
        if self.process.poll() is None:
            self.forced = True
            self.process.kill()
            self.process.wait(timeout=5)
        self.reader.join(5)
        require(not self.reader.is_alive(), "Demo output thread did not exit")
        if not self.process.stdin.closed:
            self.process.stdin.close()
        if not self.process.stdout.closed:
            self.process.stdout.close()
        if not self.stderr.closed:
            self.stderr.close()
        (self.output / "stdout.log").write_text(self.text, encoding="utf-8")
        (self.output / "process.json").write_text(json.dumps({
            "pid": self.pid, "exitCode": self.process.returncode, "forcedCleanup": self.forced,
            "module": self.command[4], "command": self.command,
            "stdoutSha256": hashlib.sha256(self.text.encode("utf-8")).hexdigest(),
        }, indent=2) + "\n", encoding="utf-8")


class Runner:
    def __init__(self, args, working):
        self.args, self.python, self.working = args, str(args.python), Path(working)
        self.output, self.children = args.output, []
        self.environment = {key: value for key, value in os.environ.items()
                            if not any(word in key.upper() for word in
                                       ("TOKEN", "SECRET", "PASSWORD", "API_KEY", "GROWTH", "PROXY", "TANSR"))}
        self.environment.pop("PYTHONPATH", None)
        self.environment.pop("PYTHONHOME", None)
        self.environment.update(PYTHONIOENCODING="utf-8", PYTHONDONTWRITEBYTECODE="1")
        if args.source_root:
            self.environment["PYTHONPATH"] = os.pathsep.join((str(args.source_root / "src"),
                                                             str(args.source_root / "demo/src")))

    def child(self, name, module, arguments):
        return Child(self, name, module, arguments)

    def fixture(self, name, mode):
        return Fixture(self.args.node, self.args.fixture, self.args.cli_root, self.output / name, mode)

    def arguments(self, fixture, name, family="sdk1"):
        from tansr_sdk.storage import PrivateDirectory
        from tansr_sdk import strict_json
        directory = self.working / (name + "-auth")
        with PrivateDirectory(str(directory), create=True) as private:
            private.write("token", fixture.info["token"].encode("ascii"))
            scope = {key: fixture.info[key] for key in
                     ("applicationScopeId", "endUserId", "authorizationRevision")}
            private.write("scope.json", strict_json.dumps(scope))
        state = self.working / (name + "-state")
        with PrivateDirectory(str(state), create=True) as private:
            private.write("key", b"71" * 32)
        return ["--base", fixture.info["baseURL"], "--family", family,
                "--token-file", str(directory / "token"), "--scope-file", str(directory / "scope.json"),
                "--timeout", "90", "--request-timeout", "20"], state

    def command(self, name, module, arguments, expected=0):
        return self.child(name, module, arguments).finish(expected)

    def close(self):
        for child in self.children:
            child.close()


def public_client(info, family="sdk1"):
    from tansr_sdk import AuthToken, Client
    return Client(info["baseURL"], lambda cancel: AuthToken(info["token"], "demo-verifier"), family)


def chat(runner):
    from tansr_sdk.session import SessionClient
    results = []
    with runner.fixture("host-chat", "session") as fixture:
        arguments, _ = runner.arguments(fixture, "chat-credentials")
        child = runner.child("chat-interactive", "tansr_demo.chat", arguments)
        session_id = child.session_id()
        child.send("PY-DEMO-FIRST")
        child.wait("[turn completed]")
        require("go-real-serve-answer" in child.text, "real model answer missing")
        child.send("/history")
        child.wait('"messages"')
        child.send("/quit")
        child.finish()
        with public_client(fixture.info) as api:
            session = SessionClient(api).attach(session_id)
            first = session.history(limit=50)
            require(first["total"] >= 2 and session.meta().status == "idle", "quit altered remote history/state")
            text = runner.command("chat-async-resume", "tansr_demo.async_chat",
                                  arguments + ["--resume", session_id, "--message", "PY-DEMO-ASYNC"])
            require("[turn completed]" in text and "session: " + session_id in text,
                    "async Demo did not resume the original session")
            require(session.history(limit=50)["total"] >= first["total"] + 2, "async turn not persisted")
            session.close()
        results.append(dict(name="chat-sync-and-async-original-session", passed=True))
    return results


def manual_archive(runner):
    from tansr_sdk.archive import ArchiveClient
    with runner.fixture("host-archive-manual", "archive-manual") as fixture:
        arguments, private = runner.arguments(fixture, "manual-credentials")
        intent = private / "binding-intent.json"
        runner.command("archive-prepare", "tansr_demo.archive", arguments + [
            "--mode", "prepare-create", "--session", fixture.info["sessionId"],
            "--source", fixture.info["sourceId"], "--request-id", identity("binding"), "--intent", str(intent)])
        with public_client(fixture.info) as api:
            archive = ArchiveClient(api)
            require(archive.binding_target(fixture.info["sessionId"])["bindingId"] is None,
                    "prepare created a binding")
            first = runner.command("archive-create", "tansr_demo.archive",
                                   arguments + ["--mode", "create", "--intent", str(intent)])
            replay = runner.command("archive-create-replay", "tansr_demo.archive",
                                    arguments + ["--mode", "create", "--intent", str(intent)])
            require(first == replay and first.startswith("binding: "), "creation replay changed binding")
            text = runner.command("archive-creation-status", "tansr_demo.archive",
                                  arguments + ["--mode", "creation-status", "--intent", str(intent)])
            require("creation state: completed" in text, "original creation status unconfirmed")
    return dict(name="archive-manual-private-intent-cold-create-replay", passed=True)


def chat_controls(runner):
    from tansr_sdk import strict_json
    from tansr_sdk.session import SessionClient, InputTarget
    with runner.fixture("host-chat-controls", "session") as fixture:
        arguments, _ = runner.arguments(fixture, "control-credentials")
        with public_client(fixture.info) as api:
            interrupted = runner.child("chat-interrupt", "tansr_demo.chat", arguments)
            interrupted_id = interrupted.session_id()
            interrupted.send("GO-BLOCK")
            interrupted.wait("go-waiting")
            interrupted.send("/interrupt")
            text = interrupted.finish(expected=1)
            require("interruption accepted" in text and "[turn aborted]" in text,
                    "explicit interruption lacked original aborted terminal")
            require("[turn completed]" not in text, "aborted turn was presented as completed")
            stopped = SessionClient(api).attach(interrupted_id)
            require(stopped.meta().status == "idle", "interrupt did not end the actual remote turn")
            stopped.close()

            child = runner.child("chat-insertion", "tansr_demo.chat", arguments)
            session_id = child.session_id()
            child.send("GO-BLOCK")
            child.wait("go-waiting")
            session = SessionClient(api).attach(session_id)
            target = session.input_capabilities()["target"]
            input_id = identity("input")
            request = dict(inputId=input_id, target=target, content=dict(text="PY-DEMO-INSERTED"), ack="memory")
            child.send("/insert " + strict_json.dumps(request).decode("utf-8"))
            child.wait("input acknowledged; acknowledgement is not core consumption")
            require(session.input_status(input_id, InputTarget(target["historyEpoch"], target["turnId"]))[
                "receipt"]["state"] == "accepted", "Demo input was not originally accepted")
            fixture.command("release-model")
            child.wait("[turn completed]")
            child.send("/quit")
            child.finish()
            require(session.input_status(input_id, InputTarget(target["historyEpoch"], target["turnId"]))[
                "receipt"]["state"] == "consumed", "accepted Demo input was not actually consumed")
            require("PY-DEMO-INSERTED" in strict_json.dumps(session.history(limit=50)).decode("utf-8"),
                    "consumed Demo input missing from original history")
            session.close()
    return dict(name="chat-original-turn-insertion-and-explicit-interrupt", passed=True,
                interruptionExitCode=1, insertionTarget=target)


def archive_family(runner, family):
    from tansr_sdk import strict_json
    from tansr_sdk.archive import ArchiveClient, FileStore, SavedIntent, identity_from_binding
    from tansr_sdk.lifecycle import now_ms
    from tansr_sdk.session import SessionClient

    label = "offload" if family != "sdk1" else "sdk1"
    mode = "archive-offload" if family != "sdk1" else "archive"
    with runner.fixture("host-" + label, mode) as fixture:
        arguments, private = runner.arguments(fixture, label + "-credentials", family)
        create = ["--request-id", identity("create")] if family != "sdk1" else []
        text = runner.command(label + "-chat", "tansr_demo.chat",
                              arguments + create + ["--message", "PY-DEMO-ARCHIVE"])
        found = re.search(r"session: ([^\r\n]+)", text)
        require(found and "[turn completed]" in text and "go-archive-answer" in text, "archive chat failed")
        session_id = found.group(1)
        with public_client(fixture.info, family) as api:
            archive = ArchiveClient(api)
            session = SessionClient(api).attach(session_id)
            target = archive.binding_target(session_id)
            binding = archive.binding(target["bindingId"])
            status = archive.status(binding["bindingId"])
            subject = identity_from_binding(binding, status)
            page = archive.records(binding)
            require(bool(page["records"]), "archive contained no original records")
            bodies = {}
            for record in page["records"]:
                for ref in [record["payload"]] + record["attachments"]:
                    if ref["artifactId"] not in bodies:
                        bodies[ref["artifactId"]] = archive.artifact(binding, ref)
            store_path = private / "archive.bin"
            with FileStore(str(store_path), b"q" * 32, "demo-key", subject, lambda value: require(
                    value == subject, "archive owner changed")) as store:
                original = dict(requestId=identity("ack"), operationEpoch=binding["operationEpoch"]["id"])
                ack = store.receive(binding, status, page, bodies, original, now_ms() + 60000)
                receipt = archive.acknowledge(ack, deadline_ms=store.pending_deadline())
                require(receipt["state"] == "completed", "real ACK was not accepted")
                require(store.pending() == ack and store.coverage() is None, "seed accidentally confirmed locally")
            store_args = ["--binding", binding["bindingId"], "--file", str(store_path),
                          "--key-file", str(private / "key"), "--key-id", "demo-key"]
            text = runner.command(label + "-recover", "tansr_demo.archive", arguments + [
                "--mode", "recover", "--request-id", identity("unused-recovery")] + store_args)
            require("pending ACK confirmed" in text, "independent process did not recover pending ACK")
            with FileStore(str(store_path), b"q" * 32, "demo-key", subject, lambda value: require(
                    value == subject, "archive owner changed")) as store:
                require(store.pending() is None and store.coverage() == ack["coverage"], "recovered coverage differs")
                for record in page["records"]:
                    require(store.body(record["payload"]) == bodies[record["payload"]["artifactId"]],
                            "independent process altered original payload")
            text = runner.command(label + "-sync", "tansr_demo.archive", arguments + ["--mode", "sync"] + store_args)
            require("archive synchronized" in text, "sync did not reach the original complete page")
            intent = private / "material.json"
            material_id = identity("material")
            response_id = identity("response")
            child = runner.child(label + "-materials", "tansr_demo.archive", arguments + [
                "--mode", "materials", "--request-id", response_id, "--intent", str(intent)] + store_args)
            child.wait("waiting for one material request")
            fixture.command("request-materials", subject=dict(endUserId="go-user", sessionId=session_id),
                            request=dict(materialRequestId=material_id, purpose="context-recall",
                                         recordIds=[record["recordId"] for record in page["records"]]))
            text = child.finish()
            require("material state: received" in text, "CLI material ingress did not receive original payload")
            text = runner.command(label + "-material-unconsumed", "tansr_demo.archive", arguments + [
                "--mode", "material-status", "--intent", str(intent)], expected=1)
            require("material state: received" in text, "received was falsely declared consumed")
            fixture.command("enqueue-materials", subject=dict(endUserId="go-user", sessionId=session_id),
                            request=dict(materialRequestId=material_id, leaseId=identity("lease")))
            runner.command(label + "-consume-chat", "tansr_demo.chat", arguments + [
                "--attach", session_id, "--message", "PY-DEMO-CONSUME-MATERIAL"])
            text = runner.command(label + "-material-consumed", "tansr_demo.archive", arguments + [
                "--mode", "material-status", "--intent", str(intent)])
            require("material state: core-consumed" in text, "actual core did not consume material")
            from tansr_sdk.storage import PrivateDirectory
            with PrivateDirectory(str(private)) as directory:
                saved = SavedIntent.load(directory, intent.name)
                require(saved.body["request"]["requestId"] == response_id, "material identity was replaced")
                body_hash = hashlib.sha256(strict_json.dumps(saved.body)).hexdigest()
            runner.command(label + "-sync-after-material", "tansr_demo.archive", arguments + ["--mode", "sync"] + store_args)
            session.close()
    return dict(name="archive-" + label, passed=True, coldAckRecovery=True,
                pendingWindow="real server accepted ACK; local durable confirmation intentionally not yet applied",
                separateCliProcesses=True, materialReceivedThenCoreConsumed=True, originalMaterialBodySha256=body_hash)


def _proxy_arguments(arguments, proxy):
    result = list(arguments)
    result[result.index("--base") + 1] = proxy.base_url
    return result


def _same_replay(proxy, suffix, original=None):
    rows = [row for row in proxy.records if row["method"] == "POST" and row["path"].endswith(suffix)]
    require(len(rows) == 2 and rows[0]["responseDropped"] and not rows[1]["responseDropped"],
            "expected exactly one original accepted loss and one explicit replay")
    fields = ("method", "path", "requestBytes", "requestSha256", "requestKey", "deadline")
    require(all(rows[0][key] == rows[1][key] for key in fields), "cold replay changed original request")
    if original is not None:
        from tansr_sdk import canonical
        require(rows[0]["requestSha256"] == hashlib.sha256(canonical.encode(original)).hexdigest(),
                "network request differs from durable original intent")
        require(rows[0]["requestKey"] == original["request"]["requestId"], "request key was not original")
    return [{key: row[key] for key in fields + ("upstreamStatus", "responseDropped")} for row in rows]


def creation_loss(runner, family):
    from integration.session_loss import AcceptedLossProxy
    from tansr_sdk.archive import ArchiveClient, SavedIntent, identity_from_binding
    from tansr_sdk.storage import PrivateDirectory
    from tansr_sdk.session import SessionClient
    label = "offload" if family != "sdk1" else "sdk1"
    mode = "archive-offload" if family != "sdk1" else "archive-manual"
    with runner.fixture("host-creation-loss-" + label, mode) as fixture:
        arguments, private = runner.arguments(fixture, "creation-loss-" + label, family)
        intent = private / "creation.json"
        with AcceptedLossProxy(fixture.info["baseURL"], "create" if family != "sdk1" else "archive-create") as proxy:
            routed = _proxy_arguments(arguments, proxy)
            if family == "sdk1":
                module = "tansr_demo.archive"
                prepare = ["--mode", "prepare-create", "--session", fixture.info["sessionId"],
                           "--source", fixture.info["sourceId"], "--request-id", identity("create-loss"),
                           "--intent", str(intent)]
                submit = ["--mode", "create", "--intent", str(intent)]
            else:
                module = "tansr_demo.chat"
                prepare = ["--prepare-create", str(intent), "--request-id", identity("create-loss")]
                submit = ["--create-intent", str(intent)]
            runner.command("creation-loss-" + label + "-prepare", module, routed + prepare)
            original_hash = hashlib.sha256(intent.read_bytes()).hexdigest()
            require(not any(row["method"] == "POST" for row in proxy.records), "prepare sent a mutation")
            lost = runner.command("creation-loss-" + label + "-lost", module, routed + submit, expected=1)
            require(proxy.dropped == 1 and "binding: " not in lost and "session: " not in lost,
                    "unknown creation was reported successful")
            with public_client(fixture.info, family) as api:
                archive, sessions = ArchiveClient(api), SessionClient(api)
                if family == "sdk1":
                    session_id = fixture.info["sessionId"]
                else:
                    listing = sessions.list()
                    require(listing.total == 1, "lost offload create did not create exactly one original session")
                    session_id = listing.sessions[0].session_id
                target = archive.binding_target(session_id)
                before = archive.binding(target["bindingId"])
                before_subject = identity_from_binding(before, archive.status(before["bindingId"]))
                replay = runner.command("creation-loss-" + label + "-replay", module, routed + submit)
                require(("binding: " + before["bindingId"] if family == "sdk1" else "session: " + session_id) in replay,
                        "new Demo process did not recover the original identity")
                after = archive.binding(before["bindingId"])
                for key in ("bindingId", "target", "sourceId", "revision"):
                    require(before[key] == after[key], "creation replay changed " + key)
                require(identity_from_binding(after, archive.status(after["bindingId"])) == before_subject,
                        "creation replay changed original source identity or generation")
                require(archive.binding_target(session_id) == target, "creation replay changed target")
                require(hashlib.sha256(intent.read_bytes()).hexdigest() == original_hash, "creation intent was rewritten")
                original = None
                if family == "sdk1":
                    with PrivateDirectory(str(private)) as directory:
                        original = SavedIntent.load(directory, intent.name).body
                rows = _same_replay(proxy, "/api/archive/bindings" if family == "sdk1" else "/api/sessions", original)
                sessions.attach(session_id).close()
            receipt = proxy.close()
        (runner.output / ("creation-loss-" + label + ".json")).write_text(
            json.dumps(dict(originalIntentSha256=original_hash, originalRequests=rows, proxy=receipt), indent=2) + "\n",
            encoding="utf-8")
    return dict(name="demo-creation-accepted-loss-" + label, passed=True, requests=rows)


def archive_delivery(runner, family):
    from integration.session_loss import AcceptedLossProxy
    from tansr_sdk.archive import ArchiveClient, FileStore, identity_from_binding
    from tansr_sdk.lifecycle import now_ms
    from tansr_sdk.session import SessionClient
    label = "offload" if family != "sdk1" else "sdk1"
    mode = "archive-offload" if family != "sdk1" else "archive"
    proofs = []
    with runner.fixture("host-delivery-" + label, mode) as fixture:
        arguments, private = runner.arguments(fixture, "delivery-" + label, family)
        create = ["--request-id", identity("delivery")] if family != "sdk1" else []
        text = runner.command("delivery-" + label + "-chat", "tansr_demo.chat",
                              arguments + create + ["--message", "PY-DEMO-DELIVERY"])
        session_id = re.search(r"session: ([^\r\n]+)", text).group(1)
        with public_client(fixture.info, family) as api:
            archive, session = ArchiveClient(api), SessionClient(api).attach(session_id)
            binding = archive.binding(archive.binding_target(session_id)["bindingId"])
            subject = identity_from_binding(binding, archive.status(binding["bindingId"]))
            path = private / "archive.bin"
            store_args = ["--binding", binding["bindingId"], "--file", str(path),
                          "--key-file", str(private / "key"), "--key-id", "demo-key"]

            def store():
                return FileStore(str(path), b"q" * 32, "demo-key", subject,
                                 lambda value: require(value == subject, "archive principal changed"))

            with AcceptedLossProxy(fixture.info["baseURL"], "archive-ack") as proxy:
                routed = _proxy_arguments(arguments, proxy)
                text = runner.command("delivery-" + label + "-ack-lost", "tansr_demo.archive",
                                      routed + ["--mode", "sync"] + store_args, expected=1)
                require(proxy.dropped == 1 and "archive synchronized" not in text, "ACK loss was reported successful")
                with store() as original_store:
                    ack, original_deadline = original_store.pending(), original_store.pending_deadline()
                    require(ack is not None and original_store.coverage() is None, "ACK loss changed durable truth")
                require(archive.status(binding["bindingId"])["acknowledgedCoverage"] == ack["coverage"],
                        "original ACK was not actually accepted by Serve")
                runner.command("delivery-" + label + "-ack-cold-recover", "tansr_demo.archive", routed + [
                    "--mode", "recover", "--request-id", identity("must-not-replace")] + store_args)
                with store() as restored:
                    require(restored.pending() is None and restored.coverage() == ack["coverage"], "cold ACK did not reconcile")
                rows = _same_replay(proxy, "/archive/acks", ack)
                proofs.append(dict(operation="ack", originalDeadlineMs=original_deadline,
                                   originalRequests=rows, proxy=proxy.close()))

            runner.command("delivery-" + label + "-append-before-stale", "tansr_demo.chat",
                           arguments + ["--attach", session_id, "--message", "PY-DEMO-PENDING-PAGE"])
            binding = archive.binding(binding["bindingId"])
            status = archive.status(binding["bindingId"])
            with store() as pending_store:
                page = archive.records(binding, pending_store.head()["sequence"])
                require(bool(page["records"]), "new turn did not produce a stale test page")
                bodies = {}
                for record in page["records"]:
                    for ref in [record["payload"]] + record["attachments"]:
                        if ref["artifactId"] not in bodies:
                            bodies[ref["artifactId"]] = archive.artifact(binding, ref)
                previous = pending_store.receive(binding, status, page, bodies,
                    dict(requestId=identity("stale-original"), operationEpoch=binding["operationEpoch"]["id"]),
                    now_ms() + 60000)
            runner.command("delivery-" + label + "-advance-revision", "tansr_demo.chat",
                           arguments + ["--attach", session_id, "--message", "PY-DEMO-STALE-REVISION"])
            with AcceptedLossProxy(fixture.info["baseURL"], "archive-rebase") as proxy:
                routed = _proxy_arguments(arguments, proxy)
                runner.command("delivery-" + label + "-stale-no-auto-rebase", "tansr_demo.archive",
                               routed + ["--mode", "sync"] + store_args, expected=1)
                require(any(row["upstreamStatus"] == 412 for row in proxy.records), "stale request did not reach real Serve")
                require(not any(row["path"].endswith("/archive/ack-rebases") for row in proxy.records),
                        "ordinary synchronization automatically rebased")
                with store() as unchanged:
                    require(unchanged.pending() == previous and unchanged.pending_rebase() is None,
                            "stale sync rewrote original pending")
                recovery_id = identity("explicit-rebase")
                runner.command("delivery-" + label + "-rebase-lost", "tansr_demo.archive", routed + [
                    "--mode", "recover", "--request-id", recovery_id] + store_args, expected=1)
                require(proxy.dropped == 1, "rebase loss did not follow actual accepted result")
                with store() as saved:
                    original = saved.pending_rebase()
                    require(original is not None and original["previous"] == previous,
                            "rebase lost or changed previous ACK")
                    require(original["request"]["requestId"] == recovery_id, "rebase did not retain original request ID")
                    rebase_deadline = saved.pending_deadline()
                text = runner.command("delivery-" + label + "-rebase-cold-recover", "tansr_demo.archive", routed + [
                    "--mode", "recover", "--request-id", identity("unused-cold-id")] + store_args)
                require("recovery did not synchronize all remaining pages" in text, "recovery claimed full synchronization")
                with store() as restored:
                    require(restored.pending() is None and restored.pending_rebase() is None
                            and restored.coverage() == previous["coverage"], "cold rebase result did not reconcile")
                rows = _same_replay(proxy, "/archive/ack-rebases", original)
                proofs.append(dict(operation="rebase", originalDeadlineMs=rebase_deadline,
                                   originalRequests=rows, proxy=proxy.close()))
            text = runner.command("delivery-" + label + "-sync-remaining", "tansr_demo.archive",
                                  arguments + ["--mode", "sync"] + store_args)
            require("archive synchronized" in text, "remaining pages were not explicitly synchronized")
            session.close()
    (runner.output / ("delivery-" + label + ".json")).write_text(json.dumps(proofs, indent=2) + "\n", encoding="utf-8")
    return dict(name="demo-ack-loss-and-explicit-stale-rebase-" + label, passed=True, proofs=proofs)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python", type=Path, default=Path(sys.executable))
    parser.add_argument("--node", required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--cli-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--source-root", type=Path,
                        help="explicit source consumption; omit for installed SDK/Demo with empty PYTHONPATH")
    parser.add_argument("--suite", choices=("all", "chat", "archive", "delivery"), default="all")
    args = parser.parse_args()
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    if args.source_root:
        args.source_root = args.source_root.resolve()
        sys.path.insert(0, str(args.source_root / "src"))
    result = dict(status="running", python=str(args.python), sourceConsumption=bool(args.source_root),
                  realCore=True, synthetic=["credentials", "platform", "model", "business materials"],
                  paidRequests=0, phases=[])
    runner = None
    try:
        with tempfile.TemporaryDirectory(prefix="tansr-python-demo-") as working:
            runner = Runner(args, working)
            probe = subprocess.run([runner.python, "-B", "-c", "import json,sys,tansr_sdk,tansr_demo;"
                                    "print(json.dumps(dict(python=sys.version,sdk=tansr_sdk.__file__,demo=tansr_demo.__file__)))"],
                                   cwd=working, env=runner.environment, capture_output=True, encoding="utf-8", timeout=20)
            require(probe.returncode == 0, "installed/source Demo import probe failed: " + probe.stderr)
            result["loaded"] = json.loads(probe.stdout)
            try:
                if args.suite in ("all", "chat"):
                    result["phases"].extend(chat(runner))
                    result["phases"].append(chat_controls(runner))
                if args.suite in ("all", "archive"):
                    result["phases"].append(manual_archive(runner))
                    for family in ("sdk1", "sdk2-offload-v1"):
                        result["phases"].append(archive_family(runner, family))
                if args.suite in ("all", "delivery"):
                    for family in ("sdk1", "sdk2-offload-v1"):
                        result["phases"].append(creation_loss(runner, family))
                        result["phases"].append(archive_delivery(runner, family))
                result["status"] = "passed"
            finally:
                runner.close()
        result["temporaryCleanup"] = "removed"
    except BaseException as error:
        result.update(status="failed", errorType=type(error).__name__, error=str(error))
    finally:
        if runner is not None:
            result["children"] = [dict(pid=child.pid, exitCode=child.process.returncode,
                                       forcedCleanup=child.forced) for child in runner.children]
        (args.output / "receipt.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(dict(status=result["status"], phases=len(result["phases"]), output=str(args.output))))
    return 0 if result["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
