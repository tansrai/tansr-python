"""Demo局部接线与失回反例；合成宿主不冒充真实Serve或原生权限验收。"""
import contextlib
import importlib
import io
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "demo/src"))
common = importlib.import_module("tansr_demo.common")
chat = importlib.import_module("tansr_demo.chat")
tools = importlib.import_module("tansr_demo.tools")
archive = importlib.import_module("tansr_demo.archive")
async_chat = importlib.import_module("tansr_demo.async_chat")
session_types = importlib.import_module("tansr_sdk.session")
sdk = importlib.import_module("tansr_sdk")
clock = importlib.import_module("tansr_sdk.lifecycle")
strict_json = importlib.import_module("tansr_sdk.strict_json")
executor = importlib.import_module("tansr_sdk.executor")


class MemoryDirectory:
    files = {}

    def __init__(self, path, **unused):
        self.path = str(path)

    def __enter__(self):
        return self

    def __exit__(self, *unused):
        self.close()

    def close(self):
        pass

    def read(self, name, max_bytes=None, **unused):
        raw = self.files[(self.path, name)]
        if max_bytes is not None and len(raw) > max_bytes:
            raise sdk.Error("capacity")
        return raw

    def write(self, name, raw, replace=True, **unused):
        if not replace and self.exists(name):
            raise sdk.Error("conflict")
        self.files[(self.path, name)] = bytes(raw)

    def exists(self, name):
        return (self.path, name) in self.files

    def lock(self, name):
        return contextlib.nullcontext()


class Host:
    def __init__(self):
        self.cancel = sdk.CancellationToken()
        self.deadline_ms = clock.now_ms() + 60000
        self.api = object()
        self.scope = {"applicationScopeId": "app", "endUserId": "user", "authorizationRevision": "1"}

    def owner(self):
        return {"base": "http://127.0.0.1:8787", "family": "sdk2-offload-v1", "scope": dict(self.scope)}

    def private_directory(self, path, **unused):
        return MemoryDirectory(str(path))

    def context(self, deadline_ms=None):
        return dict(cancel=self.cancel, deadline_ms=min(deadline_ms or self.deadline_ms, self.deadline_ms))

    def write(self, deadline_ms=None, key=None):
        return session_types.WriteOptions(request_key=key or "stable-write", **self.context(deadline_ms))


def event(kind, seq, turn="new", terminal="none", **raw):
    body = {"type": kind, "turnId": turn}
    body.update(raw)
    return session_types.SessionEvent({"type": kind, "eventId": str(seq), "terminalStatus": terminal, "raw": body})


class Stream:
    def __init__(self, values):
        self.values = iter(values)
        self.closed = False

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.values)

    def close(self):
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, *unused):
        self.close()


class Session:
    id = "session-1"

    def __init__(self, values):
        self.stream = Stream(values)
        self.calls = []

    def meta(self, **unused):
        return session_types.Meta(self.id, "idle", True, 10, {})

    def events(self, cursor, **unused):
        self.calls.append(("events", cursor))
        return self.stream

    def send(self, message, write):
        self.calls.append(("send", message))

    def permission(self, *values):
        self.calls.append(("permission",) + values)


class DemoTests(unittest.TestCase):
    def setUp(self):
        MemoryDirectory.files = {}
        self.host = Host()
        self.path = Path(tempfile.gettempdir()) / "tansr-demo-test" / "intent.json"

    def quiet(self):
        return contextlib.redirect_stdout(io.StringIO())

    def test_three_help_entries_need_no_credentials_or_network(self):
        for module in (chat, tools, archive):
            with self.subTest(module=module.__name__), self.quiet(), mock.patch.object(common, "Host") as host:
                with self.assertRaises(SystemExit) as done:
                    module.main(["--help"])
                self.assertEqual(done.exception.code, 0)
                host.assert_not_called()

    def test_conflicting_modes_fail_before_credentials(self):
        cases = [(chat, ["--resume", "s", "--attach", "s"]),
                 (chat, ["--family", "sdk2-offload-v1"]),
                 (chat, ["--attach", "s", "--model", "ignored"]),
                 (chat, ["--request-id", "sdk1-has-no-request-id"]),
                 (archive, ["--mode", "status", "--binding", "b", "--request-id", "ignored"])]
        for module, args in cases:
            with self.subTest(args=args), contextlib.redirect_stderr(io.StringIO()):
                with mock.patch.object(common, "Host") as host:
                    with self.assertRaises(SystemExit) as done:
                        module.main(args)
                    self.assertEqual(done.exception.code, 2)
                    host.assert_not_called()

    def test_current_scope_is_rechecked_and_token_is_not_logged(self):
        root = self.path.parent
        MemoryDirectory.files[(str(root), "token")] = b"secret-token\n"
        MemoryDirectory.files[(str(root), "scope")] = strict_json.dumps(self.host.scope)
        with mock.patch.object(common, "PrivateDirectory", MemoryDirectory):
            credentials = common.Credentials(root / "token", root / "scope")
            token = credentials.token(self.host.cancel)
            self.assertEqual(token.value, "secret-token")
            replacement = dict(self.host.scope, authorizationRevision="2")
            MemoryDirectory.files[(str(root), "scope")] = strict_json.dumps(replacement)
            with self.assertRaises(sdk.Error) as caught:
                credentials.token(self.host.cancel)
            self.assertEqual(caught.exception.code, "permission")
        output = io.StringIO()
        with contextlib.redirect_stderr(output):
            result = common.report(lambda: common.fail("permission"))
        self.assertEqual(result, 1)
        self.assertNotIn("secret-token", output.getvalue())

    def test_native_state_root_is_separate_and_authorization_remains_current(self):
        with tempfile.TemporaryDirectory(prefix="tansr-demo-state-boundary-") as temporary:
            root = Path(temporary)
            credentials = root / "credentials"
            scope = dict(applicationScopeId="synthetic-app", endUserId="synthetic-user", authorizationRevision="1")
            with common.PrivateDirectory(str(credentials), create=True) as directory:
                directory.write("token", b"synthetic-token")
                directory.write("scope.json", strict_json.dumps(scope))
            arguments = ["--token-file", str(credentials / "token"), "--scope-file", str(credentials / "scope.json")]
            for field in ("prepare_create", "create_intent", "intent", "file", "journal"):
                args = chat.build_parser().parse_args(arguments)
                setattr(args, field, credentials if field == "journal" else credentials / "state.json")
                with self.subTest(field=field), mock.patch.object(common, "Client") as client:
                    with self.assertRaises(sdk.Error) as caught:
                        common.Host(args)
                    self.assertEqual(caught.exception.code, "credentials_state_overlap")
                    client.assert_not_called()
            args = chat.build_parser().parse_args(arguments)
            state = root / "state" / "intent.json"
            with common.Host(args) as host:
                common.save_owned(state, host, {"original": True})
                self.assertEqual(common.load_owned(state, host), {"original": True})
                with common.PrivateDirectory(str(credentials)) as directory:
                    directory.write("scope.json", strict_json.dumps(dict(scope, authorizationRevision="2")))
                with self.assertRaises(sdk.Error) as caught:
                    common.load_owned(state, host)
                self.assertEqual(caught.exception.code, "permission")
    def test_error_formatter_never_prints_server_message_or_detail(self):
        def bad():
            raise sdk.Error("forbidden", "token-SECRET", detail={"archive": "private-BODY"})
        output = io.StringIO()
        with contextlib.redirect_stderr(output):
            self.assertEqual(common.report(bad), 1)
        self.assertNotIn("SECRET", output.getvalue())
        self.assertNotIn("private-BODY", output.getvalue())
        self.assertEqual(common.safe("a\x1b[31m\rb\x9b"), "a?[31m?b?")

    def test_owned_intent_cannot_change_owner_or_be_overwritten(self):
        original = {"requestId": "original", "deadlineMs": 123}
        common.save_owned(self.path, self.host, original)
        self.assertEqual(common.load_owned(self.path, self.host), original)
        with self.assertRaises(sdk.Error):
            common.save_owned(self.path, self.host, {"requestId": "replacement"})
        self.host.scope["endUserId"] = "other"
        with self.assertRaises(sdk.Error) as caught:
            common.load_owned(self.path, self.host)
        self.assertEqual(caught.exception.code, "permission")

    def test_chat_creation_keeps_original_body_key_and_deadline(self):
        deadline = clock.now_ms() + 10000
        saved = {"body": {"requestId": "create-one", "model": "model"},
                 "write": {"requestKey": "original-key", "deadlineMs": deadline}}
        options = chat.creation_options(saved, self.host)
        self.assertEqual(options.request_id, "create-one")
        self.assertEqual(options.model, "model")
        self.assertEqual(options.write.request_key, "original-key")
        self.assertEqual(options.write.deadline_ms, deadline)
        saved["write"]["deadlineMs"] = clock.now_ms() - 1
        with self.assertRaises(sdk.Error) as caught:
            chat.creation_options(saved, self.host)
        self.assertEqual(caught.exception.code, "timeout")

    def test_chat_opens_stream_before_send_and_ignores_old_turn_completion(self):
        current = Session([event("turn.completed", 9, turn="old", terminal="completed"),
                           event("turn.started", 11), event("turn.completed", 12, terminal="completed")])
        with self.quiet():
            chat.chat(current, self.host, "hello")
        self.assertEqual(current.calls, [("events", "10"), ("send", "hello")])
        self.assertTrue(current.stream.closed)

    def test_eof_gap_and_noninteractive_ticket_never_count_as_completion(self):
        cases = [([], "outcome_unknown"),
                 ([event("server.replay.gap", 11)], "reconciliation_required"),
                 ([event("server.permission.request", 11, requestId="ticket", digest="digest")],
                  "interactive_reply_required")]
        for values, expected in cases:
            current = Session(values)
            with self.subTest(code=expected), self.quiet():
                with self.assertRaises(sdk.Error) as caught:
                    chat.chat(current, self.host, "hello")
                self.assertEqual(caught.exception.code, expected)
            self.assertTrue(current.stream.closed)

    def test_only_observed_open_ticket_can_be_answered(self):
        current, tickets = Session([]), chat.Tickets()
        with self.quiet():
            chat.command("/allow absent", current, tickets, self.host)
            self.assertEqual(current.calls, [])
            tickets.display(event("server.permission.request", 11, requestId="ticket", digest="original-digest"))
            chat.command("/allow ticket", current, tickets, self.host)
            chat.command("/allow ticket", current, tickets, self.host)
        self.assertEqual(len(current.calls), 1)
        self.assertEqual(current.calls[0][1:4], ("ticket", "original-digest", "allow"))

    def test_insert_does_not_silently_drop_unknown_fields(self):
        value = dict(inputId="original", target=dict(historyEpoch="1", turnId="turn"), content=dict(text="hello"))
        self.assertEqual(chat.input_from(value).input_id, "original")
        value["overwriteTarget"] = True
        with self.assertRaises(sdk.Error):
            chat.input_from(value)

    def test_bounded_event_reader_can_close_while_consumer_is_full(self):
        stream = Stream(event("turn.started", seq) for seq in range(100000))
        events = chat.Events(stream)
        try:
            for unused in range(100):
                if events.queue.full():
                    break
                threading_event = self.host.cancel
                threading_event.wait(0.001)
            self.assertLessEqual(events.queue.qsize(), 16)
        finally:
            events.close()
        self.assertFalse(events.worker.is_alive())
        self.assertTrue(stream.closed)

    def test_tool_emits_first_chunk_then_returns_business_fact(self):
        seen = []
        output = types.SimpleNamespace(stdout=types.SimpleNamespace(write=lambda raw: seen.append(("out", raw))),
                                       stderr=types.SimpleNamespace(write=lambda raw: seen.append(("err", raw))))
        cancel = types.SimpleNamespace(wait=lambda seconds: seen.append(("wait", seconds)) or False)
        context = types.SimpleNamespace(cancelled=False, output=output, cancel=cancel, check=lambda: None)
        result = tools.lookup(context, {"orderId": "DEMO-001"})
        self.assertEqual(result["status"], "ok")
        self.assertEqual([item[0] for item in seen], ["out", "wait", "err"])
        cancel.wait = lambda seconds: True
        with self.assertRaises(sdk.Error) as caught:
            tools.lookup(context, {"orderId": "DEMO-001"})
        self.assertEqual(caught.exception.code, "outcome_unknown")
        with self.assertRaises(executor.Rejected):
            tools.lookup(context, {"unexpected": True})

    def test_reader_start_failure_closes_opened_stream(self):
        stream = mock.Mock()
        with mock.patch.object(chat.threading.Thread, "start", side_effect=RuntimeError("no thread")):
            with self.assertRaises(RuntimeError):
                chat.Events(stream)
        stream.close.assert_called_once_with()

    def test_sdk1_lost_creation_is_not_replayed_or_replaced(self):
        args = types.SimpleNamespace(journal=self.path.parent, session=None, family="sdk1", request_id=None)
        client = mock.Mock()
        client.create.side_effect = sdk.Error("outcome_unknown")
        with mock.patch.object(tools, "SessionClient", return_value=client), self.quiet():
            directory = MemoryDirectory(str(args.journal))
            with self.assertRaises(sdk.Error):
                tools.session_for(args, self.host, directory)
            with self.assertRaises(sdk.Error) as caught:
                tools.session_for(args, self.host, directory)
        self.assertEqual(caught.exception.code, "outcome_unknown")
        self.assertEqual(client.create.call_count, 1)

    def test_offload_lost_creation_reuses_original_identity_and_options(self):
        args = types.SimpleNamespace(journal=self.path.parent, session=None, family="sdk2-offload-v1", request_id="same")
        client = mock.Mock()
        client.create.side_effect = [sdk.Error("outcome_unknown"), types.SimpleNamespace(id="known-session")]
        with mock.patch.object(tools, "SessionClient", return_value=client):
            directory = MemoryDirectory(str(args.journal))
            with self.assertRaises(sdk.Error):
                tools.session_for(args, self.host, directory)
            self.assertEqual(tools.session_for(args, self.host, directory).id, "known-session")
        first, second = [entry[0][0] for entry in client.create.call_args_list]
        self.assertEqual(first, second)
        self.assertEqual(first.request_id, "same")
        self.assertEqual(first.client_tools, [tools.declaration()])

    def test_received_material_status_is_not_consumption_success(self):
        args = archive.build_parser().parse_args(["--mode", "material-status", "--intent", str(self.path)])
        host = mock.MagicMock()
        host.__enter__.return_value = host
        host.context.return_value = {}
        intent = types.SimpleNamespace(body={"bindingId": "binding", "materialRequestId": "original"})
        client = mock.Mock()
        client.material_status.return_value = {"state": "received"}
        with mock.patch.object(common, "Host", return_value=host), mock.patch.object(archive, "load_intent", return_value=intent):
            with mock.patch.object(archive, "ArchiveClient", return_value=client), self.quiet():
                with self.assertRaises(sdk.Error) as caught:
                    archive.run(args)
        self.assertEqual(caught.exception.code, "consumption_unconfirmed")
        client.material_status.assert_called_once_with("binding", "original")

    def test_prepared_material_identity_is_saved_before_upload_and_keeps_deadline(self):
        args = types.SimpleNamespace(intent=self.path, binding="binding", request_id="stable-response")
        saved = types.SimpleNamespace(kind="material-request", deadline_ms=clock.now_ms() + 5000,
                                      body={"bindingId": "binding"})
        original = {"identity": {"requestId": "stable-response", "operationEpoch": "old-epoch"},
                    "deadlineMs": saved.deadline_ms}
        client = mock.Mock()
        client.prepare_materials.return_value = {"response": "exact"}
        client.submit_materials.return_value = {"state": "received"}
        response = types.SimpleNamespace(deadline_ms=saved.deadline_ms)
        with mock.patch.object(archive, "exists", side_effect=lambda path, host: path.name.endswith((".request", ".identity"))):
            with mock.patch.object(archive, "load_intent", return_value=saved), mock.patch.object(common, "load_owned", return_value=original):
                with mock.patch.object(archive, "save_intent", return_value=response) as save, self.quiet():
                    archive.materials(client, "store", self.host, args)
        call = client.prepare_materials.call_args
        self.assertEqual(call[0], ("store", saved, original["identity"]))
        self.assertEqual(call[1]["deadline_ms"], saved.deadline_ms)
        self.assertEqual(save.call_args[0][4], saved.deadline_ms)
        client.binding.assert_not_called()


    def test_publication_unknown_stops_and_exposes_original_keys_without_body(self):
        publication = importlib.import_module("tansr_demo.publication")
        from tansr_sdk.storage import PrivateDirectory
        from test_memory_publication import IDENTITY, KEY, publication_operation, request
        operation = publication_operation(request("commit", transferId="original-transfer"))
        config = dict(identity=IDENTITY, sessionId=operation["sessionId"], binding=operation["binding"],
                      connection=dict(executorId=operation["binding"]["target"]["executorId"]),
                      workspace=dict(workspaceId="workspace", revision="1"))
        for status, expected in (("unknown", "outcome_unknown"), ("completed", "cancelled")):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary).resolve()
                credentials = root / "credentials"
                with PrivateDirectory(str(credentials), create=True) as private:
                    private.write("token", b"synthetic-token")
                    private.write("scope", strict_json.dumps(operation["scope"]))
                    private.write("config", strict_json.dumps(config))
                    private.write("key", KEY.hex().encode("ascii"))
                    private.write("journal-key", (b"j" * 32).hex().encode("ascii"))
                args = publication.build_parser().parse_args([
                    "--token-file", str(credentials / "token"), "--scope-file", str(credentials / "scope"),
                    "--config", str(credentials / "config"), "--file", str(root / "body" / "data.enc"),
                    "--journal-file", str(root / "journal" / "data.enc"), "--key-file", str(credentials / "key"),
                    "--journal-key-file", str(credentials / "journal-key"), "--key-id", "body",
                    "--journal-key-id", "journal", "--mode", "create"])
                states = []
                class FakeRunner:
                    def __init__(self, *unused, **options):
                        self.callback = options["on_receipt"]
                    def __enter__(self):
                        return self
                    def __exit__(self, *unused):
                        states.append("closed")
                    def run(self, *, cancel):
                        self.callback(operation, types.SimpleNamespace(receipt=dict(status=status)))
                        states.append(cancel.cancelled)
                        raise sdk.Error("cancelled")
                output = io.StringIO()
                with mock.patch.object(publication, "Runner", FakeRunner), contextlib.redirect_stdout(output):
                    with self.assertRaises(sdk.Error) as caught:
                        publication.run(args)
                self.assertEqual(caught.exception.code, expected)
                self.assertEqual(states, [status == "unknown", "closed"])
                rows = [strict_json.loads(line) for line in output.getvalue().splitlines() if line.startswith("{")]
                self.assertEqual(rows, [dict(operationId=operation["operationId"], digest=operation["digest"],
                    action="commit", transferId="original-transfer", status=status)])
                self.assertNotIn("argsJson", output.getvalue())
                with PrivateDirectory(str(root / "body")), PrivateDirectory(str(root / "journal")):
                    pass  # 原 Demo 已释放两个介质的实际 owner 锁。


if __name__ == "__main__":
    unittest.main()
