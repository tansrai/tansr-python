"""PST-05 publication真实加密介质、围栏、原键恢复与Runner接线。"""
import base64
import copy
import hashlib
import json
import threading

import pytest

from tansr_sdk import CancellationToken, Error
from tansr_sdk.executor import EncryptedJournal, ExecutorClient, FileJournal, Runner, operation_digest, validate_operation
from tansr_sdk.executor._common import receipt_for
from tansr_sdk.memory_publication import FileStore, Host, TOOL_NAME, DEFINITION_DIGEST
from tansr_sdk.storage import PrivateDirectory
from test_executor import SCOPE, Peer, connection, current_platform, operation

IDENTITY = dict(scope={key: SCOPE[key] for key in ("applicationScopeId", "endUserId")},
                sourceId="source", sourceGeneration="1", domainKey="memory")
KEY = b"p" * 32
BODY = '{"secret":"PST private publication 材料"}'.encode("utf-8")


def request(action, **fields):
    return dict(contract="terminal-services-v1", action=action,
                **{key: IDENTITY[key] for key in ("sourceId", "sourceGeneration", "domainKey")}, **fields)


def owner():
    op = operation()
    return {key: op[key] for key in ("scope", "sessionId", "binding")}


def publication_operation(value, identifier="publication-op"):
    op = operation()
    op.update(toolName="MemoryPublication", operationId=identifier)
    op["request"]["args"] = dict(name=TOOL_NAME, definitionDigest=DEFINITION_DIGEST,
                                  argsJson=json.dumps(value, ensure_ascii=False))
    op["digest"] = operation_digest(op)
    return op


def store_at(tmp_path, **options):
    values = dict(mode="create", max_transfers=8)
    values.update(options)
    return FileStore(str(tmp_path / "publication" / "data.enc"), KEY, "publication-key", IDENTITY,
                     values.pop("read_context", lambda: SCOPE), **values)


def begin(store, transfer="transfer", body=BODY, expected=None, who=None):
    return store.execute(request("begin", transferId=transfer, expectedEtag=expected,
                         byteLength=len(body), sha256=hashlib.sha256(body).hexdigest()), who or owner())


def chunk(store, transfer="transfer", body=BODY, offset=0, who=None):
    return store.execute(request("chunk", transferId=transfer, offset=offset, byteLength=len(body),
                         base64=base64.b64encode(body).decode(), payloadDigest=hashlib.sha256(body).hexdigest()),
                         who or owner())


def commit(store, transfer="transfer", who=None):
    return store.execute(request("commit", transferId=transfer), who or owner())


def test_durable_cas_replay_read_reopen_and_capacity(tmp_path):
    with store_at(tmp_path, max_transfers=2) as store:
        assert store.execute(request("head"), owner())["publication"] is None
        assert begin(store)["transfer"]["receivedBytes"] == 0
        chunk(store, body=BODY[:8])
        assert chunk(store, body=BODY[:8])["transfer"]["receivedBytes"] == 8
    with store_at(tmp_path, mode="reopen", max_transfers=2) as store:
        assert begin(store)["transfer"]["receivedBytes"] == 8
        chunk(store, body=BODY[8:], offset=8)
        first = commit(store)
        assert first["transfer"]["status"] == "committed"
        assert commit(store) == first
        begin(store, "competing")
        chunk(store, "competing")
        assert commit(store, "competing")["transfer"]["status"] == "conflict"
        assert store.capacity()["remainingTransfers"] == 0
        with pytest.raises(Error, match="capacity_exceeded"):
            begin(store, "third")
        etag = first["transfer"]["etag"]
        result = store.execute(request("read", etag=etag, offset=0, length=12288), owner())
        assert base64.b64decode(result["base64"]) == BODY
        assert result["complete"] is True
        assert store.execute(request("query", transferId="missing"), owner())["transfer"]["status"] == "unknown"
    with store_at(tmp_path, mode="reopen", max_transfers=2) as store:
        assert store.execute(request("query", transferId="transfer"), owner())["transfer"]["status"] == "committed"
    assert BODY not in (tmp_path / "publication" / "data.enc").read_bytes()
    assert base64.b64encode(BODY) not in (tmp_path / "publication" / "data.enc").read_bytes()


def test_owner_fence_and_query_only_authorized_recovery(tmp_path):
    other = owner()
    other["binding"]["target"]["connectionRevision"] = "2"
    with store_at(tmp_path) as store:
        begin(store)
        for action in ("query", "commit"):
            with pytest.raises(Error, match="request_conflict"):
                store.execute(request(action, transferId="transfer"), other)
        with pytest.raises(Error, match="stale_generation"):
            store.execute(dict(request("head"), sourceGeneration="2"), owner())
        bad = owner()
        bad["scope"]["authorizationRevision"] = "2"
        with pytest.raises(Error, match="request_conflict"):
            store.execute(request("head"), bad)
    facts = []
    with store_at(tmp_path, mode="reopen", authorize_recovery=lambda value: facts.append(value) or True) as store:
        assert store.execute(request("query", transferId="transfer"), other)["transfer"]["status"] == "staging"
        with pytest.raises(Error, match="request_conflict"):
            commit(store, who=other)
        assert len(facts) == 1 and facts[0]["originalOwner"] == owner()


def test_bad_chunk_utf8_etag_and_begin_identity_are_rejected(tmp_path):
    with store_at(tmp_path) as store:
        begin(store)
        with pytest.raises(Error, match="request_conflict"):
            chunk(store, offset=1)
        with pytest.raises(Error, match="request_conflict"):
            begin(store, body=b"changed")
        with pytest.raises(Error, match="integrity_mismatch"):
            commit(store)
        chunk(store)
        commit(store)
        with pytest.raises(Error, match="revision_conflict"):
            store.execute(request("read", etag="0" * 64, offset=0, length=1), owner())
        begin(store, "invalid-utf8", b"\xff")
        chunk(store, "invalid-utf8", b"\xff")
        with pytest.raises(Error, match="integrity_mismatch"):
            commit(store, "invalid-utf8")


@pytest.mark.parametrize("stage", ["before_replace", "replaced", "directory_synced"])
def test_commit_failure_preserves_original_transfer_on_reopen(tmp_path, stage):
    with store_at(tmp_path) as store:
        begin(store)
        chunk(store)
        def fault(current):
            if current == stage:
                raise OSError("injected commit interruption")
        store._hook = fault
        with pytest.raises(Error, match="storage_unknown"):
            commit(store)
        with pytest.raises(Error, match="storage_unknown"):
            store.execute(request("head"), owner())
    with store_at(tmp_path, mode="reopen") as store:
        status = store.execute(request("query", transferId="transfer"), owner())["transfer"]["status"]
        assert status == ("staging" if stage == "before_replace" else "committed")
        assert commit(store)["transfer"]["status"] == "committed"


def test_wrong_key_tampering_truncation_rotation_and_original_retained(tmp_path):
    with store_at(tmp_path) as store:
        begin(store)
        chunk(store)
        commit(store)
        store.rotate_key(b"q" * 32, "next-key")
    path = tmp_path / "publication" / "data.enc"
    original = path.read_bytes()
    with pytest.raises(Error):
        store_at(tmp_path, mode="reopen")
    assert path.read_bytes() == original
    options = dict(mode="reopen", max_transfers=8)
    with FileStore(str(path), b"q" * 32, "next-key", IDENTITY, lambda: SCOPE, **options) as store:
        assert store.execute(request("head"), owner())["publication"] is not None
    for invalid in (original[:-7], original[:-1] + bytes([original[-1] ^ 1])):
        with PrivateDirectory(str(path.parent)) as directory:
            directory.write(path.name, invalid)
        with pytest.raises(Error):
            FileStore(str(path), b"q" * 32, "next-key", IDENTITY, lambda: SCOPE, **options)
        assert path.read_bytes() == invalid


def test_close_waits_for_commit_and_cancellation_has_no_false_ack(tmp_path):
    reached, proceed = threading.Event(), threading.Event()
    store = store_at(tmp_path)
    begin(store)
    chunk(store)
    def hook(stage):
        if stage == "before_replace":
            reached.set()
            assert proceed.wait(5)
    store._hook = hook
    results = []
    worker = threading.Thread(target=lambda: results.append(commit(store)))
    worker.start()
    assert reached.wait(5)
    closed = threading.Event()
    closer = threading.Thread(target=lambda: (store.close(), closed.set()))
    closer.start()
    assert not closed.wait(0.05)
    proceed.set()
    worker.join(5)
    closer.join(5)
    assert closed.is_set() and results[0]["transfer"]["status"] == "committed"
    with store_at(tmp_path, mode="reopen") as store:
        token = CancellationToken()
        token.cancel()
        with pytest.raises(Error, match="cancelled"):
            store.execute(request("head"), owner(), cancel=token)


def test_lock_owner_reopen_and_scope_changed_callback(tmp_path):
    scope = copy.deepcopy(SCOPE)
    with store_at(tmp_path, read_context=lambda: scope) as store:
        with pytest.raises(Error):
            store_at(tmp_path, mode="reopen")
        def revoke(stage):
            if stage == "before_replace":
                scope["authorizationRevision"] = "2"
        store._hook = revoke
        with pytest.raises(Error, match="storage_unknown"):
            begin(store)
    with store_at(tmp_path, mode="reopen") as store:
        assert store.execute(request("query", transferId="transfer"), owner())["transfer"]["status"] == "unknown"


def journal_at(tmp_path, **options):
    config = dict(mode="create", max_records=16)
    config.update(options)
    return EncryptedJournal(str(tmp_path / "journal" / "receipts.enc"), b"j" * 32, "journal-key",
                            dict(scope=IDENTITY["scope"], executorId="device"), lambda: SCOPE, **config)


def test_runner_publication_encrypted_sensitive_receipt_and_replay(tmp_path):
    with store_at(tmp_path) as store:
        begin(store)
        chunk(store)
        etag = commit(store)["transfer"]["etag"]
        op = publication_operation(request("read", etag=etag, offset=0, length=12288))
        peer = Peer(op)
        client = ExecutorClient(peer, SCOPE)
        host = Host(store)
        registration = dict(protocol="sdk2-ext-v1", executorId="device", platform=current_platform(),
                            operations=["tool.invoke"], tools=[host.registration()],
                            workspaces=[dict(workspaceId="workspace", revision="1")])
        with journal_at(tmp_path) as journal:
            with Runner(client, registration, {}, journal, lambda op, token: True,
                        connection=connection(), memory_publication=host) as runner:
                receipt = runner.execute(op)
                assert receipt["status"] == "completed"
                assert base64.b64encode(BODY).decode() in receipt["result"]["args"]["resultJson"]
                assert runner.execute(op) == receipt
        for path in tmp_path.rglob("*"):
            if path.is_file() and path.stat().st_size:
                assert BODY not in path.read_bytes()
                assert base64.b64encode(BODY) not in path.read_bytes()
        with journal_at(tmp_path, mode="reopen") as journal:
            assert journal.claim(op).receipt == receipt
            with Runner(client, registration, {}, journal, lambda op, token: True,
                        connection=connection(), memory_publication=host) as runner:
                assert runner.execute(op) == receipt
        with FileJournal(str(tmp_path / "plain")) as journal:
            with pytest.raises(Error, match="unsupported"):
                Runner(client, registration, {}, journal, lambda op, token: True, memory_publication=host)


def test_reserved_profile_never_accepts_business_spoof(tmp_path):
    op = publication_operation(request("head"))
    validate_operation(op)
    for change in (dict(name="MemoryPublication"), dict(definitionDigest="0" * 64)):
        invalid = copy.deepcopy(op)
        invalid["request"]["args"].update(change)
        invalid["digest"] = operation_digest(invalid)
        with pytest.raises(Error):
            validate_operation(invalid)
    invalid = copy.deepcopy(op)
    invalid["toolName"] = TOOL_NAME
    invalid["digest"] = operation_digest(invalid)
    with pytest.raises(Error):
        validate_operation(invalid)


def test_journal_claim_only_capacity_rotation_and_receipt_immutability(tmp_path):
    op = publication_operation(request("head"))
    receipt = receipt_for(op, "unknown", "execution_outcome_unknown")
    with journal_at(tmp_path, max_records=1) as journal:
        assert journal.claim(op).claimed
        assert not journal.claim(op).claimed
        journal.complete(op, receipt)
        with pytest.raises(Error, match="capacity"):
            journal.claim(publication_operation(request("head"), "another"))
        with pytest.raises(Error, match="conflict"):
            journal.complete(op, receipt_for(op, "failed", "other"))
        journal.rotate_key(b"k" * 32, "journal-next")
    with EncryptedJournal(str(tmp_path / "journal" / "receipts.enc"), b"k" * 32, "journal-next",
            dict(scope=IDENTITY["scope"], executorId="device"), lambda: SCOPE, mode="reopen", max_records=1) as journal:
        assert journal.claim(op).receipt == receipt


def test_full_chunk_bounds_and_reserved_staging_budget(tmp_path):
    with store_at(tmp_path, max_staging_bytes=4194304) as store:
        payload = b"x" * 12288
        begin(store, body=payload)
        chunk(store, body=payload)
        assert commit(store)["transfer"]["status"] == "committed"
        begin(store, "large", b"x" * 4194304)
        with pytest.raises(Error, match="capacity_exceeded"):
            begin(store, "over")
        assert store.capacity()["remainingStagingBytes"] == 0

def test_swallowed_reentry_poison_prevents_publication_commit(tmp_path):
    with store_at(tmp_path) as store:
        def reenter(stage):
            if stage == "before_replace":
                with pytest.raises(Error, match="reentrant"):
                    store.execute(request("head"), owner())
        store._hook = reenter
        with pytest.raises(Error, match="storage_unknown"):
            begin(store)
    with store_at(tmp_path, mode="reopen") as store:
        assert store.execute(request("query", transferId="transfer"), owner())["transfer"]["status"] == "unknown"


def test_runner_journal_commit_failure_never_returns_completed_receipt(tmp_path):
    with store_at(tmp_path) as store, journal_at(tmp_path) as journal:
        op = publication_operation(request("head"))
        peer = Peer(op)
        host = Host(store)
        registration = dict(protocol="sdk2-ext-v1", executorId="device", platform=current_platform(),
                            operations=["tool.invoke"], tools=[host.registration()],
                            workspaces=[dict(workspaceId="workspace", revision="1")])
        calls = []
        def fault(stage):
            if stage == "before_replace":
                calls.append(stage)
                if len(calls) == 2:
                    raise OSError("receipt disk fault")
        journal._hook = fault
        with Runner(ExecutorClient(peer, SCOPE), registration, {}, journal, lambda op, token: True,
                    connection=connection(), memory_publication=host) as runner:
            with pytest.raises(Error, match="storage_unknown"):
                runner.execute(op)
            assert peer.receipt is None
    with journal_at(tmp_path, mode="reopen") as journal:
        assert not journal.claim(op).claimed
        assert journal.claim(op).receipt is None

def test_native_child_process_reopens_original_encrypted_transfer(tmp_path):
    import os
    from pathlib import Path
    import subprocess
    import sys
    with store_at(tmp_path) as store:
        begin(store)
        chunk(store)
        commit(store)
    root = Path(__file__).resolve().parents[1]
    code = """import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
sys.path.insert(0, sys.argv[2])
from test_memory_publication import store_at, request, owner
with store_at(Path(sys.argv[3]), mode='reopen') as store:
    assert store.execute(request('query', transferId='transfer'), owner())['transfer']['status'] == 'committed'
    print('original transfer committed')
"""
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
    result = subprocess.run([sys.executable, "-c", code, str(root / "src"), str(root / "tests"), str(tmp_path)],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, timeout=30)
    assert result.returncode == 0, result.stderr.decode("utf-8", "replace")
    assert b"original transfer committed" in result.stdout
