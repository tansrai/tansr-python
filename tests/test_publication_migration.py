"""PST 保源迁移/轮钥；原键、永久终态、权限及提交失回都在真实介质上检查。"""
import copy
import json

import pytest

from tansr_sdk import Error
from tansr_sdk.executor import EncryptedJournal, FileJournal, operation_digest
from tansr_sdk.executor._common import receipt_for
from tansr_sdk.memory_publication import FileStore
from tansr_sdk.storage import PrivateDirectory
from test_executor import SCOPE, operation, result
from test_memory_publication import IDENTITY, KEY, BODY, begin, chunk, commit, owner, request, store_at

JIDENTITY = dict(scope=IDENTITY["scope"], executorId="device")
NEXT = b"n" * 32


def reopen_publication(path, **options):
    return FileStore(str(path), NEXT, "next", IDENTITY, lambda: SCOPE,
                     mode="reopen", max_transfers=8, **options)


def facts():
    ops, receipts = [], []
    for status in ("pending", "unknown", "completed"):
        op = operation()
        op["operationId"] = "migration-" + status
        op["digest"] = operation_digest(op)
        ops.append(op)
        receipt = None if status == "pending" else receipt_for(op, "unknown", "execution_outcome_unknown")
        if status == "completed":
            receipt = receipt_for(op, "completed", result=dict(operation="tool.invoke", args=dict(resultJson=json.dumps(result()))))
        receipts.append(receipt)
    return ops, receipts


def assert_facts(journal, ops, receipts):
    for op, expected in zip(ops, receipts):
        replay = journal.claim(op)
        assert not replay.claimed and replay.receipt == expected
    changed = copy.deepcopy(ops[0])
    changed["request"]["args"]["argsJson"] = '{"other":true}'
    changed["digest"] = operation_digest(changed)
    with pytest.raises(Error, match="conflict"):
        journal.claim(changed)
    with pytest.raises(Error, match="conflict"):
        journal.complete(ops[1], receipt_for(ops[1], "failed", "changed"))


def test_publication_copy_preserves_every_fact_and_owner_fence(tmp_path):
    source = tmp_path / "publication" / "data.enc"
    target = tmp_path / "target" / "new.enc"
    other = owner()
    other["binding"]["target"]["connectionRevision"] = "2"
    with store_at(tmp_path) as store:
        begin(store, "committed")
        chunk(store, "committed")
        commit(store, "committed")
        begin(store, "conflict")
        chunk(store, "conflict")
        commit(store, "conflict")
        begin(store, "pending")
        chunk(store, "pending", BODY[:7])
        before = source.read_bytes()
        store.copy_to(str(target), NEXT, "next")
        assert source.read_bytes() == before
        with pytest.raises(Error, match="conflict"):
            store.copy_to(str(source), NEXT, "next")
        saved = target.read_bytes()
        with pytest.raises(Error, match="conflict"):
            store.copy_to(str(target), NEXT, "next")
        assert target.read_bytes() == saved and source.read_bytes() == before
    with reopen_publication(target, authorize_recovery=lambda facts: True) as migrated:
        for key, expected in (("committed", "committed"), ("conflict", "conflict"), ("pending", "staging"), ("absent", "unknown")):
            assert migrated.execute(request("query", transferId=key), owner())["transfer"]["status"] == expected
        assert migrated.execute(request("query", transferId="pending"), other)["transfer"]["receivedBytes"] == 7
        with pytest.raises(Error, match="request_conflict"):
            commit(migrated, "pending", other)
        # 新路径以原 owner 继续原暂存，CAS 不因复制而绕过已提交头。
        chunk(migrated, "pending", BODY[7:], 7)
        assert commit(migrated, "pending")["transfer"]["status"] == "conflict"
        assert migrated.capacity()["storedTransfers"] == 3
        migrated.rotate_key(b"r" * 32, "rotated")
    with FileStore(str(target), b"r" * 32, "rotated", IDENTITY, lambda: SCOPE, mode="reopen", max_transfers=8) as migrated:
        assert migrated.execute(request("query", transferId="committed"), owner())["transfer"]["status"] == "committed"
        assert migrated.execute(request("query", transferId="pending"), owner())["transfer"]["status"] == "conflict"
    assert BODY not in target.read_bytes() and source.read_bytes() == before


@pytest.mark.parametrize("kind", ["publication", "journal"])
@pytest.mark.parametrize("stage", ["before_replace", "replaced", "directory_synced"])
def test_copy_fault_keeps_source_and_target_is_absent_or_complete(tmp_path, kind, stage):
    target = tmp_path / "target" / "copy.enc"
    def fault(at):
        if at == stage:
            raise OSError("copy interruption")
    if kind == "publication":
        with store_at(tmp_path) as source:
            begin(source)
            chunk(source)
            commit(source)
            original = (tmp_path / "publication" / "data.enc").read_bytes()
            with pytest.raises(Error, match="storage_unknown"):
                source.copy_to(str(target), NEXT, "next", commit_hook=fault)
            assert (tmp_path / "publication" / "data.enc").read_bytes() == original
            assert source.execute(request("query", transferId="transfer"), owner())["transfer"]["status"] == "committed"
    else:
        ops, receipts = facts()
        path = tmp_path / "source" / "journal.enc"
        with EncryptedJournal(str(path), KEY, "old", JIDENTITY, lambda: SCOPE, mode="create") as source:
            for op, receipt in zip(ops, receipts):
                source.claim(op)
                if receipt is not None:
                    source.complete(op, receipt)
            original = path.read_bytes()
            with pytest.raises(Error, match="storage_unknown"):
                source.copy_to(str(target), NEXT, "next", commit_hook=fault)
            assert path.read_bytes() == original
            assert_facts(source, ops, receipts)
    if stage == "before_replace":
        assert not target.exists()
    elif kind == "publication":
        with reopen_publication(target) as copied:
            assert copied.execute(request("query", transferId="transfer"), owner())["transfer"]["status"] == "committed"
    else:
        with EncryptedJournal(str(target), NEXT, "next", JIDENTITY, lambda: SCOPE, mode="reopen") as copied:
            assert_facts(copied, ops, receipts)


def test_plaintext_migration_full_inventory_then_encrypted_rotation(tmp_path):
    plain, target, rotated = tmp_path / "plain", tmp_path / "encrypted" / "data.enc", tmp_path / "rotated" / "next.enc"
    ops, receipts = facts()
    with FileJournal(str(plain)) as source:
        for op, receipt in zip(ops, receipts):
            source.claim(op)
            if receipt is not None:
                source.complete(op, receipt)
        before = {p.name: p.read_bytes() for p in plain.glob("*.execution.json")}
        with pytest.raises(Error, match="conflict"):
            source.copy_to_encrypted(str(target), KEY, "old", JIDENTITY, lambda: SCOPE, operations=ops[:2])
        assert not target.exists()
        changed = copy.deepcopy(ops)
        changed[0]["request"]["args"]["argsJson"] = '{"wrong":true}'
        changed[0]["digest"] = operation_digest(changed[0])
        with pytest.raises(Error, match="conflict"):
            source.copy_to_encrypted(str(target), KEY, "old", JIDENTITY, lambda: SCOPE, operations=changed)
        source.copy_to_encrypted(str(target), KEY, "old", JIDENTITY, lambda: SCOPE, operations=ops, max_records=3)
        assert {p.name: p.read_bytes() for p in plain.glob("*.execution.json")} == before
        assert_facts(source, ops, receipts)
    with EncryptedJournal(str(target), KEY, "old", JIDENTITY, lambda: SCOPE, mode="reopen", max_records=3) as migrated:
        assert_facts(migrated, ops, receipts)
        original = target.read_bytes()
        migrated.copy_to(str(rotated), NEXT, "next")
        assert target.read_bytes() == original
        # 原地轮钥仍保留 pending、unknown、终态，没有清空作为轮钥捷径。
        migrated.rotate_key(b"r" * 32, "rotated")
    for path, key, key_id in ((target, b"r" * 32, "rotated"), (rotated, NEXT, "next")):
        with EncryptedJournal(str(path), key, key_id, JIDENTITY, lambda: SCOPE, mode="reopen", max_records=3) as migrated:
            assert_facts(migrated, ops, receipts)
            with pytest.raises(Error, match="capacity"):
                migrated.claim(operation())
    assert b"known-result" not in target.read_bytes() and b"known-result" not in rotated.read_bytes()


def test_migration_refuses_foreign_corrupt_files_and_scope_change(tmp_path):
    source_path, target = tmp_path / "plain", tmp_path / "new" / "journal.enc"
    ops, receipts = facts()
    with FileJournal(str(source_path)) as source:
        source.claim(ops[0])
        with PrivateDirectory(str(source_path)) as directory:
            directory.write("foreign", b"retain", replace=False)
        with pytest.raises(Error, match="conflict"):
            source.copy_to_encrypted(str(target), KEY, "old", JIDENTITY, lambda: SCOPE, operations=ops[:1])
        assert not target.exists() and (source_path / "foreign").read_bytes() == b"retain"
    scope = copy.deepcopy(SCOPE)
    with store_at(tmp_path, read_context=lambda: scope) as source:
        begin(source)
        def revoke(stage):
            if stage == "before_replace":
                scope["authorizationRevision"] = "2"
        with pytest.raises(Error, match="storage_unknown"):
            source.copy_to(str(target), NEXT, "next", commit_hook=revoke)
        assert not target.exists()
    with store_at(tmp_path, mode="reopen") as source:
        assert source.execute(request("query", transferId="transfer"), owner())["transfer"]["status"] == "staging"


def test_native_child_replays_migrated_pending_unknown_and_terminal_without_effect(tmp_path):
    import os
    from pathlib import Path
    import subprocess
    import sys
    ops, receipts = facts()
    target = tmp_path / "encrypted" / "journal.enc"
    with FileJournal(str(tmp_path / "plain")) as source:
        for op, receipt in zip(ops, receipts):
            source.claim(op)
            if receipt is not None:
                source.complete(op, receipt)
        source.copy_to_encrypted(str(target), NEXT, "next", JIDENTITY, lambda: SCOPE, operations=ops)
    script = """import json,sys
from tansr_sdk.executor import EncryptedJournal,ExecutorClient,Runner,Tool,current_platform
from test_executor import DECLARATION,SCOPE,Peer
ops=json.loads(sys.argv[2])
def effect(*unused): raise AssertionError('migrated operation repeated')
tool=Tool(DECLARATION,effect)
registration=dict(protocol='sdk2-ext-v1',executorId='device',platform=current_platform(),
 workspaces=[dict(workspaceId='workspace',revision='1')],operations=['tool.invoke'],tools=[tool.registration()])
identity=dict(scope={key:SCOPE[key] for key in ('applicationScopeId','endUserId')},executorId='device')
with EncryptedJournal(sys.argv[1],b'n'*32,'next',identity,lambda:SCOPE,mode='reopen') as journal:
 for op,status in zip(ops,('unknown','unknown','completed')):
  peer=Peer(op)
  with Runner(ExecutorClient(peer,SCOPE),registration,{'Lookup':tool},journal,lambda op,token:None,connection=peer.connection) as runner:
   receipt=runner.execute(op)
   assert receipt['status']==status
   assert runner.execute(op)==receipt
   assert journal.claim(op).receipt==receipt
print('three original keys replayed; no effects')
"""
    root = Path(__file__).resolve().parents[1]
    env = dict(os.environ, PYTHONPATH=os.pathsep.join((str(root / "src"), str(root / "tests"))), PYTHONDONTWRITEBYTECODE="1")
    child = subprocess.run([sys.executable, "-c", script, str(target), json.dumps(ops)], env=env,
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=20)
    assert child.returncode == 0, child.stderr.decode("utf-8", "replace")
    assert b"three original keys replayed; no effects" in child.stdout


@pytest.mark.parametrize("mode", ["copy-publication", "copy-journal", "migrate-journal"])
def test_existing_publication_demo_explicit_copy_modes(tmp_path, monkeypatch, mode):
    import contextlib
    from pathlib import Path
    import sys
    from types import SimpleNamespace
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "demo" / "src"))
    from tansr_demo import publication
    source_path, journal_path = tmp_path / "publication" / "data.enc", tmp_path / "journal" / "data.enc"
    target = tmp_path / "new" / "data.enc"
    legacy = tmp_path / "plain"
    ops, receipts = facts()
    with store_at(tmp_path) as source:
        begin(source)
    with EncryptedJournal(str(journal_path), b"j" * 32, "journal-key", JIDENTITY, lambda: SCOPE, mode="create", max_records=16384) as journal:
        journal.claim(ops[0])
    with FileJournal(str(legacy)) as journal:
        journal.claim(ops[0])
    args = publication.build_parser().parse_args([
        "--config", str(tmp_path / "config"), "--file", str(source_path), "--journal-file", str(journal_path),
        "--key-file", str(tmp_path / "key"), "--journal-key-file", str(tmp_path / "journal-key"),
        "--key-id", "publication-key", "--journal-key-id", "journal-key", "--mode", mode,
        "--target-file", str(target), "--target-key-file", str(tmp_path / "target-key"), "--target-key-id", "next",
        "--legacy-journal", str(legacy), "--operations-file", str(tmp_path / "operations"), "--max-transfers", "8"])
    config = dict(identity=IDENTITY, sessionId=owner()["sessionId"], binding=owner()["binding"], connection=dict(executorId="device"), workspace={})
    values = {args.config: json.dumps(config).encode(), args.key_file: KEY.hex().encode(),
              args.journal_key_file: (b"j" * 32).hex().encode(), args.target_key_file: NEXT.hex().encode(),
              args.operations_file: json.dumps(ops[:1]).encode()}
    monkeypatch.setattr(publication.common, "read_private", lambda path, limit: values[path])
    host = SimpleNamespace(credentials=SimpleNamespace(check=lambda: None, scope=SCOPE), check_state_directory=lambda path: None)
    monkeypatch.setattr(publication.common, "Host", lambda args: contextlib.nullcontext(host))
    publication.run(args)
    if mode == "copy-publication":
        with reopen_publication(target) as copied:
            assert copied.execute(request("query", transferId="transfer"), owner())["transfer"]["status"] == "staging"
    else:
        with EncryptedJournal(str(target), NEXT, "next", JIDENTITY, lambda: SCOPE, mode="reopen", max_records=16384) as copied:
            assert not copied.claim(ops[0]).claimed and copied.claim(ops[0]).receipt is None
    assert source_path.exists() and journal_path.exists() and list(legacy.glob("*.execution.json"))
