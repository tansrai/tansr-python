"""同新格式保源加密复制；真实本地介质，无自动 cutover。"""
import copy
import importlib
import json
from pathlib import Path
import sys

import pytest

from tansr_sdk import CancellationToken, Error, strict_json
from tansr_sdk.storage import PrivateDirectory
from tansr_sdk.terminal_persistence import FileStore
from test_terminal_persistence import IDENTITY, KEY, SCOPE, digest, owner, plan, publish, request, store_at

NEXT_KEY = b"t" * 32


def target_at(path, **options):
    return FileStore(str(path), NEXT_KEY, "new-key", IDENTITY, options.pop("read_context", lambda: SCOPE),
                     mode="reopen", **options)


def test_copy_preserves_root_indexes_all_tickets_pending_and_read_only_reopen(tmp_path):
    target = tmp_path / "candidate/data.enc"
    p, s = digest(b"primary"), digest(b"secondary")
    committed = plan(b"\xff\x00\x80", [(p, s, b"opaque receipt")])
    with store_at(tmp_path) as source:
        root = publish(source, committed)
        pending = plan(b"pending", transfer="pending", root=root)
        source.execute(pending[0], owner())
        source.execute(pending[1][0], owner())
        original = (tmp_path / "persistence/data.enc").read_bytes()
        before = copy.deepcopy(source._encrypted.load())
        result = source.copy_to(str(target), NEXT_KEY, "new-key")
        assert result["readOnly"] and result["cutover"] == "pending" and result["transferFacts"] == 2
        with target_at(target) as copied:
            assert copied.copy_verified_cutover_pending
            assert copied._unpack(copied._encrypted.load()) == source._unpack(before)
            for prepared in (committed, pending):
                q = dict(prepared[2], action="query")
                assert copied.execute(q, owner()) == source.execute(q, owner())
            for kind, key in (("primary", p), ("secondary", s)):
                q = request("lookup", commitRoot=root["commitRoot"], key=dict(kind=kind, digest=key))
                assert copied.execute(q, owner()) == source.execute(q, owner())
            q = request("read", part="body", commitRoot=root["commitRoot"], offset=0, length=3)
            assert copied.execute(q, owner()) == source.execute(q, owner())
            for q in (committed[0], committed[1][0], committed[2], pending[2]):
                with pytest.raises(Error, match="read_only_copy"):
                    copied.execute(q, owner())
            # Re-copy remains restrictive; no self-declared cutover/activation.
            copied.copy_to(str(tmp_path / "third/data.enc"), b"u" * 32, "third-key")
        with pytest.raises(Error):
            source.copy_to(str(target), NEXT_KEY, "new-key")
        assert (tmp_path / "persistence/data.enc").read_bytes() == original
        assert source._encrypted.load() == before
        assert b"opaque receipt" not in target.read_bytes()
        source.execute(pending[1][1], owner())  # source was never falsely sealed
    with FileStore(str(tmp_path / "third/data.enc"), b"u" * 32, "third-key", IDENTITY, lambda: SCOPE, mode="reopen") as copied:
        assert copied.copy_verified_cutover_pending


@pytest.mark.parametrize("stage", ["written", "file_synced", "before_replace", "replaced", "directory_synced"])
def test_copy_fault_keeps_source_and_published_target(tmp_path, stage):
    target = tmp_path / "candidate/data.enc"
    prepared = plan()
    with store_at(tmp_path) as source:
        publish(source, prepared)
        original = (tmp_path / "persistence/data.enc").read_bytes()
        def fault(at):
            if at == stage:
                raise OSError("synthetic copy failure")
        with pytest.raises(Error, match="storage_unknown"):
            source.copy_to(str(target), NEXT_KEY, "new-key", commit_hook=fault)
        assert (tmp_path / "persistence/data.enc").read_bytes() == original
        assert source.execute(dict(prepared[2], action="query"), owner())["transfer"]["status"] == "committed"
        if stage in ("replaced", "directory_synced"):
            with target_at(target) as copied:
                assert copied.copy_verified_cutover_pending
                assert copied.execute(dict(prepared[2], action="query"), owner()) == source.execute(dict(prepared[2], action="query"), owner())
        else:
            assert not target.exists()


@pytest.mark.parametrize("stage", ["before_replace", "replaced"])
@pytest.mark.parametrize("kind", ["cancel", "authority"])
def test_copy_cancel_and_last_commit_authority_preserve_source(tmp_path, stage, kind):
    scope = dict(SCOPE)
    token = CancellationToken()
    target = tmp_path / "candidate/data.enc"
    with store_at(tmp_path, read_context=lambda: scope) as source:
        original = (tmp_path / "persistence/data.enc").read_bytes()
        def fault(at):
            if at == stage:
                if kind == "cancel":
                    token.cancel()
                else:
                    scope["authorizationRevision"] = "changed"
        with pytest.raises(Error, match="storage_unknown"):
            source.copy_to(str(target), NEXT_KEY, "new-key", cancel=token, commit_hook=fault)
        scope.update(SCOPE)
        assert (tmp_path / "persistence/data.enc").read_bytes() == original
        source.execute(request("head"), owner())
        if target.exists():
            with target_at(target) as copied:
                assert copied.copy_verified_cutover_pending


def test_copy_key_identity_bad_body_and_exhausted_budget_do_not_change_source(tmp_path):
    path = tmp_path / "persistence/data.enc"
    target = tmp_path / "candidate/data.enc"
    with store_at(tmp_path) as source:
        original = path.read_bytes()
        for key, key_id in ((KEY, "different-id"), (NEXT_KEY, "persistence-key")):
            with pytest.raises(Error):
                source.copy_to(str(target), key, key_id)
        # A new key copy does not reset or consume the exhausted original key budget.
        source._encrypted._uses = 1 << 20
        source.copy_to(str(target), NEXT_KEY, "new-key")
        assert source._encrypted._uses == 1 << 20
        with pytest.raises(Error, match="capacity_exceeded"):
            source.execute(plan()[0], owner())
        assert path.read_bytes() == original
    for key, identity in ((b"w" * 32, IDENTITY), (KEY, dict(IDENTITY, sourceGeneration="2"))):
        with pytest.raises(Error):
            FileStore(str(path), key, "persistence-key", identity, lambda: SCOPE, mode="reopen")
        assert path.read_bytes() == original
    corrupt = original[:-1] + bytes([original[-1] ^ 1])
    path.write_bytes(corrupt)
    with pytest.raises(Error):
        store_at(tmp_path, mode="reopen")
    assert path.read_bytes() == corrupt


def test_demo_new_profile_copy_is_explicit_offline_and_never_starts_executor(tmp_path, capsys):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "demo/src"))
    publication = importlib.import_module("tansr_demo.publication")
    who = owner()
    config = dict(identity=dict(scope={key: IDENTITY[key] for key in ("applicationScopeId", "endUserId")},
        **{key: IDENTITY[key] for key in ("sourceId", "sourceGeneration", "domainKey")}),
        sessionId=who["sessionId"], binding=who["binding"], connection=dict(executorId="device"),
        workspace=dict(workspaceId="workspace", revision="1"))
    credentials = tmp_path / "credentials"
    with PrivateDirectory(str(credentials), create=True) as private:
        for name, value in {"token": b"synthetic", "scope": strict_json.dumps(SCOPE), "config": strict_json.dumps(config),
                            "key": KEY.hex().encode(), "journal-key": (b"j" * 32).hex().encode(),
                            "new-key": NEXT_KEY.hex().encode()}.items():
            private.write(name, value)
    with store_at(tmp_path):
        pass
    args = publication.build_parser().parse_args([
        "--profile", "terminal-persistence-v1", "--mode", "copy-publication", "--token-file", str(credentials / "token"),
        "--scope-file", str(credentials / "scope"), "--config", str(credentials / "config"),
        "--file", str(tmp_path / "persistence/data.enc"), "--journal-file", str(tmp_path / "journal/data.enc"),
        "--key-file", str(credentials / "key"), "--journal-key-file", str(credentials / "journal-key"),
        "--key-id", "persistence-key", "--journal-key-id", "journal", "--target-file", str(tmp_path / "candidate/data.enc"),
        "--target-key-file", str(credentials / "new-key"), "--target-key-id", "new-key"])
    publication.run(args)
    output = capsys.readouterr().out
    result = [json.loads(line) for line in output.splitlines() if line.startswith("{")][0]
    assert result["readOnly"] and result["cutover"] == "pending"
    assert not (tmp_path / "journal/data.enc").exists()
    with target_at(tmp_path / "candidate/data.enc") as copied:
        assert copied.copy_verified_cutover_pending


def test_encrypted_profile_cannot_opt_out_of_encrypted_receipt_journal(tmp_path):
    from types import SimpleNamespace
    from tansr_sdk.terminal_persistence import Host
    with store_at(tmp_path) as store:
        adapter = Host(store, require_encryption=False)
        with pytest.raises(Error, match="unsupported"):
            adapter.check_journal(SimpleNamespace(encrypted_at_rest=False))
        adapter.check_journal(SimpleNamespace(encrypted_at_rest=True))


def test_copy_requires_original_reopen_after_source_commit_uncertainty(tmp_path):
    prepared = plan()
    target = tmp_path / "candidate/data.enc"
    with store_at(tmp_path) as source:
        source.execute(prepared[0], owner())
        for put in prepared[1]:
            source.execute(put, owner())
        def fault(stage):
            if stage == "replaced":
                raise OSError("source return lost")
        source._hook = fault
        with pytest.raises(Error, match="storage_unknown"):
            source.execute(prepared[2], owner())
        with pytest.raises(Error, match="storage_unknown"):
            source.copy_to(str(target), NEXT_KEY, "new-key")
        assert not target.exists()
    with store_at(tmp_path, mode="reopen") as source:
        source.copy_to(str(target), NEXT_KEY, "new-key")
    with target_at(target) as copied:
        assert copied.execute(dict(prepared[2], action="query"), owner())["transfer"]["status"] == "committed"
