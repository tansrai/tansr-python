"""专用 publication/journal 的坏密文、AAD 与临时文件覆盖；不代替 Archive。"""
import base64
import copy

import pytest

from tansr_sdk import Error
from tansr_sdk.executor import EncryptedJournal
from tansr_sdk.executor._common import receipt_for
from tansr_sdk.memory_publication import FileStore
from tansr_sdk.storage import PrivateDirectory
from test_executor import SCOPE, operation
from test_memory_publication import BODY, IDENTITY, KEY, begin, chunk, commit, store_at


@pytest.mark.parametrize("kind", ["publication", "journal"])
@pytest.mark.parametrize("damage", ["wrong-key", "wrong-identity", "filename-aad", "truncated", "tag", "plaintext"])
def test_specialized_media_rejects_before_delivery_and_retains_bytes(tmp_path, kind, damage):
    path = tmp_path / kind / "data.enc"
    identity = IDENTITY if kind == "publication" else dict(scope=IDENTITY["scope"], executorId="device")
    def open_store(filename=path, key=KEY, who=identity, mode="reopen"):
        if kind == "publication":
            return FileStore(str(filename), key, "test-key", who, lambda: SCOPE, mode=mode, max_transfers=8)
        return EncryptedJournal(str(filename), key, "test-key", who, lambda: SCOPE, mode=mode)
    with open_store(mode="create") as store:
        if kind == "publication":
            begin(store)
            chunk(store)
            commit(store)
        else:
            op = operation()
            assert store.claim(op).claimed
            store.complete(op, receipt_for(op, "unknown", "execution_outcome_unknown"))
    original = path.read_bytes()
    key, who, target = KEY, copy.deepcopy(identity), path
    if damage == "wrong-key":
        key = b"z" * 32
    elif damage == "wrong-identity":
        who["scope"]["endUserId"] = "foreign"
    elif damage == "filename-aad":
        target = path.with_name("foreign.enc")
        with PrivateDirectory(str(path.parent)) as directory:
            directory.write(target.name, original)
    elif damage == "truncated":
        with PrivateDirectory(str(path.parent)) as directory:
            directory.write(path.name, original[:-7])
    elif damage == "tag":
        with PrivateDirectory(str(path.parent)) as directory:
            directory.write(path.name, original[:-1] + bytes([original[-1] ^ 1]))
    elif damage == "plaintext":
        with PrivateDirectory(str(path.parent)) as directory:
            directory.write(path.name, b'untrusted-plaintext-marker')
    expected = target.read_bytes()
    with pytest.raises(Error) as caught:
        open_store(target, key, who)
    assert target.read_bytes() == expected
    assert "untrusted-plaintext-marker" not in str(caught.value)
    assert BODY.decode("utf-8") not in str(caught.value)
    if target != path:
        assert path.read_bytes() == original


@pytest.mark.parametrize("kind", ["publication", "journal"])
def test_specialized_temporary_snapshots_are_ciphertext(tmp_path, kind):
    observed = []
    def hook(stage):
        if stage != "written":
            return
        files = list(tmp_path.rglob(".tansr-tmp-*"))
        assert files
        for path in files:
            raw = path.read_bytes()
            for marker in (BODY, base64.b64encode(BODY), KEY, b"resultJson", b"execution_outcome_unknown"):
                assert marker not in raw
            observed.append(path.name)
    if kind == "publication":
        with store_at(tmp_path, commit_hook=hook) as store:
            begin(store)
            chunk(store)
            commit(store)
    else:
        with EncryptedJournal(str(tmp_path / "journal" / "data.enc"), KEY, "key",
                              dict(scope=IDENTITY["scope"], executorId="device"), lambda: SCOPE,
                              mode="create", commit_hook=hook) as journal:
            op = operation()
            assert journal.claim(op).claimed
            journal.complete(op, receipt_for(op, "unknown", "execution_outcome_unknown"))
    assert len(observed) >= 3
    assert not list(tmp_path.rglob(".tansr-tmp-*"))
