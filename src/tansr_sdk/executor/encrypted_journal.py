"""原claim/receipt端口的有界AES-GCM实现；不保存明文敏感结果副本。"""
import os
import threading
from contextlib import contextmanager
from typing import Any, Callable, Iterator

from .. import strict_json
from ..errors import Error
from ..storage import EncryptedStore, PrivateDirectory
from ._common import cancellation, equal, snapshot, validate, validate_receipt
from .journal import Claim, journal_key

FORMAT = "tansr-python-encrypted-execution-journal-v1"


class EncryptedJournal:
    """目录、全快照、锁均由本对象持有；关闭保留原claim/永久回执。

    identity只绑定app/user/executor；每次操作仍核对受信当前authorizationRevision。
    使用独立密钥与独立路径，不自动迁移原明文FileJournal，也不清空满额日志。
    """
    encrypted_at_rest = True

    def __init__(self, path: str, key: bytes, key_id: str, identity: dict,
                 read_context: Callable[[], dict], *, mode: str,
                 max_records: int = 4096, max_bytes: int = 32 << 20,
                 commit_hook: Any = None) -> None:
        if (not os.path.isabs(path) or mode not in ("create", "reopen") or not callable(read_context) or
                type(max_records) is not int or not 1 <= max_records <= 1048576 or
                type(max_bytes) is not int or not 65536 <= max_bytes <= 128 << 20 or
                not isinstance(identity, dict) or set(identity) != {"scope", "executorId"} or
                not isinstance(identity["scope"], dict) or
                set(identity["scope"]) != {"applicationScopeId", "endUserId"}):
            raise Error("invalid_argument", "encrypted journal options")
        validate("Scope", dict(identity["scope"], authorizationRevision="1"))
        validate("LegacyId", identity["executorId"])
        self._identity, self._context = snapshot(identity), read_context
        self._limits = dict(maxRecords=max_records, maxBytes=max_bytes)
        self._mutex = threading.RLock()
        self._entered = self._poisoned = self._closed = self._uncertain = False
        self._before = None  # type: Any
        self._directory = self._encrypted = None  # type: Any
        self._hook = commit_hook
        try:
            with self._run():
                self._directory = PrivateDirectory(os.path.dirname(path), create=mode == "create",
                    check_access=self._check, max_file_bytes=max_bytes + 8192,
                    max_total_bytes=3 * (max_bytes + 8192))
                self._encrypted = EncryptedStore(self._directory, os.path.basename(path), key, key_id,
                                                  max_bytes=max_bytes)
                value = self._encrypted.load()
                if mode == "create":
                    if value is not None:
                        raise Error("conflict", "journal already exists")
                    self._save(dict(format=FORMAT, identity=self._identity, limits=self._limits, records={}), False)
                else:
                    self._validate(value)
        except BaseException:
            self.close()
            raise

    def _check(self) -> None:
        if self._closed or self._uncertain or self._poisoned:
            raise Error("storage_unknown" if self._uncertain else "closed" if self._closed else "reentrant")
        scope = snapshot(self._context())
        validate("Scope", scope)
        if (self._poisoned or any(scope[key] != value for key, value in self._identity["scope"].items()) or
                self._before is not None and not equal(scope, self._before)):
            raise Error("permission", "journal context changed")

    @contextmanager
    def _run(self, operation: Any = None, cancel: Any = None) -> Iterator[None]:
        token = cancellation(cancel)
        while not self._mutex.acquire(timeout=0.05):
            token.check()
        entered = False
        try:
            if self._entered:
                self._poisoned = True
                raise Error("reentrant")
            self._entered = entered = True
            self._poisoned = False
            token.check()
            self._before = snapshot(self._context())
            self._check()
            if operation is not None and (not equal(operation["scope"], self._before) or
                    operation["binding"]["target"]["executorId"] != self._identity["executorId"]):
                raise Error("permission", "journal operation owner rejected")
            yield
            self._check()
        finally:
            if entered:
                self._before = None
                self._entered = False
            self._mutex.release()

    def _validate(self, state: Any) -> dict:
        if (not isinstance(state, dict) or set(state) != {"format", "identity", "limits", "records"} or
                state["format"] != FORMAT or not equal(state["identity"], self._identity) or
                not equal(state["limits"], self._limits) or not isinstance(state["records"], dict) or
                len(state["records"]) > self._limits["maxRecords"]):
            raise Error("integrity", "invalid encrypted execution journal")
        for key, record in state["records"].items():
            validate("Digest", key)
            if not isinstance(record, dict) or set(record) != {"digest", "receipt"}:
                raise Error("integrity", "invalid execution record")
            validate("Digest", record["digest"])
            if record["receipt"] is not None:
                validate("ExecutionReceiptRequest", record["receipt"])
        return state

    def _save(self, state: dict, replace: bool = True) -> None:
        try:
            strict_json.dumps(state, max_bytes=self._limits["maxBytes"])
        except Error:
            raise Error("capacity", "encrypted journal snapshot capacity exceeded") from None
        self._check()
        try:
            self._encrypted.save(state, replace=replace, hook=self._hook)
            self._check()
        except BaseException:
            self._uncertain = True
            raise Error("storage_unknown", "journal commit requires reopen") from None

    def claim(self, operation: dict, cancel: Any = None) -> Claim:
        operation = snapshot(operation)
        key = journal_key(operation)
        with self._run(operation, cancel):
            state = self._validate(self._encrypted.load())
            row = state["records"].get(key)
            if row is not None:
                if row["digest"] != operation["digest"]:
                    raise Error("conflict", "journal operation changed")
                if row["receipt"] is not None:
                    validate_receipt(operation, row["receipt"])
                return Claim(False, row["receipt"])
            if len(state["records"]) >= self._limits["maxRecords"]:
                raise Error("capacity", "journal retains permanent operation facts")
            state["records"][key] = dict(digest=operation["digest"], receipt=None)
            self._save(state)
            return Claim(True)

    def complete(self, operation: dict, receipt: dict, cancel: Any = None) -> None:
        operation, receipt = snapshot(operation), snapshot(receipt)
        validate_receipt(operation, receipt)
        key = journal_key(operation)
        with self._run(operation, cancel):
            state = self._validate(self._encrypted.load())
            row = state["records"].get(key)
            if row is None or row["digest"] != operation["digest"]:
                raise Error("conflict", "receipt requires original durable claim")
            if row["receipt"] is not None:
                if not equal(row["receipt"], receipt):
                    raise Error("conflict", "immutable execution receipt")
                return
            row["receipt"] = receipt
            self._save(state)

    def copy_to(self, path: str, key: bytes, key_id: str, *, commit_hook: Any = None) -> None:
        """保留原件，将全部事实复制到不存在的新路径；也可显式提供新密钥轮钥。"""
        with self._run():
            self._validate(self._encrypted.load())
            self._encrypted.copy_to(path, key, key_id, hook=commit_hook)

    def rotate_key(self, key: bytes, key_id: str) -> None:
        with self._run():
            self._validate(self._encrypted.load())
            try:
                self._encrypted.rotate_key(key, key_id, hook=self._hook)
                self._check()
            except BaseException:
                self._uncertain = True
                raise Error("storage_unknown", "journal key rotation requires reopen") from None

    def close(self) -> None:
        with self._mutex:
            if self._entered:
                self._poisoned = True
                raise Error("reentrant")
            if not self._closed:
                if self._encrypted is not None:
                    self._encrypted.close()
                if self._directory is not None:
                    self._directory.close()
                self._closed = True

    def __enter__(self) -> "EncryptedJournal":
        self._check()
        return self

    def __exit__(self, *unused: Any) -> None:
        self.close()


def copy_plaintext(source, path, key, key_id, identity, read_context,
                   operations, max_records, max_bytes, commit_hook):
    """旧格式只保存摘要/原键；完整清单证明归属且禁止遗漏任何耐久认领。"""
    from ..storage.encrypted_store import _separate_target, _write_new_snapshot
    from .journal import FileJournal
    if (not isinstance(source, FileJournal) or not isinstance(identity, dict) or
            set(identity) != {"scope", "executorId"} or not isinstance(identity["scope"], dict) or
            set(identity["scope"]) != {"applicationScopeId", "endUserId"} or not callable(read_context) or
            type(max_records) is not int or not 1 <= max_records <= 1048576 or
            type(max_bytes) is not int or not 65536 <= max_bytes <= 128 << 20 or
            not isinstance(operations, list) or len(operations) > max_records):
        raise Error("invalid_argument", "invalid plaintext journal migration options")
    identity = snapshot(identity)
    validate("Scope", dict(identity["scope"], authorizationRevision="1"))
    validate("LegacyId", identity["executorId"])
    before = snapshot(read_context())
    validate("Scope", before)
    if any(before[field] != value for field, value in identity["scope"].items()):
        raise Error("permission", "journal migration current scope rejected")
    _separate_target(path, source._directory.path)

    def check():
        if source._closed:
            raise Error("closed")
        current = snapshot(read_context())
        validate("Scope", current)
        if not equal(current, before):
            raise Error("permission", "journal migration context changed")
        source._directory.check_access()

    records, names = {}, set()
    operations = snapshot(operations)
    with source._mutex:
        check()
        with source._directory.lock("executor-journal.lock"):
            for operation in operations:
                record_key = journal_key(operation)
                if (operation["binding"]["target"]["executorId"] != identity["executorId"] or
                        any(operation["scope"][field] != value for field, value in identity["scope"].items())):
                    raise Error("permission", "journal migration operation identity rejected")
                name = record_key + ".execution.json"
                if name in names:
                    raise Error("conflict", "duplicate journal migration operation")
                names.add(name)
                try:
                    value = source._read(name, operation)
                except FileNotFoundError:
                    raise Error("conflict", "journal migration operation missing") from None
                records[record_key] = dict(digest=value["digest"], receipt=value["receipt"])
            if set(source._directory.names()) != names | {"executor-journal.lock"}:
                raise Error("conflict", "journal migration requires every original operation and no foreign files")
            state = dict(format=FORMAT, identity=identity,
                         limits=dict(maxRecords=max_records, maxBytes=max_bytes), records=records)
            check()
            _write_new_snapshot(path, state, key, key_id, max_bytes, check, commit_hook)
