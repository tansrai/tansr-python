"""有界加密快照中的 publication CAS 与永久 transfer 事实，不执行记忆业务。"""
import hashlib
import os
import threading
from contextlib import contextmanager
from typing import Any, Callable, Iterator, Optional

from .. import strict_json
from ..errors import Error
from ..executor._common import equal, snapshot, validate
from ..storage import EncryptedStore, PrivateDirectory
from ..storage.encrypted_store import decode_bytes, encode_bytes

MAX_BODY = 4194304
MAX_CHUNK = 12288
FORMAT = "tansr-python-memory-publication-v1"
FIELDS = ("sourceId", "sourceGeneration", "domainKey")


class PublicationError(Error):
    """已确定未提交的协议错误；未知介质结果不能转换成该类。"""


def need(value: Any, code: str = "integrity_mismatch") -> None:
    if not value:
        raise PublicationError(code)


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def owner_value(value: dict) -> dict:
    need(isinstance(value, dict) and set(value) == {"scope", "sessionId", "binding"}, "invalid_request")
    validate("Scope", value["scope"])
    validate("LegacyId", value["sessionId"])
    validate("ExecutionBinding", value["binding"])
    need(len(strict_json.dumps(value)) <= 8192, "invalid_request")
    return snapshot(value)


class FileStore:
    """使用原 PrivateDirectory 与 AES-GCM 容器；显式 create/reopen，绝不清坏卷。

    read_context 返回当前受信 Scope，不从请求推导授权。完整 owner 围栏保存于
    transfer；换连接只允许可信 authorize_recovery 显式批准只读 query。
    每次修改重写有界快照，保留终态直至容量耗尽；不承诺掉电或介质回滚防护。
    """
    atomic_durable_publication = True
    encrypted_at_rest = True

    def __init__(self, path: str, key: bytes, key_id: str, identity: dict,
                 read_context: Callable[[], dict], *, mode: str,
                 max_transfers: int, max_staging_bytes: int = 2 * MAX_BODY,
                 max_bytes: int = 32 << 20,
                 authorize_recovery: Optional[Callable[[dict], bool]] = None,
                 commit_hook: Any = None) -> None:
        need(os.path.isabs(path) and mode in ("create", "reopen") and callable(read_context), "invalid_request")
        need(isinstance(identity, dict) and set(identity) == {"scope", *FIELDS}, "invalid_request")
        need(isinstance(identity["scope"], dict) and set(identity["scope"]) ==
             {"applicationScopeId", "endUserId"}, "invalid_request")
        validate("Scope", dict(identity["scope"], authorizationRevision="1"))
        validate("MemoryPublicationRequest", dict(contract="terminal-services-v1", action="head",
                                                  **{field: identity[field] for field in FIELDS}),
                 "terminal-services-v1")
        need(type(max_transfers) is int and 1 <= max_transfers <= 1048576 and
             type(max_staging_bytes) is int and MAX_BODY <= max_staging_bytes <= 8 * MAX_BODY and
             type(max_bytes) is int and 65536 <= max_bytes <= 128 << 20 and
             (authorize_recovery is None or callable(authorize_recovery)), "invalid_request")
        self._identity, self._read_context = snapshot(identity), read_context
        self._limits = dict(maxTransfers=max_transfers, maxStagingBytes=max_staging_bytes, maxBytes=max_bytes)
        self._recovery, self._hook = authorize_recovery, commit_hook
        self._mutex = threading.RLock()
        self._entered = self._poisoned = self._closed = self._uncertain = False
        self._before = None  # type: Any
        self._encrypted = self._directory = None  # type: Any
        self._state = {}  # type: dict
        try:
            with self._run():
                self._directory = PrivateDirectory(os.path.dirname(path), create=mode == "create",
                    check_access=self._check, max_file_bytes=max_bytes + 8192,
                    max_total_bytes=3 * (max_bytes + 8192))
                self._encrypted = EncryptedStore(self._directory, os.path.basename(path), key, key_id,
                                                  max_bytes=max_bytes)
                saved = self._encrypted.load()
                if mode == "create":
                    need(saved is None, "request_conflict")
                    self._save(dict(format=FORMAT, identity=self._identity, limits=self._limits,
                                    publication=None, transfers={}), replace=False)
                else:
                    need(saved is not None, "integrity_mismatch")
                    self._validate(saved)
                    self._state = saved
        except BaseException:
            self.close()
            raise

    @property
    def identity(self) -> dict:
        return snapshot(self._identity)

    def _context(self) -> dict:
        value = snapshot(self._read_context())
        validate("Scope", value)
        if any(value[key] != expected for key, expected in self._identity["scope"].items()):
            raise Error("permission", "publication current scope rejected")
        return value

    def _check(self) -> None:
        if self._closed or self._uncertain or self._poisoned:
            raise Error("storage_unknown" if self._uncertain else "closed" if self._closed else "reentrant")
        current = self._context()
        if self._poisoned or self._before is not None and not equal(current, self._before):
            raise Error("permission", "publication context changed during operation")

    @contextmanager
    def _run(self, cancel: Any = None, deadline_ms: Optional[int] = None) -> Iterator[None]:
        from ..executor._common import cancellation
        token = cancellation(cancel)
        while not self._mutex.acquire(timeout=0.05):
            token.check(deadline_ms)
        entered = False
        try:
            if self._entered:
                self._poisoned = True
                raise Error("reentrant")
            self._entered = entered = True
            self._poisoned = False
            token.check(deadline_ms)
            self._before = self._context()
            self._check()
            yield
            self._check()
        finally:
            if entered:
                self._before = None
                self._entered = False
            self._mutex.release()

    def _save(self, state: dict, replace: bool = True) -> None:
        # 容量拒绝在写入前发生，旧原键事实仍可查。I/O或提交后复验失证必须重开对账。
        try:
            strict_json.dumps(state, max_bytes=self._limits["maxBytes"])
        except Error:
            raise PublicationError("capacity_exceeded", "publication snapshot capacity exceeded") from None
        self._check()
        try:
            self._encrypted.save(state, replace=replace, hook=self._hook)
            self._check()
        except BaseException:
            self._uncertain = True
            raise Error("storage_unknown", "publication commit requires reopen and original transfer query") from None
        self._state = state

    def _validate(self, state: dict) -> None:
        need(set(state) == {"format", "identity", "limits", "publication", "transfers"})
        need(state["format"] == FORMAT and equal(state["identity"], self._identity) and
             equal(state["limits"], self._limits))
        publication = state["publication"]
        if publication is not None:
            need(isinstance(publication, dict) and set(publication) == {"etag", "body"})
            body = decode_bytes(publication["body"], max_bytes=MAX_BODY)
            need(bool(body) and digest(body) == publication["etag"])
            try:
                body.decode("utf-8", "strict")
            except UnicodeError:
                need(False)
        transfers = state["transfers"]
        need(isinstance(transfers, dict) and len(transfers) <= self._limits["maxTransfers"])
        staging = 0
        for key, row in transfers.items():
            need(isinstance(row, dict) and set(row) == {"request", "owner", "status", "received", "body", "etag"})
            request = row["request"]
            validate("MemoryPublicationRequest", request, "terminal-services-v1")
            need(request["action"] == "begin" and request["transferId"] == key and
                 all(request[field] == self._identity[field] for field in FIELDS))
            owner = owner_value(row["owner"])
            need(all(owner["scope"][field] == value for field, value in self._identity["scope"].items()))
            need(isinstance(row["received"], int) and not isinstance(row["received"], bool) and 0 <= row["received"] <= request["byteLength"])
            if row["status"] == "staging":
                need(row["etag"] is None)
                body = decode_bytes(row["body"], max_bytes=MAX_BODY)
                need(len(body) == row["received"])
                staging += request["byteLength"]  # 预留完整容量，不能靠空begin超订。
            else:
                need(row["body"] is None and (row["status"] == "committed" and
                     row["received"] == request["byteLength"] and row["etag"] == request["sha256"] or
                     row["status"] == "conflict" and row["etag"] is None))
        need(staging <= self._limits["maxStagingBytes"])

    def _load(self) -> dict:
        saved = self._encrypted.load()
        need(isinstance(saved, dict))
        self._validate(saved)
        self._state = saved
        return saved

    def capacity(self) -> dict:
        with self._run():
            state = self._load()
            count = len(state["transfers"])
            staging = sum(row["request"]["byteLength"] for row in state["transfers"].values()
                          if row["status"] == "staging")
            return dict(self._limits, storedTransfers=count,
                        remainingTransfers=self._limits["maxTransfers"] - count, stagingBytes=staging,
                        remainingStagingBytes=self._limits["maxStagingBytes"] - staging,
                        storedBytes=len(strict_json.dumps(state, max_bytes=self._limits["maxBytes"])))

    def execute(self, request: dict, owner: dict, *, cancel: Any = None,
                deadline_ms: Optional[int] = None) -> dict:
        request, owner = snapshot(request), owner_value(snapshot(owner))
        validate("MemoryPublicationRequest", request, "terminal-services-v1")
        need(all(request[field] == self._identity[field] for field in FIELDS), "stale_generation")
        with self._run(cancel, deadline_ms):
            need(equal(owner["scope"], self._before), "request_conflict")
            state = self._load()
            response = dict(contract="terminal-services-v1", action=request["action"],
                            **{field: request[field] for field in FIELDS})
            result = self._execute(state, request, owner, response)
            validate("MemoryPublicationResponse", result, "terminal-services-v1")
            return result

    def _execute(self, state: dict, request: dict, owner: dict, response: dict) -> dict:
        action, publication = request["action"], state["publication"]
        if action == "head":
            response["publication"] = None if publication is None else dict(
                etag=publication["etag"], sha256=publication["etag"],
                byteLength=len(decode_bytes(publication["body"], max_bytes=MAX_BODY)))
            return response
        if action == "read":
            need(publication is not None and publication["etag"] == request["etag"], "revision_conflict")
            body = decode_bytes(publication["body"], max_bytes=MAX_BODY)
            need(request["offset"] <= len(body), "invalid_request")
            data = body[request["offset"]:request["offset"] + request["length"]]
            response.update(etag=publication["etag"], offset=request["offset"], byteLength=len(data),
                            base64=encode_bytes(data), payloadDigest=digest(data),
                            nextOffset=request["offset"] + len(data),
                            complete=request["offset"] + len(data) == len(body))
            return response
        transfer_id = request["transferId"]
        row = state["transfers"].get(transfer_id)
        if row is not None and not equal(row["owner"], owner):
            need(action == "query" and self._recovery is not None, "request_conflict")
            assert self._recovery is not None
            allowed = self._recovery(dict(identity=self.identity, transferId=transfer_id,
                originalOwner=snapshot(row["owner"]), currentOwner=snapshot(owner)))
            self._check()
            need(allowed is True, "request_conflict")
        changed = False
        if action == "begin":
            if row is not None:
                need(equal(row["request"], request), "request_conflict")
            else:
                staging = sum(item["request"]["byteLength"] for item in state["transfers"].values()
                              if item["status"] == "staging")
                need(len(state["transfers"]) < self._limits["maxTransfers"] and
                     staging + request["byteLength"] <= self._limits["maxStagingBytes"], "capacity_exceeded")
                row = dict(request=request, owner=owner, status="staging", received=0, body="", etag=None)
                state["transfers"][transfer_id] = row
                changed = True
        elif action == "chunk" and row is not None:
            try:
                data = decode_bytes(request["base64"], max_bytes=MAX_CHUNK)
            except Error:
                raise PublicationError("integrity_mismatch") from None
            need(len(data) == request["byteLength"] and digest(data) == request["payloadDigest"])
            need(row["status"] == "staging" and request["offset"] + len(data) <= row["request"]["byteLength"],
                 "request_conflict")
            body = decode_bytes(row["body"], max_bytes=MAX_BODY)
            if request["offset"] < row["received"]:
                need(request["offset"] + len(data) <= row["received"] and
                     body[request["offset"]:request["offset"] + len(data)] == data, "request_conflict")
            else:
                need(request["offset"] == row["received"], "request_conflict")
                row.update(body=encode_bytes(body + data), received=row["received"] + len(data))
                changed = True
        elif action == "commit" and row is not None and row["status"] == "staging":
            body = decode_bytes(row["body"], max_bytes=MAX_BODY)
            need(row["received"] == row["request"]["byteLength"] and digest(body) == row["request"]["sha256"])
            try:
                body.decode("utf-8", "strict")
            except UnicodeError:
                need(False)
            if (None if publication is None else publication["etag"]) != row["request"]["expectedEtag"]:
                row.update(status="conflict", body=None)
            else:
                state["publication"] = dict(etag=digest(body), body=encode_bytes(body))
                row.update(status="committed", body=None, etag=digest(body))
            changed = True
        if changed:
            self._save(state)
        response["transfer"] = dict(transferId=transfer_id, status="unknown" if row is None else row["status"],
            receivedBytes=None if row is None else row["received"], etag=None if row is None else row["etag"])
        return response

    def copy_to(self, path: str, key: bytes, key_id: str, *, commit_hook: Any = None) -> None:
        """保留原件，将全部事实复制到不存在的新路径；也可显式提供新密钥轮钥。"""
        with self._run():
            self._load()
            self._encrypted.copy_to(path, key, key_id, hook=commit_hook)

    def rotate_key(self, key: bytes, key_id: str) -> None:
        with self._run():
            self._load()
            try:
                self._encrypted.rotate_key(key, key_id, hook=self._hook)
                self._check()
            except BaseException:
                self._uncertain = True
                raise Error("storage_unknown", "key rotation requires reopen with original or new key") from None

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

    def __enter__(self) -> "FileStore":
        self._check()
        return self

    def __exit__(self, *unused: Any) -> None:
        self.close()
