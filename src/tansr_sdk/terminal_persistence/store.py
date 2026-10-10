"""显式独立加密布局；共同元数据事务内提交根、永久索引、原键事实和机械引用回收。"""
import base64
import copy
import hashlib
import os
import threading
from contextlib import contextmanager

from .. import strict_json
from ..errors import Error
from ..executor._common import cancellation, snapshot
from ..storage import EncryptedStore, PrivateDirectory
from ..storage.encrypted_store import _key, _separate_target, _write_new_snapshot, decode_bytes
from ._state import DEFAULT_LIMITS, FORMAT, Engine, Measurements, StorageError, initial, need
from ._wire import validate

_MAX_STATE = 64 << 20
_MAX_NODES = 2000000
_FIELDS = ("sourceId", "sourceGeneration", "domainKey")


class FileStore:
    """原 PrivateDirectory/AES-GCM 的独立 profile，终态票据和双键值永久保留。

    一个进程持有原介质锁。每次变更重写有界加密快照，正文/页只回收共同根与
    活跃 transfer 均不再引用的对象。未声明备份、任意 delete、回滚检测或跨机接管。
    read_context 与 query-only authorize_recovery 必须来自宿主；请求不产生权限。
    """
    atomic_durable_persistence = True
    encrypted_at_rest = True

    def __init__(self, path, key, key_id, identity, read_context, *, mode,
                 limits=None, max_snapshot_bytes=128 << 20, authorize_recovery=None, commit_hook=None):
        validate("Identity", identity)
        identity = snapshot(identity)
        limits = snapshot(DEFAULT_LIMITS if limits is None else limits)
        validate("CapacityLimits", limits)
        need(all(isinstance(value, int) and not isinstance(value, bool) and 0 < value <= DEFAULT_LIMITS[name]
                 for name, value in limits.items()), "invalid_request")
        need(os.path.isabs(path) and mode in ("create", "reopen") and callable(read_context) and
             (authorize_recovery is None or callable(authorize_recovery)) and
             type(max_snapshot_bytes) is int and 1048576 <= max_snapshot_bytes <= 128 << 20, "invalid_request")
        self._identity, self._limits = identity, limits
        self._physical_limit = max_snapshot_bytes
        self._read_context, self._recovery, self._hook = read_context, authorize_recovery, commit_hook
        self._measurements = Measurements()
        self._mutex = threading.RLock()
        self._entered = self._poisoned = self._closed = self._uncertain = False
        self._before = self._token = self._deadline = None
        self._written = False
        self._copy_only = False
        self._directory = self._encrypted = None
        try:
            with self._run():
                self._directory = PrivateDirectory(os.path.dirname(path), create=mode == "create",
                    check_access=self._check, max_file_bytes=max_snapshot_bytes + 8192,
                    max_total_bytes=3 * (max_snapshot_bytes + 8192))
                self._encrypted = EncryptedStore(self._directory, os.path.basename(path), key, key_id,
                                                  max_bytes=max_snapshot_bytes)
                self._key_digest, self._key_id = hashlib.sha256(key).digest(), key_id
                saved = self._encrypted.load()
                if mode == "create":
                    need(saved is None, "request_conflict")
                    self._save(initial(identity, limits), replace=False)
                else:
                    need(saved is not None)
                    self._unpack(saved)
        except BaseException:
            self.close()
            raise

    @property
    def identity(self):
        return snapshot(self._identity)

    def _context(self):
        scope = snapshot(self._read_context())
        validate("Scope", scope)
        if any(scope[key] != self._identity[key] for key in ("applicationScopeId", "endUserId")):
            raise Error("permission", "persistence current scope rejected")
        return scope

    def _check(self):
        if self._closed or self._uncertain or self._poisoned:
            raise Error("storage_unknown" if self._uncertain else "closed" if self._closed else "reentrant")
        if self._token is not None:
            self._token.check(self._deadline)
        scope = self._context()
        if self._poisoned or self._before is not None and scope != self._before:
            raise Error("permission", "persistence scope changed")

    @contextmanager
    def _run(self, cancel=None, deadline_ms=None):
        token = cancellation(cancel)
        while not self._mutex.acquire(timeout=0.05):
            token.check(deadline_ms)
        entered = False
        try:
            if self._entered:
                self._poisoned = True
                raise Error("reentrant")
            self._entered = entered = True
            self._token, self._deadline, self._written = token, deadline_ms, False
            self._copy_written = False
            token.check(deadline_ms)
            self._before = self._context()
            self._check()
            yield
            try:
                self._check()
            except BaseException:
                if self._copy_written:
                    raise Error("storage_unknown", "copy requires original target reopen; source retained") from None
                if self._written:
                    self._uncertain = True
                    raise Error("storage_unknown", "reopen and query original transfer") from None
                raise
        finally:
            if entered:
                self._before = self._token = self._deadline = None
                self._entered = False
            self._mutex.release()

    def _pack(self, state, *, read_only_copy=False):
        try:
            raw = strict_json.dumps(state, max_bytes=_MAX_STATE, max_nodes=_MAX_NODES)
            packed = dict(format=FORMAT, payload=base64.b64encode(raw).decode("ascii"), physicalLimit=self._physical_limit)
            if read_only_copy:
                packed["readOnlyCopy"] = True
            strict_json.dumps(packed, max_bytes=self._physical_limit)
            return packed
        except Error:
            raise StorageError("capacity_exceeded", "persistence physical snapshot capacity") from None

    def _unpack(self, packed):
        need(type(packed) is dict and set(packed) in ({"format", "payload", "physicalLimit"},
             {"format", "payload", "physicalLimit", "readOnlyCopy"}) and
             packed["format"] == FORMAT and packed["physicalLimit"] == self._physical_limit)
        marker = "readOnlyCopy" in packed
        need(not marker or packed["readOnlyCopy"] is True)
        need(not self._copy_only or marker)
        self._copy_only = marker
        raw = decode_bytes(packed["payload"], max_bytes=_MAX_STATE)
        state = strict_json.loads(raw, max_bytes=_MAX_STATE, max_nodes=_MAX_NODES)
        Engine(state, validate, self._measurements).audit(self._identity, self._limits)
        self._check()
        return state

    def _completion_bytes(self, state, packed):
        # Frozen digest/ref shapes bound object + accepted-map overhead by 512
        # inner JSON bytes, primary + secondary rows by 768 per added entry,
        # and root/result/progress growth by 4096 per active ticket. Existing
        # begin/owner/base facts are already encoded. Count base64 padding per
        # object, then the independent outer payload base64 expansion.
        payload = packed["payload"]
        inner = len(payload) // 4 * 3 - (len(payload) - len(payload.rstrip("=")))
        maximum = inner + 128
        for row in state["transfers"].values():
            if row["transfer"]["status"] != "staging":
                continue
            begin = row["begin"]
            count = begin["declared"]["objects"] - len(row["accepted"])
            remaining = begin["declared"]["bytes"] - sum(
                state["objects"][key]["byteLength"] for key in row["accepted"])
            maximum += 4 * ((remaining + 2 * count) // 3) + 512 * count + 768 * begin["index"]["addedCount"] + 4096
        outer = len(strict_json.dumps(packed, max_bytes=self._physical_limit)) - len(payload) + 4 * ((maximum + 2) // 3)
        return maximum, outer

    def _reserve_admission(self, state):
        inner, outer = self._completion_bytes(state, self._pack(state))
        need(inner <= _MAX_STATE and outer <= self._physical_limit, "capacity_exceeded")

    def _budget(self, state, packed):
        # 原容器每钥写次数/总加密字节上限不变。先为所有未决计划预留最坏必要
        # put、commit/拒绝及这一笔写；只依已核接收/复用事实逐次释放预留。
        pending = [row for row in state["transfers"].values() if row["transfer"]["status"] == "staging"]
        writes = 1 + sum(row["begin"]["declared"]["objects"] - len(row["accepted"]) + 2 for row in pending)
        maximum = min(self._physical_limit, self._completion_bytes(state, packed)[1])
        assert self._encrypted is not None
        try:
            self._encrypted._require_write_budget(writes, writes * maximum)
        except Error as error:
            if error.code == "capacity_exceeded":
                raise StorageError(error.code) from None
            raise

    def _save(self, state, replace=True):
        packed = self._pack(state)
        self._budget(state, packed)
        self._check()
        assert self._encrypted is not None
        try:
            self._encrypted.save(packed, replace=replace, hook=self._hook,
                                  cancel=self._token, deadline_ms=self._deadline)
            self._written = True
            self._check()
        except BaseException:
            self._uncertain = True
            raise Error("storage_unknown", "reopen and query original transfer") from None

    def execute(self, request, owner, *, cancel=None, deadline_ms=None):
        request, owner = snapshot(request), snapshot(owner)
        validate("Request", request)
        validate("Owner", owner)
        need(len(strict_json.dumps(request)) <= 32768, "invalid_request")
        need(all(request[field] == self._identity[field] for field in _FIELDS), "stale_generation")
        with self._run(cancel, deadline_ms):
            need(owner["scope"] == self._before, "request_conflict")
            assert self._encrypted is not None
            saved = self._encrypted.load(cancel=self._token, deadline_ms=self._deadline)
            need(saved is not None)
            state = self._unpack(saved)
            need(not self._copy_only or request["action"] in ("head", "read", "lookup", "query"), "read_only_copy")
            new_admission = request["action"] == "begin" and request["transferId"] not in state["transfers"]
            engine = Engine(state, validate, self._measurements)
            def recovery(proof):
                self._check()
                result = self._recovery(copy.deepcopy(proof)) if self._recovery is not None else False
                self._check()
                return result
            result = engine.execute(request, owner, recovery)
            validate("Response", result)
            self._check()
            if engine.changed:
                # Reserve all active completions in the same locked snapshot. Old
                # tickets keep their original query/continuation and format.
                if new_admission:
                    self._reserve_admission(state)
                self._save(state)
            if engine.error is not None:
                raise StorageError(engine.error)
            return result

    @property
    def copy_verified_cutover_pending(self):
        """持久只读候选标记；不代表取得共同 writer 围栏，不能自动切换。"""
        return self._copy_only

    def copy_to(self, path, key, key_id, *, cancel=None, deadline_ms=None, commit_hook=None):
        """保源复制到不同钥的新路径，冷重开核验；返回只读/cutover pending 摘要。

        完整保留 root、对象、双键索引及所有票据（含 staging）。目标普通 reopen
        仍拒 begin/put/commit；本入口没有激活写入功能。失败保留源和已提交目标，
        用原目标路径/钥重开查验，不能把回包失败当成目标不存在。
        """
        _key(key, key_id)
        need(hashlib.sha256(key).digest() != self._key_digest and key_id != self._key_id, "invalid_request")
        with self._run(cancel, deadline_ms):
            assert self._encrypted is not None and self._directory is not None
            _separate_target(path, self._directory.path)
            state = self._unpack(self._encrypted.load(cancel=self._token, deadline_ms=self._deadline))
            packed = self._pack(state, read_only_copy=True)
            directory = self._directory
            def check():
                self._check()
                directory.check_access()
            # New key starts its own physical budget; the source budget and bytes
            # are untouched. The only target commit includes the read-only marker.
            _write_new_snapshot(path, packed, key, key_id, self._physical_limit, check, hook=commit_hook)
            self._copy_written = True
            try:
                check()
                with FileStore(path, key, key_id, self._identity, self._read_context, mode="reopen",
                        limits=self._limits, max_snapshot_bytes=self._physical_limit,
                        authorize_recovery=self._recovery) as target:
                    assert target._encrypted is not None
                    with target._run(cancel, deadline_ms):
                        copied = target._unpack(target._encrypted.load())
                        need(target._copy_only and copied == state)
                need(self._unpack(self._encrypted.load()) == state)
                check()
            except BaseException:
                raise Error("storage_unknown", "copy requires original target reopen; source retained") from None
            return dict(readOnly=True, cutover="pending",
                        objects=len(state["objects"]), receiptEntries=len(state["primary"]),
                        transferFacts=len(state["transfers"]))

    def capacity(self):
        with self._run():
            assert self._encrypted is not None
            state = self._unpack(self._encrypted.load())
            return Engine(state, validate, self._measurements).check_capacity()

    def close(self):
        with self._mutex:
            if self._entered:
                self._poisoned = True
                raise Error("reentrant")
            if not self._closed:
                if self._encrypted is not None:
                    self._encrypted.close()
                if self._directory is not None:
                    self._directory.close()
                self._measurements.clear()
                self._closed = True

    def __enter__(self):
        self._check()
        return self

    def __exit__(self, *unused):
        self.close()
