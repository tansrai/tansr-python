"""有界的私有平面目录；所有路径操作以已验证句柄为依据。"""
import contextlib
import os
import re
import sys
import threading
from typing import Callable, List, Optional

from ..errors import Error
from ..lifecycle import CancellationToken, now_ms

if sys.platform == "win32":
    from ._windows import Backend
else:
    from ._posix import Backend


_CHUNK = 1024 * 1024
_GATE = ".tansr-directory.lock"
_DEVICE = re.compile(r"^(?:CON|PRN|AUX|NUL|COM[1-9¹²³]|LPT[1-9¹²³])(?:\.|$)", re.I)


def safe_name(name: str) -> str:
    """Windows/Unix 一致的单文件名；不接受路径、ADS、设备名或内部保留名。"""
    if (not isinstance(name, str) or not name or name in (".", "..") or
            name[-1:] in (".", " ") or any(c in name for c in '/\\:\x00<>"|?*') or
            any(ord(c) < 32 for c in name) or _DEVICE.match(name) or
            name.lower().startswith(".tansr-")):
        raise Error("permission", "unsafe private storage basename")
    try:
        length = len(name.encode("utf-8", "strict"))
    except UnicodeError as exc:
        raise Error("permission", "unsafe private storage basename") from exc
    if length > 200:
        raise Error("capacity", "private storage basename too long")
    return name


def _path(path):
    path = os.fspath(path)
    if not isinstance(path, str) or not os.path.isabs(path) or "\x00" in path:
        raise Error("permission", "private storage requires an absolute path")
    # normpath 之前拒绝 dot 段；Win32 设备空间、UNC 和 ADS 不属于本地私有根。
    if any(part in (".", "..") for part in re.split(r"[/\\]", path)):
        raise Error("permission", "private storage dot path rejected")
    if os.name == "nt":
        drive, tail = os.path.splitdrive(path)
        if len(drive) != 2 or drive[1] != ":" or ":" in tail:
            raise Error("permission", "private storage requires a local drive path")
    path = os.path.normpath(path)
    safe_name(os.path.basename(path))
    return path


def _bound(value, maximum, name):
    if type(value) is not int or value < 1 or value > maximum:
        raise Error("capacity", "invalid private storage " + name)
    return value


def _cancel(cancel, deadline_ms):
    if cancel is not None:
        cancel.check(deadline_ms)
    elif deadline_ms is not None and now_ms() >= deadline_ms:
        raise Error("timeout")


class DirectoryLock:
    """持有期间不可替换/删除锁文件；释放不删除锁文件以免并存两把锁。"""
    def __init__(self, directory, name):
        self._directory = directory
        self.name = safe_name(name)
        self._handle = None
        self._identity = None

    def __enter__(self):
        directory = self._directory
        with directory._operation():
            if self._handle is not None:
                raise Error("reentrant", "storage lock already entered")
            try:
                directory._identity_at(self.name)
                added_files = 0
            except FileNotFoundError:
                added_files = 1
            directory._capacity(0, added_files)
            handle = directory._backend.open(self.name, create=True, writable=True)
            try:
                directory._backend.lock(handle)
                self._identity = directory._backend.identity(handle)
                self._handle = handle
                directory._locks.append(self)
                directory._verify()
            except BaseException:
                if self in directory._locks:
                    directory._locks.remove(self)
                self._handle = None
                directory._backend.close_file(handle)
                raise
        return self

    def _verify(self):
        if self._handle is None:
            raise Error("closed", "storage lock is not held")
        backend = self._directory._backend
        backend.check(self._handle)
        other = backend.open(self.name)
        try:
            if backend.identity(other) != self._identity:
                raise Error("permission", "storage lock file was replaced")
        finally:
            backend.close_file(other)

    def _release(self):
        if self._handle is not None:
            handle, self._handle = self._handle, None
            try:
                self._directory._backend.unlock(handle)
            finally:
                try:
                    self._directory._backend.close_file(handle)
                finally:
                    if self in self._directory._locks:
                        self._directory._locks.remove(self)

    def close(self):
        directory = self._directory
        with directory._mutex:
            if directory._entered:
                raise Error("reentrant", "storage lock release during callback rejected")
            self._release()

    def __exit__(self, *unused):
        self.close()


class PrivateDirectory:
    """私有目录，不隐式修复已有权限，也不隐式创建父目录。

    check_access 是宿主当前身份授权回调，返回 False 或抛错即拒绝；None
    仅表示本层只核验 OS 身份/权限，业务调用者仍须提供实时授权。
    总量上限包括旧快照、临时副本和未清理残件，不能透支恢复空间。
    """
    def __init__(self, path, *, create: bool = False,
                 check_access: Optional[Callable[[], object]] = None,
                 max_file_bytes: int = 64 << 20, max_total_bytes: int = 256 << 20,
                 max_files: int = 4096) -> None:
        self.path = _path(path)
        self.max_file_bytes = _bound(max_file_bytes, 256 << 20, "file bound")
        self.max_total_bytes = _bound(max_total_bytes, 1 << 40, "total bound")
        self.max_files = _bound(max_files, 1000000, "file count")
        if check_access is not None and not callable(check_access):
            raise Error("permission", "invalid storage authorization callback")
        self._access = check_access
        self._mutex = threading.RLock()
        self._entered = False
        self._closed = False
        self._uncertain = False
        self._locks: List[DirectoryLock] = []
        self._gate: Optional[int] = None
        self._authorize()
        self._backend = Backend(self.path, create)
        try:
            self._verify()
        except BaseException:
            self._backend.close()
            raise

    @property
    def uncertain(self) -> bool:
        return self._uncertain

    def _authorize(self):
        if self._access is not None:
            try:
                result = self._access()
            except Error:
                raise
            except Exception as exc:
                raise Error("permission", "storage authorization rejected") from exc
            if result is False:
                raise Error("permission", "storage authorization revoked")

    def _verify(self):
        if self._closed:
            raise Error("closed", "private storage closed")
        if self._uncertain:
            raise Error("unknown", "storage commit uncertain; close and reopen")
        self._authorize()
        try:
            self._backend.verify()
            if self._gate is not None:
                self._backend.check(self._gate)
                if self._identity_at(_GATE) != self._backend.identity(self._gate):
                    raise Error("permission", "private storage directory lock was replaced")
            for lock in self._locks:
                lock._verify()
        except OSError as exc:
            raise Error("permission", "private storage directory unavailable or replaced") from exc

    @contextlib.contextmanager
    def _operation(self, cancel=None, deadline_ms=None):
        # 不在等待磁盘事务时释放资格；close 也等待实际事务完成。
        while not self._mutex.acquire(timeout=0.05):
            _cancel(cancel, deadline_ms)
        gate = None
        entered = False
        try:
            if self._entered:
                raise Error("reentrant", "private storage callback reentry rejected")
            self._entered = entered = True
            _cancel(cancel, deadline_ms)
            self._verify()
            gate = self._backend.open(_GATE, create=True, writable=True)
            self._backend.lock(gate)
            self._gate = gate
            self._verify()
            yield
        except OSError as exc:
            if isinstance(exc, (FileNotFoundError, FileExistsError)):
                raise
            raise Error("io", "private storage operation failed") from exc
        finally:
            try:
                if gate is not None:
                    self._backend.close_file(gate)  # 关闭本 fd/HANDLE 释放其 OS 锁。
            finally:
                if entered:
                    self._gate = None
                    self._entered = False
                self._mutex.release()

    def _identity_at(self, name):
        handle = self._backend.open(name)
        try:
            return self._backend.identity(handle)
        finally:
            self._backend.close_file(handle)

    def _writable_target(self, name):
        handle = self._backend.open(name, writable=True)
        try:
            # 目录门锁阻止其它 SDK 实例在此核查与替换之间新认领同名锁。
            self._backend.lock(handle)
            self._backend.unlock(handle)
            return self._backend.identity(handle)
        finally:
            self._backend.close_file(handle)

    def _capacity(self, added_bytes, added_files):
        total, count = 0, added_files
        if count > self.max_files:
            raise Error("capacity", "private storage file count exceeded")
        # 分页式枚举一旦超出文件数便停止，不能先把未知目录载入内存。
        with contextlib.closing(self._backend.names()) as names:
            for name in names:
                count += 1
                if count > self.max_files:
                    raise Error("capacity", "private storage file count exceeded")
                handle = self._backend.open(name)
                try:
                    total += self._backend.size(handle)
                finally:
                    self._backend.close_file(handle)
                if total + added_bytes > self.max_total_bytes:
                    raise Error("capacity", "private storage total bytes exceeded")
        if total + added_bytes > self.max_total_bytes:
            raise Error("capacity", "private storage total bytes exceeded")

    def check_access(self) -> None:
        with self._operation():
            pass

    def lock(self, name: str) -> DirectoryLock:
        return DirectoryLock(self, name)

    def read(self, name: str, max_bytes: Optional[int] = None, *,
             cancel: Optional[CancellationToken] = None,
             deadline_ms: Optional[int] = None) -> bytes:
        name = safe_name(name)
        limit = self.max_file_bytes if max_bytes is None else min(
            self.max_file_bytes, _bound(max_bytes, 256 << 20, "read bound"))
        with self._operation(cancel, deadline_ms):
            handle = self._backend.open(name)
            try:
                identity = self._backend.identity(handle)
                length = self._backend.size(handle)
                if length < 0 or length > limit:
                    raise Error("capacity", "private storage read limit exceeded")
                parts, remaining = [], length
                while remaining:
                    _cancel(cancel, deadline_ms)
                    data = self._backend.read(handle, min(remaining, _CHUNK))
                    if not data:
                        raise Error("integrity", "private storage file changed during read")
                    parts.append(data)
                    remaining -= len(data)
                if self._backend.read(handle, 1) or self._backend.size(handle) != length:
                    raise Error("integrity", "private storage file grew during read")
                self._backend.check(handle)
                if self._identity_at(name) != identity:
                    raise Error("permission", "private storage file was replaced")
                self._verify()
                _cancel(cancel, deadline_ms)
                return b"".join(parts)
            finally:
                self._backend.close_file(handle)

    def exists(self, name: str) -> bool:
        name = safe_name(name)
        with self._operation():
            try:
                self._identity_at(name)
            except FileNotFoundError:
                return False
            self._verify()
            return True

    def write(self, name: str, data: bytes, *, replace: bool = True,
              cancel: Optional[CancellationToken] = None,
              deadline_ms: Optional[int] = None, hook=None) -> None:
        name = safe_name(name)
        if type(data) is not bytes:
            raise Error("invalid_argument", "storage write requires owned immutable bytes")
        if len(data) > self.max_file_bytes:
            raise Error("capacity", "private storage write limit exceeded")
        with self._operation(cancel, deadline_ms):
            if any(lock.name == name for lock in self._locks):
                raise Error("conflict", "cannot replace a held storage lock")
            try:
                self._writable_target(name)
            except FileNotFoundError:
                pass
            else:
                if not replace:
                    raise Error("conflict", "private storage target exists")
            self._capacity(len(data), 1)
            temp = ".tansr-tmp-" + os.urandom(16).hex()
            handle = self._backend.open(temp, create=True, exclusive=True, writable=True)
            identity = self._backend.identity(handle)
            committed = False
            try:
                offset = 0
                while offset < len(data):
                    _cancel(cancel, deadline_ms)
                    count = self._backend.write(handle, data[offset:offset + _CHUNK])
                    if count <= 0:
                        raise Error("io", "private storage short write")
                    offset += count
                self._hook(hook, "written")
                _cancel(cancel, deadline_ms)
                self._backend.sync_file(handle)
                self._hook(hook, "file_synced")
                self._verify()
                self._hook(hook, "before_replace")
                self._verify()
                _cancel(cancel, deadline_ms)
                self._backend.check(handle)
                if self._identity_at(temp) != identity:
                    raise Error("permission", "storage temporary file was replaced")
                # Windows MoveFileEx 要关闭不共享删除的临时句柄；提交后再核 inode。
                self._backend.close_file(handle)
                handle = None
                try:
                    self._backend.replace(temp, name, replace)
                except FileExistsError as exc:
                    raise Error("conflict", "private storage target exists") from exc
                except Error as exc:
                    if exc.code == "unknown":
                        committed = self._uncertain = True
                    raise
                committed = self._uncertain = True
                self._hook(hook, "replaced")
                self._backend.sync_directory()
                self._hook(hook, "directory_synced")
                if self._identity_at(name) != identity:
                    raise Error("permission", "storage committed file was replaced")
                self._uncertain = False
                self._verify()
                # 已提交不再检查取消；上层必须依据完成事实决定是否发送 ACK。
            except BaseException as exc:
                if committed:
                    self._uncertain = True
                    if isinstance(exc, Exception):
                        raise Error("unknown", "storage commit uncertain; close and reopen") from exc
                raise
            finally:
                if handle is not None:
                    self._backend.close_file(handle)
                if not committed:
                    # 只清理本次创建且身份仍匹配的临时项，不删除被替换的未知文件。
                    try:
                        if self._identity_at(temp) == identity:
                            self._backend.remove(temp)
                    except (OSError, Error):
                        pass

    write_atomic = write

    @staticmethod
    def _hook(hook, stage):
        if hook is not None:
            try:
                hook(stage)
            except Error:
                raise
            except Exception as exc:
                raise Error("io", "storage commit stage failed") from exc

    def remove(self, name: str, *, cancel=None, deadline_ms=None) -> None:
        name = safe_name(name)
        with self._operation(cancel, deadline_ms):
            if any(lock.name == name for lock in self._locks):
                raise Error("conflict", "cannot remove a held storage lock")
            self._writable_target(name)
            self._verify()
            _cancel(cancel, deadline_ms)
            self._uncertain = True
            try:
                self._backend.remove_durable(name)
                self._backend.sync_directory()
                self._uncertain = False
                self._verify()
            except BaseException as exc:
                self._uncertain = True
                if isinstance(exc, Exception):
                    raise Error("unknown", "storage deletion uncertain; close and reopen") from exc
                raise

    def close(self) -> None:
        with self._mutex:
            if self._entered:
                raise Error("reentrant", "storage close during callback rejected")
            if self._closed:
                return
            self._closed = True
            failure = None
            try:
                for lock in list(reversed(self._locks)):
                    try:
                        lock._release()
                    except Exception as exc:
                        if failure is None:
                            failure = exc
            finally:
                self._backend.close()
            if failure is not None:
                raise failure

    def __enter__(self):
        self.check_access()
        return self

    def __exit__(self, *unused):
        self.close()
