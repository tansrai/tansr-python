"""仅 Windows 导入；按已打开句柄核验所有者、DACL、reparse 与硬链接。"""
import ctypes
import os
import sys
from ctypes import wintypes as w

from ..errors import Error

assert sys.platform == "win32"


_kernel = ctypes.WinDLL("kernel32", use_last_error=True)
_advapi = ctypes.WinDLL("advapi32", use_last_error=True)
_INVALID = ctypes.c_void_p(-1).value
_PTR = ctypes.c_void_p


class _Info(ctypes.Structure):
    _fields_ = [("attributes", w.DWORD), ("created", w.FILETIME),
                ("accessed", w.FILETIME), ("written", w.FILETIME),
                ("volume", w.DWORD), ("size_high", w.DWORD),
                ("size_low", w.DWORD), ("links", w.DWORD),
                ("index_high", w.DWORD), ("index_low", w.DWORD)]


class _Security(ctypes.Structure):
    _fields_ = [("length", w.DWORD), ("descriptor", _PTR), ("inherit", w.BOOL)]


class _Acl(ctypes.Structure):
    _fields_ = [("revision", w.BYTE), ("reserved", w.BYTE), ("size", w.WORD),
                ("count", w.WORD), ("reserved2", w.WORD)]


class _Ace(ctypes.Structure):
    _fields_ = [("kind", w.BYTE), ("flags", w.BYTE), ("size", w.WORD),
                ("mask", w.DWORD), ("sid_start", w.DWORD)]


class _Overlapped(ctypes.Structure):
    _fields_ = [("internal", ctypes.c_size_t), ("internal_high", ctypes.c_size_t),
                ("offset", w.DWORD), ("offset_high", w.DWORD), ("event", w.HANDLE)]


def _bind(dll, name, args, result):
    function = getattr(dll, name)
    function.argtypes, function.restype = args, result
    return function


_create = _bind(_kernel, "CreateFileW", [w.LPCWSTR, w.DWORD, w.DWORD, _PTR,
                                      w.DWORD, w.DWORD, w.HANDLE], w.HANDLE)
_close = _bind(_kernel, "CloseHandle", [w.HANDLE], w.BOOL)
_info = _bind(_kernel, "GetFileInformationByHandle", [w.HANDLE, ctypes.POINTER(_Info)], w.BOOL)
_read = _bind(_kernel, "ReadFile", [w.HANDLE, _PTR, w.DWORD, ctypes.POINTER(w.DWORD), _PTR], w.BOOL)
_write = _bind(_kernel, "WriteFile", [w.HANDLE, _PTR, w.DWORD, ctypes.POINTER(w.DWORD), _PTR], w.BOOL)
_flush = _bind(_kernel, "FlushFileBuffers", [w.HANDLE], w.BOOL)
_move = _bind(_kernel, "MoveFileExW", [w.LPCWSTR, w.LPCWSTR, w.DWORD], w.BOOL)
_delete = _bind(_kernel, "DeleteFileW", [w.LPCWSTR], w.BOOL)
_mkdir = _bind(_kernel, "CreateDirectoryW", [w.LPCWSTR, ctypes.POINTER(_Security)], w.BOOL)
_lock = _bind(_kernel, "LockFileEx", [w.HANDLE, w.DWORD, w.DWORD, w.DWORD,
                                  w.DWORD, ctypes.POINTER(_Overlapped)], w.BOOL)
_unlock = _bind(_kernel, "UnlockFileEx", [w.HANDLE, w.DWORD, w.DWORD,
                                      w.DWORD, ctypes.POINTER(_Overlapped)], w.BOOL)
_process = _bind(_kernel, "GetCurrentProcess", [], w.HANDLE)
_local_free = _bind(_kernel, "LocalFree", [_PTR], _PTR)
_token_open = _bind(_advapi, "OpenProcessToken", [w.HANDLE, w.DWORD, ctypes.POINTER(w.HANDLE)], w.BOOL)
_token_info = _bind(_advapi, "GetTokenInformation", [w.HANDLE, ctypes.c_int, _PTR,
                                                  w.DWORD, ctypes.POINTER(w.DWORD)], w.BOOL)
_sid_text = _bind(_advapi, "ConvertSidToStringSidW", [_PTR, ctypes.POINTER(_PTR)], w.BOOL)
_sddl = _bind(_advapi, "ConvertStringSecurityDescriptorToSecurityDescriptorW",
              [w.LPCWSTR, w.DWORD, ctypes.POINTER(_PTR), _PTR], w.BOOL)
_security = _bind(_advapi, "GetSecurityInfo", [w.HANDLE, ctypes.c_int, w.DWORD,
                                            ctypes.POINTER(_PTR), _PTR,
                                            ctypes.POINTER(_PTR), _PTR,
                                            ctypes.POINTER(_PTR)], w.DWORD)
_get_ace = _bind(_advapi, "GetAce", [_PTR, w.DWORD, ctypes.POINTER(_PTR)], w.BOOL)
_equal_sid = _bind(_advapi, "EqualSid", [_PTR, _PTR], w.BOOL)


def _fail(code=None):
    code = ctypes.get_last_error() if code is None else code
    if code in (2, 3):
        raise FileNotFoundError(code, "private storage item unavailable")
    if code in (80, 183):
        raise FileExistsError(code, "private storage item exists")
    if code in (5, 32, 33, 4390):
        raise Error("permission", "private storage handle access rejected")
    raise Error("io", "native private storage operation failed", detail={"osCode": code})


def _user():
    token = w.HANDLE()
    if not _token_open(_process(), 0x0008, ctypes.byref(token)):
        _fail()
    try:
        size = w.DWORD()
        _token_info(token, 1, None, 0, ctypes.byref(size))
        if not size.value:
            _fail()
        buffer = ctypes.create_string_buffer(size.value)
        if not _token_info(token, 1, buffer, size, ctypes.byref(size)):
            _fail()
        # TOKEN_USER 的首字段 SID_AND_ATTRIBUTES 的首字段为 PSID。
        sid = ctypes.cast(buffer, ctypes.POINTER(_PTR))[0]
        return buffer, sid
    finally:
        _close(token)


def _descriptor(directory):
    buffer, sid = _user()
    text = _PTR()
    if not _sid_text(sid, ctypes.byref(text)):
        _fail()
    try:
        owner = ctypes.wstring_at(text)
        descriptor = _PTR()
        value = "O:{}D:P(A;{};FA;;;{})".format(owner, "OICI" if directory else "", owner)
        if not _sddl(value, 1, ctypes.byref(descriptor), None):
            _fail()
        return descriptor
    finally:
        _local_free(text)


def _private_acl(handle):
    owner, acl, descriptor = _PTR(), _PTR(), _PTR()
    result = _security(handle, 1, 0x00000001 | 0x00000004, ctypes.byref(owner),
                       None, ctypes.byref(acl), None, ctypes.byref(descriptor))
    if result:
        _fail(result)
    try:
        buffer, sid = _user()
        if not owner or not acl or not _equal_sid(owner, sid):
            raise Error("permission", "private storage owner or DACL rejected")
        # OWNER RIGHTS(S-1-3-4) 与当前用户可授权；不默认放行 Administrators/SYSTEM。
        rights = ctypes.create_string_buffer(b"\x01\x01\x00\x00\x00\x00\x00\x03\x04\x00\x00\x00")
        count = ctypes.cast(acl, ctypes.POINTER(_Acl)).contents.count
        for index in range(count):
            raw = _PTR()
            if not _get_ace(acl, index, ctypes.byref(raw)):
                _fail()
            if raw.value is None:
                raise Error("permission", "private storage invalid ACL entry")
            ace = ctypes.cast(raw, ctypes.POINTER(_Ace)).contents
            if ace.kind == 1:  # ACCESS_DENIED_ACE_TYPE 不增加权限。
                continue
            allowed = raw.value + _Ace.sid_start.offset
            if ace.kind != 0 or not (_equal_sid(allowed, sid) or _equal_sid(allowed, rights)):
                raise Error("permission", "private storage broad or unsupported ACE rejected")
    finally:
        _local_free(descriptor)


def _check(handle, directory=False, private=True):
    info = _Info()
    if not _info(handle, ctypes.byref(info)):
        _fail()
    if info.attributes & 0x400 or bool(info.attributes & 0x10) != directory:
        raise Error("permission", "private storage reparse point or file type rejected")
    if not directory and info.links != 1:
        raise Error("permission", "private storage hard link rejected")
    if private:
        _private_acl(handle)
    return info


class Backend:
    def __init__(self, path, create):
        self.path = path
        self._handles = []
        self._pid = os.getpid()
        if create:
            parents = self._ancestors(os.path.dirname(path), False)
            descriptor = None
            try:
                descriptor = _descriptor(True)
                sa = _Security(ctypes.sizeof(_Security), descriptor, False)
                if not _mkdir(path, ctypes.byref(sa)) and ctypes.get_last_error() != 183:
                    _fail()
            finally:
                if descriptor:
                    _local_free(descriptor)
                for handle in reversed(parents):
                    _close(handle)
        self._handles = self._ancestors(path, True)
        try:
            self._identity = self.identity(self._handles[-1])
        except BaseException:
            self.close()
            raise

    @staticmethod
    def _ancestors(path, private):
        drive, tail = os.path.splitdrive(path)
        current = drive + "\\"
        held = []
        try:
            for part in tail.strip("\\/").split("\\"):
                if not part:
                    continue
                current = os.path.join(current, part)
                handle = _create(current, 0x80 | 0x20000, 3, None, 3, 0x02000000 | 0x00200000, None)
                if handle == _INVALID:
                    _fail()
                held.append(handle)
                _check(handle, True, private and os.path.normcase(current) == os.path.normcase(path))
            if not held:
                raise Error("permission", "private storage root directory rejected")
            return held
        except BaseException:
            for handle in reversed(held):
                _close(handle)
            raise

    def verify(self):
        if os.getpid() != self._pid:
            raise Error("permission", "inherited storage must be reopened")
        for handle in self._handles[:-1]:
            _check(handle, True, False)
        _check(self._handles[-1], True)
        now = self._ancestors(self.path, True)
        try:
            if self.identity(now[-1]) != self._identity:
                raise Error("permission", "private storage directory was replaced")
        finally:
            for handle in reversed(now):
                _close(handle)

    @staticmethod
    def identity(handle):
        info = _check(handle, bool(_raw_info(handle).attributes & 0x10), False)
        return info.volume, (info.index_high << 32) | info.index_low

    def open(self, name, create=False, exclusive=False, writable=False):
        descriptor = _descriptor(False) if create else None
        try:
            sa = _Security(ctypes.sizeof(_Security), descriptor, False)
            disposition = 1 if exclusive else (4 if create else 3)
            access = 0x80000000 | 0x20000 | (0x40000000 if writable else 0)
            handle = _create(os.path.join(self.path, name), access, 3,
                             ctypes.byref(sa) if create else None,
                             disposition, 0x80 | 0x00200000, None)
            if handle == _INVALID:
                _fail()
            try:
                _check(handle)
                return handle
            except BaseException:
                _close(handle)
                raise
        finally:
            if descriptor:
                _local_free(descriptor)

    @staticmethod
    def check(handle):
        return _check(handle)

    @staticmethod
    def close_file(handle):
        if not _close(handle):
            _fail()

    @staticmethod
    def size(handle):
        info = _raw_info(handle)
        return (info.size_high << 32) | info.size_low

    @staticmethod
    def read(handle, length):
        buffer, count = ctypes.create_string_buffer(length), w.DWORD()
        if not _read(handle, buffer, length, ctypes.byref(count), None):
            _fail()
        return buffer.raw[:count.value]

    @staticmethod
    def write(handle, data):
        count = w.DWORD()
        buffer = ctypes.create_string_buffer(data)
        if not _write(handle, buffer, len(data), ctypes.byref(count), None):
            _fail()
        return count.value

    @staticmethod
    def sync_file(handle):
        if not _flush(handle):
            _fail()

    def sync_directory(self):
        # Windows 的提交屏障是 FlushFileBuffers + MOVEFILE_WRITE_THROUGH。
        # 普通目录句柄不支持 FlushFileBuffers，不能把其失败当作 fsync 成功。
        self.verify()

    def names(self):
        with os.scandir(self.path) as entries:
            for entry in entries:
                yield entry.name

    def replace(self, source, target, replace):
        if not _move(os.path.join(self.path, source), os.path.join(self.path, target),
                     0x8 | (0x1 if replace else 0)):
            _fail()

    def remove(self, name):
        if not _delete(os.path.join(self.path, name)):
            _fail()

    def remove_durable(self, name):
        # 先 WRITE_THROUGH 挪离公开名字，再清理本次私有墓碑。
        # 崩溃最多遗留受容量计入的墓碑，不把原名字删除误称为未提交。
        tombstone = ".tansr-tomb-" + os.urandom(16).hex()
        self.replace(name, tombstone, False)
        self.remove(tombstone)

    @staticmethod
    def lock(handle):
        overlap = _Overlapped()
        if not _lock(handle, 0x1 | 0x2, 0, 0xffffffff, 0xffffffff, ctypes.byref(overlap)):
            code = ctypes.get_last_error()
            if code in (32, 33, 158):
                raise Error("conflict", "private storage already locked")
            _fail(code)

    @staticmethod
    def unlock(handle):
        overlap = _Overlapped()
        if not _unlock(handle, 0, 0xffffffff, 0xffffffff, ctypes.byref(overlap)):
            _fail()

    def close(self):
        handles, self._handles = self._handles, []
        for handle in reversed(handles):
            _close(handle)


def _raw_info(handle):
    info = _Info()
    if not _info(handle, ctypes.byref(info)):
        _fail()
    return info
