"""单快照 AES-256-GCM 容器；业务档案格式由上层管理。

密钥必须来自宿主，建议每个存储使用独立密钥。96-bit 随机 nonce；AAD
绑定容器版本、文件名、key_id 与用量。用量为本存储的成功提交及本实例尝试，
不声称跨文件密钥总量控制或介质回滚防护；宿主负责全局密钥轮换策略。
Python 对象释放不构成可靠的秘密零化。
"""
import base64
import binascii
import contextlib
import os
import struct
import threading
from typing import Optional

from .. import strict_json
from ..errors import Error
from ..lifecycle import CancellationToken
from .private_directory import PrivateDirectory, _bound, _cancel, safe_name


_MAGIC = b"Tansr-Python-EncryptedStore/1\n"
_HEADER_LIMIT = 4096
_AAD_DOMAIN = b"tansr.python.encrypted-store.v1\x00"


def encode_bytes(value: bytes, *, max_bytes: int = 64 << 20) -> str:
    """对原 bytes 做 base64，不经文本解码或换行转换。"""
    if type(value) is not bytes:
        raise Error("invalid_argument", "base64 input must be owned immutable bytes")
    if len(value) > _bound(max_bytes, 256 << 20, "base64 byte bound"):
        raise Error("capacity", "base64 input exceeded byte bound")
    return base64.b64encode(value).decode("ascii")


def decode_bytes(value: str, *, max_bytes: int = 64 << 20) -> bytes:
    """只接受标准、有 padding、无空白且 pad bits 正确的 canonical base64。"""
    limit = _bound(max_bytes, 256 << 20, "base64 byte bound")
    if not isinstance(value, str) or len(value) > 4 * ((limit + 2) // 3):
        raise Error("capacity", "base64 encoded byte bound exceeded")
    try:
        encoded = value.encode("ascii", "strict")
        raw = base64.b64decode(encoded, validate=True)
    except (ValueError, UnicodeError, binascii.Error) as exc:
        raise Error("integrity", "invalid stored base64") from exc
    if len(raw) > limit:
        raise Error("capacity", "base64 decoded byte bound exceeded")
    if base64.b64encode(raw) != encoded:
        raise Error("integrity", "noncanonical stored base64")
    return raw


def _key(value, key_id):
    if type(value) is not bytes or len(value) != 32:
        raise Error("invalid_argument", "AES-256-GCM requires an external 32-byte bytes key")
    if not isinstance(key_id, str) or not key_id or any(ord(c) < 32 for c in key_id):
        raise Error("invalid_argument", "invalid encrypted storage key identifier")
    try:
        if len(key_id.encode("utf-8", "strict")) > 128:
            raise Error("capacity", "key identifier exceeded byte bound")
    except UnicodeError as exc:
        raise Error("invalid_argument", "invalid key identifier UTF-8") from exc


class EncryptedStore:
    """持有 filename+'.lock'，close 只释放自身资源，保留借用目录。"""
    def __init__(self, directory: PrivateDirectory, filename: str, key: bytes,
                 key_id: str, *, max_bytes: int = 64 << 20,
                 max_encryptions: int = 1 << 20,
                 max_encrypted_bytes: int = 1 << 36) -> None:
        if not isinstance(directory, PrivateDirectory):
            raise Error("invalid_argument", "EncryptedStore requires a PrivateDirectory")
        _key(key, key_id)
        self.directory = directory
        self.filename = safe_name(filename)
        safe_name(filename + ".lock")
        self.max_bytes = _bound(max_bytes, (256 << 20) - _HEADER_LIMIT - 64,
                                "plaintext byte bound")
        self.max_encryptions = _bound(max_encryptions, 1 << 20, "encryption count")
        self.max_encrypted_bytes = _bound(max_encrypted_bytes, 1 << 36, "encryption bytes")
        self._key = key  # type: Optional[bytes]
        self.key_id = key_id
        self._mutex = threading.RLock()
        self._entered = False
        self._closed = False
        self._uses = 0
        self._encrypted_bytes = 0
        self._loaded = False
        # 密码库只在实际创建加密存储时导入；不会给普通文件操作添加依赖。
        try:
            from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        except ImportError as exc:
            raise Error("dependency", "cryptography AESGCM unavailable") from exc
        self._aes_type = AESGCM
        self._cipher = AESGCM(key)  # type: Optional[AESGCM]
        self._lock = directory.lock(filename + ".lock")
        self._lock.__enter__()

    @contextlib.contextmanager
    def _operation(self, cancel=None, deadline_ms=None):
        while not self._mutex.acquire(timeout=0.05):
            _cancel(cancel, deadline_ms)
        entered = False
        try:
            if self._entered:
                raise Error("reentrant", "encrypted storage callback reentry rejected")
            if self._closed:
                raise Error("closed", "encrypted storage closed")
            self._entered = entered = True
            _cancel(cancel, deadline_ms)
            self.directory.check_access()
            yield
        finally:
            if entered:
                self._entered = False
            self._mutex.release()

    def _aad(self, header):
        return _AAD_DOMAIN + self.filename.encode("utf-8") + b"\x00" + header

    def _load(self, cancel=None, deadline_ms=None):
        assert self._cipher is not None
        try:
            blob = self.directory.read(self.filename, self.max_bytes + _HEADER_LIMIT + 64,
                                       cancel=cancel, deadline_ms=deadline_ms)
        except FileNotFoundError:
            self._loaded = True
            return None
        if len(blob) < len(_MAGIC) + 4 + 16 or not blob.startswith(_MAGIC):
            raise Error("integrity", "unsupported or truncated encrypted storage format")
        length = struct.unpack(">I", blob[len(_MAGIC):len(_MAGIC) + 4])[0]
        start = len(_MAGIC) + 4
        if not 1 <= length <= _HEADER_LIMIT or start + length + 16 > len(blob):
            raise Error("integrity", "invalid encrypted storage header")
        raw_header = blob[start:start + length]
        header = strict_json.loads(raw_header, max_bytes=_HEADER_LIMIT)
        if (not isinstance(header, dict) or set(header) !=
                {"version", "keyId", "nonce", "uses", "encryptedBytes"} or
                not isinstance(header["version"], int) or
                header["version"] != 1 or isinstance(header["version"], bool) or
                header["keyId"] != self.key_id):
            raise Error("integrity", "encrypted storage version or key identity rejected")
        for field, maximum in (("uses", self.max_encryptions),
                               ("encryptedBytes", self.max_encrypted_bytes)):
            value = header[field]
            if not isinstance(value, int) or isinstance(value, bool) or not 1 <= value <= maximum:
                raise Error("capacity", "encrypted storage usage limit rejected")
        nonce = decode_bytes(header["nonce"], max_bytes=12)
        if len(nonce) != 12:
            raise Error("integrity", "invalid encrypted storage nonce")
        from cryptography.exceptions import InvalidTag
        try:
            # AESGCM 两个入口均只接收连续、自有、不可变 bytes。
            plain = self._cipher.decrypt(nonce, blob[start + length:], self._aad(raw_header))
        except InvalidTag as exc:
            raise Error("integrity", "encrypted storage authentication failed") from exc
        if len(plain) > self.max_bytes:
            raise Error("capacity", "encrypted storage plaintext limit exceeded")
        state = strict_json.loads(plain, max_bytes=self.max_bytes)
        if not isinstance(state, dict):
            raise Error("integrity", "encrypted storage state must be an object")
        self.directory.check_access()
        _cancel(cancel, deadline_ms)
        self._uses = max(self._uses, header["uses"])
        self._encrypted_bytes = max(self._encrypted_bytes, header["encryptedBytes"])
        self._loaded = True
        return state

    def load(self, *, cancel: Optional[CancellationToken] = None,
             deadline_ms: Optional[int] = None) -> Optional[dict]:
        with self._operation(cancel, deadline_ms):
            return self._load(cancel, deadline_ms)

    def _save(self, state, replace, cancel, deadline_ms, hook):
        assert self._cipher is not None
        if not isinstance(state, dict):
            raise Error("invalid_argument", "encrypted storage state must be an object")
        if not self._loaded:
            self._load(cancel, deadline_ms)  # 坏旧介质不能被 save 静默覆盖。
        plain = strict_json.dumps(state, max_bytes=self.max_bytes)
        if self._uses >= self.max_encryptions or len(plain) > self.max_encrypted_bytes - self._encrypted_bytes:
            raise Error("capacity", "encrypted storage key rotation required")
        self.directory.check_access()
        _cancel(cancel, deadline_ms)
        self._uses += 1
        self._encrypted_bytes += len(plain)
        nonce = os.urandom(12)
        header = strict_json.dumps({"version": 1, "keyId": self.key_id,
                                    "nonce": encode_bytes(nonce, max_bytes=12),
                                    "uses": self._uses,
                                    "encryptedBytes": self._encrypted_bytes}, max_bytes=_HEADER_LIMIT)
        sealed = self._cipher.encrypt(nonce, plain, self._aad(header))
        blob = _MAGIC + struct.pack(">I", len(header)) + header + sealed
        self.directory.write(self.filename, blob, replace=replace, cancel=cancel,
                             deadline_ms=deadline_ms, hook=hook)

    def save(self, state: dict, *, replace: bool = True,
             cancel: Optional[CancellationToken] = None,
             deadline_ms: Optional[int] = None, hook=None) -> None:
        with self._operation(cancel, deadline_ms):
            self._save(state, replace, cancel, deadline_ms, hook)

    def rotate_key(self, key: bytes, key_id: str, *, cancel=None,
                   deadline_ms=None, hook=None) -> None:
        """用当前 key 验证原快照，再以新 key 原子提交；不自动清档或造新 key。"""
        _key(key, key_id)
        with self._operation(cancel, deadline_ms):
            if key == self._key or key_id == self.key_id:
                raise Error("invalid_argument", "key rotation requires a distinct key and identifier")
            state = self._load(cancel, deadline_ms)
            previous = self._key, self.key_id, self._cipher, self._uses, self._encrypted_bytes
            self._key, self.key_id = key, key_id
            self._cipher, self._uses, self._encrypted_bytes = self._aes_type(key), 0, 0
            try:
                if state is not None:
                    self._save(state, True, cancel, deadline_ms, hook)
            except BaseException:
                self._key, self.key_id, self._cipher, self._uses, self._encrypted_bytes = previous
                raise

    def close(self) -> None:
        with self._mutex:
            if self._entered:
                raise Error("reentrant", "encrypted storage close during callback rejected")
            if self._closed:
                return
            self._lock.close()
            self._closed = True
            self._cipher = None
            self._key = None

    def __enter__(self):
        with self._operation():
            return self

    def __exit__(self, *unused):
        self.close()
