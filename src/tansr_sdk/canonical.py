"""冻结 UAPI r7 控制字节；不用于业务工具参数或原档案正文重编码。"""
import hashlib
import re
from urllib.parse import quote

from . import strict_json
from .errors import Error

MAX_DEPTH = 32
MAX_NODES = 100000
MAX_SAFE_INTEGER = 9007199254740991
DEFAULT_MAX_BYTES = 8 * 1024 * 1024
DOMAIN_CLOSURE = "tansr.unified.closure.v1"
_UNSIGNED = re.compile(r"(?:0|[1-9][0-9]*)\Z", re.ASCII)


def encode(value, max_bytes=DEFAULT_MAX_BYTES):
    """验证自有树后按 ASCII 键排序；控制数字保持严格整数字面量。"""
    owned = strict_json.snapshot(value, max_bytes=max_bytes,
                                  max_depth=MAX_DEPTH, max_nodes=MAX_NODES)

    def control(item):
        if isinstance(item, (strict_json.JsonInt, strict_json.JsonFloat)):
            token = strict_json.number_lexeme(item)
            if not _UNSIGNED.fullmatch(token) or len(token) > 16 or int(token) > MAX_SAFE_INTEGER:
                raise Error("contract", "invalid control integer")
            return item
        if type(item) is dict:
            for key in item:
                if not key or any(ord(char) < 0x21 or ord(char) > 0x7E for char in key):
                    raise Error("contract", "invalid control key")
            return {key: control(item[key]) for key in sorted(item)}
        if type(item) is list:
            return [control(child) for child in item]
        return item

    return strict_json.dumps(control(owned), max_bytes=max_bytes,
                              max_depth=MAX_DEPTH, max_nodes=MAX_NODES)


def decode(raw, max_bytes=DEFAULT_MAX_BYTES):
    value = strict_json.loads(raw, max_bytes=max_bytes,
                              max_depth=MAX_DEPTH, max_nodes=MAX_NODES)
    encode(value, max_bytes)
    return value


def parse_strict(raw, max_bytes=DEFAULT_MAX_BYTES):
    value = decode(raw, max_bytes)
    original = raw.encode("utf-8") if type(raw) is str else raw
    if encode(value, max_bytes) != original:
        raise Error("contract", "JSON not_canonical")
    return value


def digest_bytes(domain, raw):
    if type(domain) is not str or not domain or "\x00" in domain or type(raw) is not bytes:
        raise Error("invalid_input", "invalid digest domain/bytes")
    try:
        prefix = domain.encode("utf-8", "strict")
    except UnicodeError:
        raise Error("invalid_input", "invalid digest domain") from None
    state = hashlib.sha256()
    state.update(prefix)
    state.update(b"\x00")
    state.update(raw)
    return state.hexdigest()


def digest(domain, value):
    return digest_bytes(domain, encode(value))


def encode_path_segment(value):
    if type(value) is not str:
        raise Error("invalid_input", "path segment must be str")
    try:
        return quote(value, safe="-_.!~*'()", encoding="utf-8", errors="strict")
    except UnicodeError:
        raise Error("invalid_input", "path segment invalid Unicode") from None
