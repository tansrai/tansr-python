"""会话领域验证，不另造HTTP、认证或重试实现。"""

import math
import re
from contextlib import contextmanager
from typing import Any, Dict, Optional

from ..errors import Error
from ..lifecycle import CancellationToken
from ..strict_json import snapshot
from .types import ImageBlock, Meta, TextBlock

MAX_SAFE_INTEGER = 9007199254740991
MEDIA_BYTES = 32 * 1024 * 1024


def invalid(message: str) -> Error:
    return Error("invalid_input", "session: " + message)


def contract(message: str) -> Error:
    return Error("contract", "session: " + message)


def text(value: Any, *, nonempty: bool = False, limit: Optional[int] = None) -> str:
    if not isinstance(value, str):
        raise invalid("string required")
    try:
        units = len(value.encode("utf-16-le")) // 2
    except UnicodeError:
        raise invalid("valid Unicode required") from None
    if (nonempty and not value) or (limit is not None and units > limit):
        raise invalid("string length is outside its contract")
    return value


def safe_integer(value: Any, *, response: bool = False) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= MAX_SAFE_INTEGER:
        raise (contract if response else invalid)("nonnegative safe integer required")
    return int(value)


def finite_number(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise invalid("finite number required")
    try:
        result = float(value)
    except (ValueError, OverflowError):
        raise invalid("finite number required") from None
    if not math.isfinite(result):
        raise invalid("finite number required")
    return result


def required_string(value: Any, key: str) -> str:
    if not isinstance(value, dict) or not isinstance(value.get(key), str) or not value[key]:
        raise contract("missing or empty string field")
    try:
        return text(value[key], nonempty=True)
    except Error:
        raise contract("response contains invalid Unicode") from None


def object_response(response: Any, status: int = 200) -> Dict[str, Any]:
    media_type = response.headers.get("content-type", "").split(";", 1)[0].strip().lower()
    if response.status != status or media_type != "application/json" or not isinstance(response.body, dict):
        raise contract("unexpected response status, content type or object")
    return snapshot(response.body)


def check_family(value: Dict[str, Any], family: str) -> None:
    if family == "sdk2-offload-v1" and (
        value.get("contract") != family or value.get("availability") != "source-required"
    ):
        raise contract("offload family/source lifecycle is missing")
    declared = value.get("contract")
    if declared is not None and declared != family:
        raise contract("response changed the selected family")


def read_meta(raw: Dict[str, Any], family: str) -> Meta:
    identity = required_string(raw, "sessionId")
    status = required_string(raw, "status")
    if status not in ("idle", "running", "ended") or not isinstance(raw.get("live"), bool):
        raise contract("invalid session metadata")
    seq = safe_integer(raw.get("lastSeq"), response=True)
    check_family(raw, family)
    return Meta(identity, status, raw["live"], seq, raw)


def blocks_json(blocks: Any, *, text_only: bool = False) -> list:
    # 先取定一份，后续发现/认证回调不能改变待发请求。
    if not isinstance(blocks, (list, tuple)) or not 1 <= len(blocks) <= 64:
        raise invalid("message requires 1 to 64 blocks")
    result = []
    for block in blocks:
        if isinstance(block, TextBlock):
            result.append({"t": "text", "text": text(block.text, nonempty=True, limit=262144)})
        elif isinstance(block, ImageBlock) and not text_only:
            if block.mime not in ("image/png", "image/jpeg", "image/gif", "image/webp"):
                raise invalid("unsupported image MIME type")
            result.append({"t": "image", "mime": block.mime, "data": text(block.data, nonempty=True)})
        else:
            raise invalid("unsupported message block")
    return result


def parse_sequence(value: Any) -> int:
    if not isinstance(value, str) or not re.fullmatch(r"(?:0|[1-9][0-9]{0,15})", value):
        raise invalid("cursor must be canonical nonnegative decimal")
    return safe_integer(int(value))


@contextmanager
def linked_cancel(*tokens: Optional[CancellationToken]):
    child = CancellationToken()
    unregister = []
    try:
        for token in tokens:
            if token is not None:
                unregister.append(token.register(child.cancel))
        child.check()
        yield child
    finally:
        for remove in unregister:
            remove()
