"""冻结的旧工具声明摘要，不借用 SDK2 控制 canonical。"""
import hashlib
import json
import math
import re
from typing import Any, Callable, Dict

from .. import strict_json
from ..errors import Error


def utf16_length(text: str) -> int:
    try:
        return len(text.encode("utf-16-le")) // 2
    except UnicodeError:
        raise Error("invalid_request", "invalid Unicode")


def _text(value: Any, limit: int, nonempty: bool = False) -> bool:
    return isinstance(value, str) and (not nonempty or bool(value)) and utf16_length(value) <= limit


def _parameter(value: Any, depth: int, counted: bool = True) -> None:
    if (counted and depth > 8 or not isinstance(value, dict) or
            set(value) - {"type", "description", "optional", "items", "properties"} or
            value.get("type") not in ("string", "number", "boolean", "array", "object")):
        raise Error("invalid_request", "tool parameter")
    if "description" in value and not _text(value["description"], 2048):
        raise Error("invalid_request", "parameter description")
    if "optional" in value and type(value["optional"]) is not bool:
        raise Error("invalid_request", "parameter optional")
    if "items" in value:
        _parameter(value["items"], depth + 1, counted)
    if "properties" in value:
        if not isinstance(value["properties"], dict):
            raise Error("invalid_request", "parameter properties")
        for key, child in value["properties"].items():
            _parameter(child, depth + 1, counted and key != "__proto__")


def _key_order(key: str) -> Any:
    # JS Object.keys 在排序后仍先枚举 uint32 索引，再按 UTF-16 排序其余键。
    if len(key) <= 10 and re.fullmatch(r"0|[1-9][0-9]*", key) and int(key) < 4294967295:
        return (0, int(key))
    return (1, key.encode("utf-16-be"))


def _legacy(value: Any) -> str:
    if isinstance(value, dict):
        # 冻结 Zod record / sortKeysDeep 的普通对象赋值不保留 __proto__ 自有键。
        return "{" + ",".join(_legacy(key) + ":" + _legacy(value[key])
                              for key in sorted(value, key=_key_order) if key != "__proto__") + "}"
    if isinstance(value, list):
        return "[" + ",".join(_legacy(item) for item in value) + "]"
    if isinstance(value, bool) or value is None or isinstance(value, str):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    # 声明的唯一数值是已验证的 1000..600000 timeoutMs，JS 将 1e3/1000.0 归一为 1000。
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(int(value))
    raise Error("invalid_request", "tool declaration JSON")


def definition_bytes(declaration: Dict[str, Any]) -> bytes:
    # __proto__ 的 schema 仍须验证，但其被 JS 丢弃的分支不参加最终 depth 8 计算。
    value = strict_json.snapshot(declaration, max_depth=128, max_bytes=262144)
    if (not isinstance(value, dict) or
            set(value) - {"name", "description", "parameters", "readOnly", "effects", "timeoutMs"} or
            not isinstance(value.get("name"), str) or
            not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,63}", value["name"]) or
            not _text(value.get("description"), 2048, True)):
        raise Error("invalid_request", "tool declaration")
    if "readOnly" in value and type(value["readOnly"]) is not bool:
        raise Error("invalid_request", "tool readOnly")
    if "timeoutMs" in value:
        timeout = value["timeoutMs"]
        if (isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or
                not 1000 <= timeout <= 600000 or int(timeout) != timeout):
            raise Error("invalid_request", "tool timeout")
        value["timeoutMs"] = int(timeout)
    if "effects" in value:
        effects = value["effects"]
        if (not isinstance(effects, list) or len(effects) > 4 or
                any(effect not in ("irreversible", "financial", "external", "affects-others")
                    for effect in effects) or len(set(effects)) != len(effects)):
            raise Error("invalid_request", "tool effects")
    if "parameters" in value:
        if not isinstance(value["parameters"], dict):
            raise Error("invalid_request", "tool parameters")
        for key, spec in value["parameters"].items():
            _parameter(spec, 1, key != "__proto__")
        if len(_legacy(value["parameters"]).encode("utf-8")) > 32768:
            raise Error("payload_too_large", "tool parameters")
    encoded = _legacy(value).encode("utf-8")
    if len(encoded) > 262144:
        raise Error("payload_too_large")
    return encoded


def definition_digest(declaration: Dict[str, Any]) -> str:
    return hashlib.sha256(b"tansr.sdk2.client-tool.v1\0" + definition_bytes(declaration)).hexdigest()


def parse_tool_arguments(raw: Any) -> Dict[str, Any]:
    if not isinstance(raw, (str, bytes)):
        raise Error("invalid_request", "business JSON")
    data = raw.encode("utf-8") if isinstance(raw, str) else raw
    if len(data) > 32768:
        raise Error("payload_too_large")
    value = strict_json.loads(data)
    nodes = [0]

    def walk(item: Any, depth: int) -> None:
        nodes[0] += 1
        if depth > 32 or nodes[0] > 32768:
            raise Error("payload_too_large")
        if isinstance(item, (int, float)) and not isinstance(item, bool):
            try:
                finite = math.isfinite(float(item))
            except OverflowError:
                finite = False
            if not finite:
                raise Error("invalid_request", "non-finite business number")
        if isinstance(item, dict):
            for child in item.values():
                walk(child, depth + 1)
        elif isinstance(item, list):
            for child in item:
                walk(child, depth + 1)
    walk(value, 0)
    if not isinstance(value, dict):
        raise Error("invalid_request", "business object required")
    return value


def verify_tool_result(value: Any) -> Dict[str, Any]:
    value = parse_tool_arguments(strict_json.dumps(value))
    if value.get("status") == "error":
        if not _text(value.get("message"), 4096, True):
            raise Error("invalid_request", "business error message")
        return value
    content = value.get("content")
    if (value.get("status") != "ok" or ("isError" in value and type(value["isError"]) is not bool)
            or not isinstance(content, list) or not 1 <= len(content) <= 64):
        raise Error("invalid_request", "tool result")
    for part in content:
        if not isinstance(part, dict):
            raise Error("invalid_request", "tool content")
        if part.get("t") == "text" and isinstance(part.get("text"), str):
            continue
        if (part.get("t") == "image" and part.get("mime") in
                ("image/png", "image/jpeg", "image/webp", "image/gif") and
                isinstance(part.get("data"), str)):
            continue
        raise Error("invalid_request", "tool content")
    return value


class Rejected(Error):
    """仅供宿主确认没有副作用的拒绝；普通异常必须保留 unknown。"""
    def __init__(self, code: str) -> None:
        if not isinstance(code, str) or not 1 <= len(code) <= 128:
            raise Error("invalid_request", "rejection code")
        super().__init__(code)


class Tool:
    def __init__(self, declaration: Dict[str, Any], handler: Callable[..., Any]) -> None:
        if not callable(handler):
            raise Error("invalid_request", "tool handler")
        encoded = definition_bytes(declaration)
        self.declaration = strict_json.loads(encoded)
        self.definition_digest = hashlib.sha256(b"tansr.sdk2.client-tool.v1\0" + encoded).hexdigest()
        self.name = self.declaration["name"]
        self.handler = handler

    def registration(self) -> Dict[str, Any]:
        return {"name": self.name, "definitionDigest": self.definition_digest}
