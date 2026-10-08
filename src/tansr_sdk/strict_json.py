"""有界、自有 JSON；数字保留原词法，不改变承诺正文的原始字节。"""
import json
import math
import re
from typing import Any, Callable, Dict, List, NoReturn, Tuple, Union, cast

from .errors import Error

DEFAULT_MAX_BYTES = 8 * 1024 * 1024
DEFAULT_MAX_DEPTH = 32
DEFAULT_MAX_NODES = 100000
DEFAULT_MAX_NUMBER_CHARS = 4096
_NUMBER = re.compile(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?\Z", re.ASCII)
_INTEGER = re.compile(r"-?(?:0|[1-9][0-9]*)\Z", re.ASCII)
_SCANSTRING = cast(Callable[[str, int, bool], Tuple[str, int]], getattr(json.decoder, "scanstring"))


def _fail(reason) -> NoReturn:
    raise Error("contract", "invalid JSON: " + reason)


def _number_text(value, integer):
    if type(value) is not str or len(value) > DEFAULT_MAX_NUMBER_CHARS:
        _fail("number length/type")
    if not (_INTEGER if integer else _NUMBER).fullmatch(value):
        _fail("number grammar")
    return value


class JsonInt(int):
    """整数的 Python 值与不可变原词法，例如 -0。"""
    _lexeme: str

    def __new__(cls, lexeme):
        token = _number_text(lexeme, True)
        try:
            obj = int.__new__(cls, token)
        except ValueError:
            _fail("integer conversion limit")
        object.__setattr__(obj, "_lexeme", token)
        return obj

    @property
    def lexeme(self):
        return self._lexeme

    def __setattr__(self, name, value):
        raise AttributeError("JSON number is immutable")

    def __reduce__(self):
        return (JsonInt, (self.lexeme,))


class JsonFloat(float):
    """普通 JSON 数词；越出 double 范围仍保原词法，由所属域决定合法性。"""
    _lexeme: str

    def __new__(cls, lexeme):
        token = _number_text(lexeme, False)
        obj = float.__new__(cls, token)
        object.__setattr__(obj, "_lexeme", token)
        return obj

    @property
    def lexeme(self):
        return self._lexeme

    def __setattr__(self, name, value):
        raise AttributeError("JSON number is immutable")

    def __reduce__(self):
        return (JsonFloat, (self.lexeme,))


def number_lexeme(value):
    """只接收 JSON 数字；bool 不因继承 int 而获准。"""
    if type(value) is JsonInt:
        token = _number_text(value.lexeme, True)
        if int(token) != value:
            _fail("number value differs from original token")
        return token
    if type(value) is JsonFloat:
        token = _number_text(value.lexeme, False)
        if float(token) != value:
            _fail("number value differs from original token")
        return token
    if type(value) is int:
        # 在转换成十进制之前限制工作量，不修改解释器全局 int 限额。
        if value.bit_length() > 13607:
            _fail("number too large")
        try:
            return _number_text(str(value), True)
        except ValueError:
            _fail("integer conversion limit")
    if type(value) is float and math.isfinite(value):
        return _number_text(repr(value), False)
    _fail("not a finite native JSON number")


def _limits(max_bytes, max_depth, max_nodes, max_number_chars):
    for value, lower, upper in ((max_bytes, 1, 256 * 1024 * 1024),
                                 (max_depth, 0, 128), (max_nodes, 1, 2000000),
                                 (max_number_chars, 1, DEFAULT_MAX_NUMBER_CHARS)):
        if type(value) is not int or not lower <= value <= upper:
            raise Error("invalid_input", "invalid JSON resource limit")


def _text(value):
    if type(value) is not str:
        _fail("string required")
    try:
        value.encode("utf-8", "strict")
    except UnicodeError:
        _fail("invalid Unicode string")
    return value


def utf16_length(value):
    _text(value)
    return sum(2 if ord(char) > 0xFFFF else 1 for char in value)


class _Parser:
    def __init__(self, text, max_depth, max_nodes, max_number_chars):
        self.text = text
        self.at = 0
        self.max_depth = max_depth
        self.max_nodes = max_nodes
        self.max_number_chars = max_number_chars
        self.nodes = 0

    def whitespace(self):
        while self.at < len(self.text) and self.text[self.at] in " \t\r\n":
            self.at += 1

    def string(self):
        try:
            value, self.at = _SCANSTRING(self.text, self.at + 1, True)
        except (ValueError, OverflowError):
            _fail("invalid string/escape")
        return _text(value)

    def value(self, depth):
        if depth > self.max_depth:
            _fail("depth exceeded")
        self.nodes += 1
        if self.nodes > self.max_nodes:
            _fail("nodes exceeded")
        self.whitespace()
        if self.at >= len(self.text):
            _fail("value missing")
        char = self.text[self.at]
        if char == '"':
            return self.string()
        if char in "[{":
            self.at += 1
            end = "]" if char == "[" else "}"
            result: Union[List[Any], Dict[str, Any]] = [] if char == "[" else {}
            self.whitespace()
            if self.at < len(self.text) and self.text[self.at] == end:
                self.at += 1
                return result
            while True:
                if char == "{":
                    self.whitespace()
                    if self.at >= len(self.text) or self.text[self.at] != '"':
                        _fail("object key required")
                    key = self.string()
                    if key in result:
                        _fail("duplicate decoded key")
                    self.whitespace()
                    if self.at >= len(self.text) or self.text[self.at] != ":":
                        _fail("colon required")
                    self.at += 1
                    cast(Dict[str, Any], result)[key] = self.value(depth + 1)
                else:
                    cast(List[Any], result).append(self.value(depth + 1))
                self.whitespace()
                if self.at >= len(self.text):
                    _fail("unclosed container")
                sep = self.text[self.at]
                self.at += 1
                if sep == end:
                    return result
                if sep != ",":
                    _fail("container separator")
        for literal, value in (("null", None), ("true", True), ("false", False)):
            if self.text.startswith(literal, self.at):
                self.at += len(literal)
                return value
        start = self.at
        while self.at < len(self.text) and self.text[self.at] in "0123456789eE+.-":
            self.at += 1
            if self.at - start > self.max_number_chars:
                _fail("number token too long")
        token = self.text[start:self.at]
        if not token or not _NUMBER.fullmatch(token):
            _fail("number grammar or unknown literal")
        return JsonInt(token) if _INTEGER.fullmatch(token) else JsonFloat(token)


def loads(raw, *, max_bytes=DEFAULT_MAX_BYTES, max_depth=DEFAULT_MAX_DEPTH,
          max_nodes=DEFAULT_MAX_NODES, max_number_chars=DEFAULT_MAX_NUMBER_CHARS):
    """严格 UTF-8 JSON；根深度为零，键不计值节点；解析前逐层限制。"""
    _limits(max_bytes, max_depth, max_nodes, max_number_chars)
    if type(raw) is bytes:
        if len(raw) > max_bytes:
            _fail("bytes exceeded")
        try:
            text = raw.decode("utf-8", "strict")
        except UnicodeError:
            _fail("invalid UTF-8")
    elif type(raw) is str:
        if len(raw) > max_bytes:
            _fail("bytes exceeded")
        text = _text(raw)
        if len(text.encode("utf-8")) > max_bytes:
            _fail("bytes exceeded")
    else:
        raise Error("invalid_input", "JSON input must be bytes or str")
    parser = _Parser(text, max_depth, max_nodes, max_number_chars)
    value = parser.value(0)
    parser.whitespace()
    if parser.at != len(text):
        _fail("trailing content")
    return value


def dumps(value, *, max_bytes=DEFAULT_MAX_BYTES, max_depth=DEFAULT_MAX_DEPTH,
          max_nodes=DEFAULT_MAX_NODES, max_number_chars=DEFAULT_MAX_NUMBER_CHARS):
    """普通 JSON 编码；原数词保留。不是承诺字节的重编码入口。"""
    _limits(max_bytes, max_depth, max_nodes, max_number_chars)
    output = bytearray()
    active = set()
    nodes = [0]

    def emit(data):
        if len(data) > max_bytes - len(output):
            _fail("bytes exceeded")
        output.extend(data)

    def string(text):
        _text(text)
        if len(text) > max_bytes:
            _fail("bytes exceeded")
        emit(json.dumps(text, ensure_ascii=False).encode("utf-8"))

    def write(item, depth):
        if depth > max_depth:
            _fail("depth exceeded")
        nodes[0] += 1
        if nodes[0] > max_nodes:
            _fail("nodes exceeded")
        kind = type(item)
        if item is None:
            emit(b"null")
        elif kind is bool:
            emit(b"true" if item else b"false")
        elif kind in (int, float, JsonInt, JsonFloat):
            token = number_lexeme(item)
            if len(token) > max_number_chars:
                _fail("number token too long")
            emit(token.encode("ascii"))
        elif kind is str:
            string(item)
        elif kind in (list, dict):
            identity = id(item)
            if identity in active:
                _fail("circular container")
            if len(item) > max_nodes - nodes[0]:
                _fail("nodes exceeded")
            active.add(identity)
            try:
                emit(b"[" if kind is list else b"{")
                first = True
                for key in item:
                    if not first:
                        emit(b",")
                    first = False
                    if kind is dict:
                        string(key)
                        emit(b":")
                        write(item[key], depth + 1)
                    else:
                        write(key, depth + 1)
                emit(b"]" if kind is list else b"}")
            finally:
                active.remove(identity)
        else:
            _fail("unsupported JSON value")

    write(value, 0)
    return bytes(output)


def snapshot(value, **limits):
    """深复制并验证，无用户自定义序列化回调；数词仍保留。"""
    return loads(dumps(value, **limits), **limits)
