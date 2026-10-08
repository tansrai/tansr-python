"""仅解释冻结合同使用的 JSON Schema 子集；未知规则明确拒绝。"""
import re
import threading
from typing import Dict, Pattern, Tuple

from .. import strict_json
from ..errors import Error

_KEYWORDS = frozenset(("$comment", "$id", "$ref", "$schema", "additionalProperties",
                       "allOf", "anyOf", "const", "contains", "default", "definitions",
                       "description", "else", "enum", "format", "if", "items", "maximum",
                       "maxItems", "maxLength", "maxProperties", "minimum", "minItems",
                       "minLength", "minProperties", "multipleOf", "not", "oneOf", "pattern",
                       "properties", "propertyNames", "required", "then", "title", "type",
                       "uniqueItems", "x-wire-limits", "exclusiveMinimum", "exclusiveMaximum"))
_INTEGER = re.compile(r"-?(?:0|[1-9][0-9]*)\Z", re.ASCII)
_DATE = re.compile(r"([0-9]{4})-([0-9]{2})-([0-9]{2})[Tt]([0-9]{2}):([0-9]{2}):"
                   r"([0-9]{2})(?:\.[0-9]+)?(?:[Zz]|([+-])([0-9]{2}):([0-9]{2}))\Z")
_SCHEMAS = None
_PATTERNS: Dict[str, Tuple[Pattern[str], bool]] = {}
_LOCK = threading.Lock()
_NUMBERS = (int, float, strict_json.JsonInt, strict_json.JsonFloat)
# 与既有冻结消费者的 Unicode 空白集合一致，不使用 Python 多收 U+001C..001F 的 \s。
_SPACE = r"\u0009-\u000d\u0020\u0085\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000"


def _pattern(expression):
    cached = _PATTERNS.get(expression)
    if cached is not None:
        return cached
    if type(expression) is not str or not expression.startswith("^"):
        raise Error("contract", "unsupported frozen pattern")
    # 本集合的两个前缀模式不要求完整匹配；其余去掉 $，防其宽松接收尾部换行。
    prefix = expression in ("^/", "^/v3/sdk2/")
    if not prefix and not expression.endswith("$"):
        raise Error("contract", "unsupported frozen pattern boundary")
    body = expression[1:] if prefix else expression[1:-1]
    body = body.replace(r"\s", _SPACE).replace(r"\d", "[0-9]")
    compiled = re.compile(body, re.ASCII)
    result = (compiled, prefix)
    _PATTERNS[expression] = result
    return result


def _matches(expression, value):
    # 长 base64 避免反复正则分组，原形状不要求额外的规范 padding 位。
    if expression == r"^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$":
        if len(value) % 4:
            return False
        padding = len(value) - len(value.rstrip("="))
        return padding <= 2 and all(char in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
                                    for char in value[:len(value) - padding if padding else len(value)])
    compiled, prefix = _pattern(expression)
    return bool(compiled.match(value) if prefix else compiled.fullmatch(value))


def _audit(rule):
    if type(rule) is bool:
        return
    if type(rule) is not dict or any(key not in _KEYWORDS for key in rule):
        raise Error("contract", "unsupported frozen schema keyword")
    if "pattern" in rule:
        _pattern(rule["pattern"])
    if "format" in rule and rule["format"] != "date-time":
        raise Error("contract", "unsupported frozen schema format")
    for name in ("definitions", "properties"):
        for child in rule.get(name, {}).values():
            _audit(child)
    for name in ("allOf", "anyOf", "oneOf"):
        for child in rule.get(name, []):
            _audit(child)
    for name in ("additionalProperties", "contains", "items", "propertyNames", "if", "then", "else", "not"):
        if name in rule:
            _audit(rule[name])


def _schemas():
    global _SCHEMAS
    if _SCHEMAS is None:
        with _LOCK:
            if _SCHEMAS is None:
                from ._schema_data import SCHEMAS
                # 无运行时文件或远程 $ref；生成数据作为自有不可暴露缓存。
                copied = strict_json.snapshot(SCHEMAS, max_depth=64, max_nodes=200000)
                for schema in copied.values():
                    _audit(schema)
                _SCHEMAS = copied
    return _SCHEMAS


def _pointer(root, reference):
    if type(reference) is not str or not reference.startswith("#"):
        raise Error("contract", "external schema reference forbidden")
    current = root
    path = reference[1:]
    if path and not path.startswith("/"):
        raise Error("contract", "invalid schema pointer")
    for part in path[1:].split("/") if path else []:
        if re.search(r"~(?![01])", part):
            raise Error("contract", "invalid schema pointer escape")
        key = part.replace("~1", "/").replace("~0", "~")
        if type(current) is not dict or key not in current:
            raise Error("contract", "unresolved schema pointer")
        current = current[key]
    return current


def _key(value):
    kind = type(value)
    if kind is dict:
        return ("object", tuple((key, _key(value[key])) for key in sorted(value)))
    if kind is list:
        return ("array", tuple(_key(child) for child in value))
    if kind in _NUMBERS:
        return ("number", strict_json.number_lexeme(value))
    if kind is bool:
        return ("boolean", value)
    if value is None:
        return ("null",)
    return ("string", value)


def _integer(value):
    if type(value) not in _NUMBERS:
        return None
    token = strict_json.number_lexeme(value)
    return int(token) if _INTEGER.fullmatch(token) else None


def _type_matches(kind, value):
    actual = type(value)
    if kind == "integer":
        number = _integer(value)
        return number is not None and -(1 << 63) <= number <= (1 << 64) - 1
    if kind == "number":
        return actual in _NUMBERS
    expected = {"null": type(None), "boolean": bool, "object": dict, "array": list, "string": str}
    if kind not in expected:
        raise Error("contract", "unsupported frozen schema type")
    return actual is expected[kind]


def _date_time(value):
    match = _DATE.fullmatch(value)
    if not match:
        return False
    year, month, day, hour, minute, second = (int(match.group(i)) for i in range(1, 7))
    if not (1 <= month <= 12 and 0 <= hour <= 23 and 0 <= minute <= 59 and 0 <= second <= 60):
        return False
    days = (31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)
    leap = month == 2 and year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)
    return (1 <= day <= days[month - 1] + int(leap) and
            (match.group(7) is None or (int(match.group(8)) <= 23 and int(match.group(9)) <= 59)))


class _Checker:
    def __init__(self, root):
        self.root = root
        self.budget = 4000000

    @staticmethod
    def size(rule, size, low, high):
        return (low not in rule or size >= rule[low]) and (high not in rule or size <= rule[high])

    def check(self, rule, value, depth=0):
        self.budget -= 1
        if depth > 128 or self.budget < 0:
            raise Error("contract", "schema resource limit")
        if type(rule) is bool:
            return rule
        if "$ref" in rule:
            return self.check(_pointer(self.root, rule["$ref"]), value, depth + 1)
        if "const" in rule and _key(rule["const"]) != _key(value):
            return False
        if "enum" in rule and not any(_key(item) == _key(value) for item in rule["enum"]):
            return False
        if "type" in rule:
            kinds = [rule["type"]] if type(rule["type"]) is str else rule["type"]
            if not any(_type_matches(kind, value) for kind in kinds):
                return False
        for name in ("allOf", "anyOf", "oneOf"):
            if name in rule:
                matches = sum(self.check(child, value, depth + 1) for child in rule[name])
                if ((name == "allOf" and matches != len(rule[name])) or
                        (name == "anyOf" and matches == 0) or (name == "oneOf" and matches != 1)):
                    return False
        if "not" in rule and self.check(rule["not"], value, depth + 1):
            return False
        if "if" in rule:
            branch = "then" if self.check(rule["if"], value, depth + 1) else "else"
            if branch in rule and not self.check(rule[branch], value, depth + 1):
                return False
        kind = type(value)
        if kind is str:
            if not self.size(rule, len(value), "minLength", "maxLength"):
                return False
            if "pattern" in rule and not _matches(rule["pattern"], value):
                return False
            if "format" in rule and not _date_time(value):
                return False
        elif kind in _NUMBERS:
            if any(key in rule for key in ("minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum", "multipleOf")):
                number = _integer(value)
                if number is None:
                    return False
                if "multipleOf" in rule:
                    divisor = _integer(rule["multipleOf"])
                    if divisor is None or not 0 < divisor <= 9007199254740991:
                        raise Error("contract", "invalid frozen multipleOf")
                    if number % divisor:
                        return False
                for name in ("minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum"):
                    if name not in rule:
                        continue
                    bound = _integer(rule[name])
                    if bound is None:
                        raise Error("contract", "noninteger frozen bound")
                    if ((name == "minimum" and number < bound) or (name == "maximum" and number > bound) or
                            (name == "exclusiveMinimum" and number <= bound) or
                            (name == "exclusiveMaximum" and number >= bound)):
                        return False
        elif kind is list:
            if not self.size(rule, len(value), "minItems", "maxItems"):
                return False
            if rule.get("uniqueItems") and len({_key(item) for item in value}) != len(value):
                return False
            if "items" in rule and not all(self.check(rule["items"], item, depth + 1) for item in value):
                return False
            if "contains" in rule and not any(self.check(rule["contains"], item, depth + 1) for item in value):
                return False
        elif kind is dict:
            if not self.size(rule, len(value), "minProperties", "maxProperties"):
                return False
            if any(key not in value for key in rule.get("required", [])):
                return False
            properties = rule.get("properties", {})
            for key, item in value.items():
                if "propertyNames" in rule and not self.check(rule["propertyNames"], key, depth + 1):
                    return False
                member = properties.get(key, rule.get("additionalProperties", True))
                if not self.check(member, item, depth + 1):
                    return False
        return True


def validate_wire(family, definition, value):
    """成功返回 None，失败抛 Error；只接受被冻结的具名定义。"""
    if type(family) is not str or type(definition) is not str:
        raise Error("invalid_input", "schema family/definition must be str")
    root = _schemas().get(family)
    if root is None:
        raise Error("invalid_input", "unsupported schema family")
    rule = root.get("definitions", {}).get(definition)
    if rule is None:
        raise Error("invalid_input", "unknown schema definition")
    owned = strict_json.snapshot(value, max_depth=64, max_nodes=200000)
    if not _Checker(root).check(rule, owned):
        raise Error("contract", "wire value does not match frozen schema")


def validate_sandbox(kind, value, *, request=None):
    """独立 opt-in profile 的结构与跨字段语义；不代表 OS 隔离或用户批准。

    对应冻结 shell-sandbox-codec 的 Request/Response；校验响应时调用者应
    同时提供原 request，以验证动作、升级批准和原输出上限的关联。
    普通 validate_wire 仍只负责 schema，不能替代这里的上下文校验。
    """
    if kind not in ("request", "response"):
        raise Error("invalid_input", "unknown sandbox message kind")
    if kind == "request" and request is not None:
        raise Error("invalid_input", "request context applies only to a response")
    owned = strict_json.snapshot(value, max_depth=64, max_nodes=200000)
    validate_wire("terminal-shell-sandbox-v1", kind.capitalize(), owned)

    def fail():
        raise Error("contract", "invalid sandbox message correlation")

    if kind == "request":
        launch = owned["action"] == "exec" or owned["request"]["action"] == "launch"
        if (launch and "call" not in owned) or (not launch and
                                                ("call" in owned or "escalation" in owned)):
            fail()
        if "escalation" in owned:
            approved = owned["escalation"]["approvedCall"]
            if any(approved[key] != owned["call"][key] for key in ("turnId", "toolCallId")):
                fail()
        return

    state, result = owned["sandbox"], owned["result"]
    if ((state["escalated"] and
         (state["mode"] != "on" or state["capability"] == "none" or state["isolationDenied"])) or
            (state["isolationDenied"] and (state["mode"] == "off" or state["capability"] == "none")) or
            (result is None and not state["isolationDenied"])):
        fail()
    total = 0
    if owned["action"] == "exec":
        output = owned["output"]
        if (result is None) != (output is None):
            fail()
        if result is not None:
            if result["exitCode"] is not None and not -(1 << 31) <= int(result["exitCode"]) < (1 << 31):
                fail()
            out, err = len(result["stdout"].encode("utf-8")), len(result["stderr"].encode("utf-8"))
            total_out, total_err = output["stdout"]["totalBytes"], output["stderr"]["totalBytes"]
            total = total_out + total_err
            if (out + err > 4096 or out > total_out or err > total_err or total > 65536 or
                    output["previewTruncated"] != (out + err < total)):
                fail()
    if request is not None:
        original = strict_json.snapshot(request, max_depth=64, max_nodes=200000)
        validate_sandbox("request", original)
        if original["action"] != owned["action"] or (state["escalated"] and "escalation" not in original):
            fail()
        if result is not None:
            if original["action"] == "exec":
                if total > original["maxOutputBytes"]:
                    fail()
            elif original["request"]["action"] != result["action"]:
                fail()
