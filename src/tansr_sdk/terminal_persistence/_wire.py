"""独立 profile 使用原严格 schema 解释器，不覆盖旧 MemoryPublication。"""
from .. import strict_json
from ..api.schema import _Checker, _audit
from ..errors import Error
from ._schema import SCHEMA

_audit(SCHEMA)


def validate(definition, value):
    rule = SCHEMA["definitions"].get(definition)
    if rule is None or not _Checker(SCHEMA).check(rule, strict_json.snapshot(value)):
        raise Error("invalid_request", "terminal persistence schema rejected")
