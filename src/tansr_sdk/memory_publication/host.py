"""冻结保留工具的显式宿主接线；不向模型注册业务工具。"""
from typing import Any

from .. import strict_json
from ..errors import Error
from ..executor._common import equal, snapshot, validate, validate_operation
from .store import PublicationError

TOOL_NAME = "TansrTerminalMemoryPublication"
DEFINITION_DIGEST = "8532a582d40d2d8993a80db59412a89671670ed7f994eaf9bf972bd544a111b0"


def valid_invocation(operation: dict) -> bool:
    args = operation["request"]["args"]
    if (operation["toolName"] != "MemoryPublication" or args["name"] != TOOL_NAME or
            args["definitionDigest"] != DEFINITION_DIGEST):
        return False
    validate("MemoryPublicationRequest", strict_json.loads(args["argsJson"].encode("utf-8"), max_bytes=32768),
             "terminal-services-v1")
    return True


class Host:
    """只有显式安装才可运行；存储与执行日志均须声明真实加密能力。"""
    def __init__(self, store: Any, *, require_encryption: bool = True) -> None:
        if (getattr(store, "atomic_durable_publication", False) is not True or
                require_encryption and getattr(store, "encrypted_at_rest", False) is not True):
            raise Error("unsupported", "durable publication storage required")
        self.store = store
        self.require_encryption = require_encryption
        self._identity = snapshot(store.identity)
        validate("Scope", dict(self._identity["scope"], authorizationRevision="1"))
        validate("MemoryPublicationRequest", dict(contract="terminal-services-v1", action="head",
            **{key: self._identity[key] for key in ("sourceId", "sourceGeneration", "domainKey")}),
            "terminal-services-v1")

    def registration(self) -> dict:
        return dict(name=TOOL_NAME, definitionDigest=DEFINITION_DIGEST)

    def check_journal(self, journal: Any) -> None:
        if self.require_encryption and getattr(journal, "encrypted_at_rest", False) is not True:
            raise Error("unsupported", "publication execution receipts require an encrypted journal")

    def execute(self, operation: dict, context: Any, arguments: dict) -> dict:
        operation, arguments = snapshot(operation), snapshot(arguments)
        validate_operation(operation)
        if (operation["request"]["operation"] != "tool.invoke" or not valid_invocation(operation) or
                not equal(strict_json.loads(operation["request"]["args"]["argsJson"]), arguments) or
                any(operation["scope"][key] != value for key, value in self._identity["scope"].items()) or
                any(arguments[key] != self._identity[key] for key in ("sourceId", "sourceGeneration", "domainKey"))):
            raise Error("forbidden", "publication invocation identity rejected")
        context.check()
        owner = {key: operation[key] for key in ("scope", "sessionId", "binding")}
        try:
            response = self.store.execute(arguments, owner, cancel=context.cancel, deadline_ms=context.deadline_ms)
            validate("MemoryPublicationResponse", response, "terminal-services-v1")
            return dict(status="ok", content=[dict(t="text", text=strict_json.dumps(response).decode("utf-8"))])
        except PublicationError as error:
            return dict(status="error", message=error.code)
        # 提交失证沿原Runner unknown；不借用确定失败重执CAS。
