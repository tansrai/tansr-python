"""批准的持久化保留 profile；只处理存储，不注册普通业务工具。"""
from .. import strict_json
from ..errors import Error
from ..executor._common import snapshot, validate_operation
from ._state import StorageError
from ._wire import validate

TOOL_NAME = "TansrTerminalPersistenceV1"
DEFINITION_DIGEST = "33029a264edf81f3fda2a13fc382403d0cd7ffefa1f38088bb9366f387a13587"


def valid_invocation(operation):
    args = operation["request"]["args"]
    if (operation["toolName"] != "MemoryPublication" or args["name"] != TOOL_NAME or
            args["definitionDigest"] != DEFINITION_DIGEST):
        return False
    validate("Request", strict_json.loads(args["argsJson"].encode("utf-8"), max_bytes=32768))
    return True


class Host:
    """显式独立存储与加密 journal；原 operation/scope/binding 围栏始终保留。"""
    def __init__(self, store, *, require_encryption=True):
        if (getattr(store, "atomic_durable_persistence", False) is not True or
                require_encryption and getattr(store, "encrypted_at_rest", False) is not True):
            raise Error("unsupported", "atomic durable persistence storage required")
        self._identity = snapshot(store.identity)
        validate("Identity", self._identity)
        self._execute = store.execute
        self.require_encryption = require_encryption or getattr(store, "encrypted_at_rest", False) is True

    def registration(self):
        return dict(name=TOOL_NAME, definitionDigest=DEFINITION_DIGEST)

    def check_journal(self, journal):
        if self.require_encryption and getattr(journal, "encrypted_at_rest", False) is not True:
            raise Error("unsupported", "persistence receipts require encrypted journal")

    def execute(self, operation, context, arguments):
        operation, arguments = snapshot(operation), snapshot(arguments)
        validate_operation(operation)
        if (operation["request"]["operation"] != "tool.invoke" or not valid_invocation(operation) or
                strict_json.loads(operation["request"]["args"]["argsJson"]) != arguments or
                any(operation["scope"][key] != self._identity[key] for key in ("applicationScopeId", "endUserId")) or
                any(arguments[key] != self._identity[key] for key in ("sourceId", "sourceGeneration", "domainKey"))):
            raise Error("forbidden", "persistence invocation identity rejected")
        context.check()
        owner = {key: operation[key] for key in ("scope", "sessionId", "binding")}
        try:
            response = self._execute(arguments, owner, cancel=context.cancel, deadline_ms=context.deadline_ms)
            validate("Response", response)
            if (any(response[key] != arguments[key] for key in ("contract", "action", "sourceId", "sourceGeneration", "domainKey")) or
                    "transferId" in arguments and any(response["transfer"][key] != arguments[key] for key in ("transferId", "intentSha256")) or
                    "commitRoot" in arguments and response["commitRoot"] != arguments["commitRoot"]):
                raise Error("integrity", "persistence response did not match original request")
            return dict(status="ok", content=[dict(t="text", text=strict_json.dumps(response).decode("utf-8"))])
        except StorageError as error:
            return dict(status="error", message=error.code)
        # 不确定提交沿 Runner 原 unknown 处理，不凭失败重执副作用。
