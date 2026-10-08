"""执行协议的共同校验；控制 canonical 与业务 JSON 各守其边界。"""
import datetime
import hashlib
import re
from typing import Any, Dict, Optional

from .. import canonical, strict_json
from ..errors import Error
from ..lifecycle import CancellationToken, now_ms

PROTOCOL = "sdk2-ext-v1"
TERMINAL = "terminal-services-v1"
CONTROL_BYTES = 262144


def snapshot(value: Any) -> Any:
    return strict_json.snapshot(value)


def validate(name: str, value: Any, family: str = PROTOCOL) -> None:
    from ..api.schema import validate_wire
    validate_wire(family, name, value)


def equal(left: Any, right: Any) -> bool:
    return canonical.encode(left) == canonical.encode(right)


def expiry(value: str) -> int:
    if not isinstance(value, str) or not re.fullmatch(
            r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})", value):
        raise Error("invalid_response", "execution expiry")
    try:
        normalized = re.sub(r"\.(\d{6})\d+", r".\1", value.replace("Z", "+00:00"))
        # 3.7 的 fromisoformat 对小数秒仅接受 3 或 6 位。
        normalized = re.sub(r"\.(\d{1,5})(?=[+-])", lambda m: "." + m.group(1).ljust(6, "0"), normalized)
        instant = datetime.datetime.fromisoformat(normalized)
        return int(instant.timestamp() * 1000)
    except (ValueError, OverflowError):
        raise Error("invalid_response", "execution expiry")


def live(connection: Dict[str, Any]) -> int:
    validate("ExecutorConnection", connection)
    deadline = expiry(connection["expiresAt"])
    if deadline <= now_ms():
        raise Error("lease_expired")
    return deadline


def cancellation(cancel: Optional[CancellationToken] = None) -> CancellationToken:
    return cancel if cancel is not None else CancellationToken()


def operation_digest(operation: Dict[str, Any]) -> str:
    value = snapshot(operation)
    value["digest"] = "0" * 64
    validate("ExecutionOperation", value)
    del value["digest"]
    return hashlib.sha256(b"tansr.sdk2.execution.v1\0" + canonical.encode(value)).hexdigest()


def validate_operation(operation: Dict[str, Any]) -> None:
    from .tool import parse_tool_arguments
    if operation_digest(operation) != operation.get("digest"):
        raise Error("conflict", "operation digest")
    request = operation["request"]
    if request["operation"] == "tool.invoke":
        if request["args"]["name"] != operation["toolName"]:
            raise Error("conflict", "tool identity")
        parse_tool_arguments(request["args"]["argsJson"])
    elif request["operation"] == "process.exec":
        if not equal(operation["binding"]["target"].get("interpreter"),
                     request["args"]["interpreter"]):
            raise Error("conflict", "interpreter identity")


def receipt_for(operation: Dict[str, Any], status: str, code: Optional[str] = None,
                result: Any = None) -> Dict[str, Any]:
    target = operation["binding"]["target"]
    return {"protocol": PROTOCOL, "executorId": target["executorId"],
            "connectionId": target["connectionId"], "operationId": operation["operationId"],
            "digest": operation["digest"], "status": status, "result": result, "errorCode": code}


def validate_receipt(operation: Dict[str, Any], receipt: Dict[str, Any]) -> None:
    from .tool import verify_tool_result
    validate_operation(operation)
    validate("ExecutionReceiptRequest", receipt)
    target = operation["binding"]["target"]
    if (receipt["operationId"] != operation["operationId"] or
            receipt["digest"] != operation["digest"] or
            receipt["executorId"] != target["executorId"] or
            receipt["connectionId"] != target["connectionId"]):
        raise Error("conflict", "receipt identity")
    if receipt["status"] != "completed":
        if receipt["result"] is not None or receipt["errorCode"] is None:
            raise Error("invalid_response", "non-completed receipt")
        return
    result = receipt["result"]
    if (result is None or receipt["errorCode"] is not None or
            result["operation"] != operation["request"]["operation"]):
        raise Error("invalid_response", "completed receipt")
    if result["operation"] == "tool.invoke":
        verify_tool_result(strict_json.loads(result["args"]["resultJson"]))
