"""控制者与受限执行票共用协议传输，不在拒绝后切换权限入口。"""
import platform
from typing import Any, Dict, Optional

from ..errors import Error, MISSING
from ..lifecycle import CancellationToken
from ._common import (CONTROL_BYTES, PROTOCOL, TERMINAL, cancellation, equal,
                      expiry, live, snapshot, validate, validate_operation, validate_receipt)


def current_platform() -> Dict[str, str]:
    name = {"Darwin": "macos", "Windows": "windows", "Linux": "linux"}.get(platform.system())
    if name is None:
        raise Error("unsupported", "platform")
    machine = platform.machine().lower()
    arch = {"x86_64": "amd64", "x64": "amd64", "aarch64": "arm64"}.get(machine, machine)
    return {"platform": name, "arch": arch, "language": "python",
            "runtimeVersion": platform.python_version(), "adapterVersion": "python-executor-v1"}


def validate_registration(value: Dict[str, Any]) -> None:
    validate("ExecutorRegistrationRequest", value)
    for field, key in (("workspaces", "workspaceId"), ("tools", "name")):
        values = [item[key] for item in value.get(field, [])]
        if len(set(values)) != len(values):
            raise Error("invalid_request", "duplicate registration")
    if ("tool.invoke" in value["operations"]) != bool(value.get("tools")):
        raise Error("invalid_request", "tool registration mismatch")


class ExecutorClient:
    def __init__(self, client: Any, scope: Dict[str, Any], restricted: bool = False) -> None:
        validate("Scope", scope)
        self.api = client
        self._scope = snapshot(scope)
        self.restricted = bool(restricted)

    @property
    def scope(self) -> Dict[str, Any]:
        return snapshot(self._scope)

    def _controller(self) -> None:
        if self.restricted:
            raise Error("forbidden", "controller operation with executor-only ticket")

    def _call(self, operation: str, definition: str, *, family: str = PROTOCOL,
              status: int = 200, cancel: Optional[CancellationToken] = None,
              deadline_ms: Optional[int] = None, **options: Any) -> Dict[str, Any]:
        from ..api.operations import get_operation
        token = cancellation(cancel)
        deadline = deadline_ms if deadline_ms is not None else self.api.default_deadline_ms()
        token_deadline = getattr(token, "deadline_ms", None)
        if token_deadline is not None:
            deadline = min(deadline, token_deadline)
        token.check(deadline)
        # 自有快照先于 provider，manifest 决定请求 schema，禁止手写网络路径。
        prepared = {key: snapshot(value) for key, value in options.items()}
        body = prepared.get("body", MISSING)
        if body is not MISSING:
            request = get_operation(operation).get("request")
            if not request or "#" not in request:
                raise Error("invalid_operation")
            req_family, req_definition = request.split("#", 1)
            validate(req_definition, body, req_family)
        response = self.api.call(operation, cancel=token, deadline_ms=deadline,
                                 max_response_bytes=CONTROL_BYTES, **prepared)
        if response.status != status:
            raise Error("invalid_response", "executor response status")
        value = snapshot(response.body)
        validate(definition, value, family)
        return value

    def register(self, registration: Dict[str, Any], **options: Any) -> Dict[str, Any]:
        registration = snapshot(registration)
        validate_registration(registration)
        result = self._call("executor.register", "ExecutorConnection", body=registration,
                            status=201, **options)
        if result["executorId"] != registration["executorId"]:
            raise Error("conflict", "executor identity")
        live(result)
        return result

    register_executor = register

    def heartbeat(self, connection: Dict[str, Any], **options: Any) -> Dict[str, Any]:
        connection = snapshot(connection)
        deadline = live(connection)
        options["deadline_ms"] = min(deadline, options.get("deadline_ms") or self.api.default_deadline_ms())
        result = self._call("executor.heartbeat", "ExecutorConnection",
                            parameters={"id": connection["executorId"]},
                            body={"protocol": PROTOCOL, "executorId": connection["executorId"],
                                  "connectionId": connection["connectionId"]}, **options)
        if any(result[key] != connection[key] for key in ("executorId", "connectionId")):
            raise Error("conflict", "heartbeat identity")
        live(result)
        return result

    def check_identity(self, connection: Dict[str, Any], operation: Dict[str, Any]) -> None:
        target = operation["binding"]["target"]
        if (not equal(operation["scope"], self._scope) or
                any(target[key] != connection[key] for key in
                    ("executorId", "connectionId", "connectionRevision"))):
            raise Error("conflict", "execution scope or generation")

    def poll(self, connection: Dict[str, Any], **options: Any) -> Dict[str, Any]:
        connection = snapshot(connection)
        deadline = live(connection)
        options["deadline_ms"] = min(deadline, options.get("deadline_ms") or self.api.default_deadline_ms())
        result = self._call("executor.operations.poll", "ExecutionBatch",
                            parameters={"id": connection["executorId"]},
                            query={"connectionId": connection["connectionId"]}, **options)
        if any(result[key] != connection[key] for key in ("executorId", "connectionId")):
            raise Error("conflict", "poll identity")
        seen = set()
        for operation in result["operations"]:
            if operation["operationId"] in seen:
                raise Error("conflict", "duplicate operation")
            seen.add(operation["operationId"])
            validate_operation(operation)
            self.check_identity(connection, operation)
        return result

    def _capabilities(self, value: Dict[str, Any], session_id: str) -> Dict[str, Any]:
        names = [item["name"] for item in value["effectiveTools"]]
        if value["sessionId"] != session_id or len(set(names)) != len(names):
            raise Error("conflict", "execution capabilities")
        return value

    def initialize(self, session_id: str, platform_info: Dict[str, Any],
                   requested_tools: Any = None, closure: Optional[str] = None,
                   **options: Any) -> Dict[str, Any]:
        self._controller()
        body = {"protocol": PROTOCOL, "sessionId": session_id, "platform": snapshot(platform_info)}
        if requested_tools is not None:
            body["requestedTools"] = snapshot(requested_tools)
            if len(set(requested_tools)) != len(requested_tools):
                raise Error("invalid_request", "duplicate requested tools")
        result = self._call("execution.initialize", "SessionExecutionCapabilities",
                            parameters={"id": session_id}, body=body,
                            capability_closure=closure, **options)
        if not equal(result["platform"], body["platform"]):
            raise Error("conflict", "initialization platform")
        return self._capabilities(result, session_id)

    def capabilities(self, session_id: str, **options: Any) -> Dict[str, Any]:
        self._controller()
        return self._capabilities(self._call("execution.capabilities", "SessionExecutionCapabilities",
                                            parameters={"id": session_id}, **options), session_id)

    execution_capabilities = capabilities

    def bind(self, session_id: str, connection: Dict[str, Any], workspace: Dict[str, Any],
             capability_revision: str, closure: Optional[str] = None, **options: Any) -> Dict[str, Any]:
        self._controller()
        connection, workspace = snapshot(connection), snapshot(workspace)
        deadline = live(connection)
        options["deadline_ms"] = min(deadline, options.get("deadline_ms") or self.api.default_deadline_ms())
        body = {"protocol": PROTOCOL, "sessionId": session_id, "executorId": connection["executorId"],
                "connectionId": connection["connectionId"], "workspaceId": workspace["workspaceId"],
                "expectedCapabilityRevision": capability_revision}
        result = self._call("execution.binding.create", "SessionExecutionCapabilities",
                            parameters={"id": session_id}, body=body,
                            capability_closure=closure, **options)
        binding = result["binding"]
        if binding is None:
            raise Error("conflict", "missing binding")
        target = binding["target"]
        if (any(target[key] != connection[key] for key in
                ("executorId", "connectionId", "connectionRevision")) or
                target["workspaceId"] != workspace["workspaceId"] or
                target["workspaceRevision"] != workspace["revision"]):
            raise Error("conflict", "binding target")
        return self._capabilities(result, session_id)

    def negotiate_output(self, session: Dict[str, Any], binding: Dict[str, Any], request_id: str,
                         **options: Any) -> Dict[str, Any]:
        self._controller()
        session, binding = snapshot(session), snapshot(binding)
        result = self._call("terminal.binding.create", "BindingResponse", family=TERMINAL,
                            body={"contract": TERMINAL, "requestId": request_id, "session": session,
                                  "executionBinding": binding, "required": ["execution-stream-v1"],
                                  "optional": []}, **options)
        if (result["requestId"] != request_id or not equal(result["session"], session) or
                not equal(result["executionBinding"], binding) or not equal(result["scope"], self._scope) or
                "execution-stream-v1" not in result["accepted"]):
            raise Error("conflict", "terminal binding")
        return result

    def validate_status(self, value: Dict[str, Any]) -> None:
        validate("ExecutionStatus", value)
        operation = value["operation"]
        validate_operation(operation)
        if any(operation["scope"][key] != self._scope[key] for key in ("applicationScopeId", "endUserId")):
            raise Error("conflict", "status scope")
        if value["status"] == "pending" or value["status"] == "unknown" and value["receipt"] is None:
            if value["receipt"] is not None:
                raise Error("conflict", "pending receipt")
        else:
            if value["receipt"] is None or value["receipt"]["status"] != value["status"]:
                raise Error("conflict", "status receipt")
            validate_receipt(operation, value["receipt"])

    def status(self, session_id: str, operation_id: str, **options: Any) -> Dict[str, Any]:
        self._controller()
        result = self._call("execution.status", "ExecutionStatus",
                            parameters={"id": session_id, "targetId": operation_id}, **options)
        self.validate_status(result)
        if (result["operation"]["sessionId"] != session_id or
                result["operation"]["operationId"] != operation_id):
            raise Error("conflict", "status identity")
        return result

    def executor_status(self, session: Dict[str, Any], connection: Dict[str, Any],
                        operation: Dict[str, Any], **options: Any) -> Dict[str, Any]:
        session, connection, operation = snapshot(session), snapshot(connection), snapshot(operation)
        validate("SessionReference", session, TERMINAL)
        validate_operation(operation)
        self.check_identity(connection, operation)
        deadline = min(live(connection), expiry(operation["expiresAt"]))
        options["deadline_ms"] = min(deadline, options.get("deadline_ms") or self.api.default_deadline_ms())
        if session["sessionId"] != operation["sessionId"]:
            raise Error("conflict", "terminal session")
        result = self._call("terminal.execution.state", "ExecutionState", family=TERMINAL,
                            parameters={"id": connection["executorId"], "targetId": operation["operationId"]},
                            query={"sessionContract": session["sessionContract"], "sessionId": session["sessionId"],
                                   "requestDigest": operation["digest"], "connectionId": connection["connectionId"]},
                            **options)
        if not equal(result["session"], session) or not equal(result["execution"]["operation"], operation):
            raise Error("conflict", "terminal status identity")
        self.validate_status(result["execution"])
        return result["execution"]

    def submit(self, operation: Dict[str, Any], receipt: Dict[str, Any], **options: Any) -> Dict[str, Any]:
        operation, receipt = snapshot(operation), snapshot(receipt)
        validate_receipt(operation, receipt)
        if any(operation["scope"][key] != self._scope[key] for key in ("applicationScopeId", "endUserId")):
            raise Error("conflict", "receipt scope")
        result = self._call("executor.receipt.submit", "ExecutionStatus",
                            parameters={"id": receipt["executorId"]}, body=receipt, **options)
        self.validate_status(result)
        if (not equal(result["operation"], operation) or not equal(result["receipt"], receipt)
                or result["status"] != receipt["status"]):
            raise Error("conflict", "submitted receipt")
        return result
