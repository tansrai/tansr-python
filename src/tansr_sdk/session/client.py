"""两会话族的共享同步状态机。异步门面只适配调度，不复制协议。"""

import re
from dataclasses import replace
from typing import Any, Dict, List, Optional

from ..api import CallOptions
from ..errors import MISSING
from ..lifecycle import CancellationToken
from ..strict_json import snapshot
from ._validation import (
    MEDIA_BYTES,
    blocks_json,
    check_family,
    contract,
    finite_number,
    invalid,
    linked_cancel,
    object_response,
    read_meta,
    required_string,
    safe_integer,
    text,
)
from .types import (
    Accepted,
    Answer,
    CapabilityClosure,
    Checkpoint,
    CompactOptions,
    CreateOptions,
    Created,
    Input,
    InputTarget,
    LabeledCheckpoint,
    Meta,
    ResumeReference,
    SessionList,
    SpeechRequest,
    TranscriptionRequest,
    WriteOptions,
)


def _write_options(value: Optional[WriteOptions]) -> CallOptions:
    if value is None:
        value = WriteOptions()
    if not isinstance(value, WriteOptions):
        raise invalid("WriteOptions required")
    return CallOptions(request_key=value.request_key, deadline_ms=value.deadline_ms, cancel=value.cancel)


def _create_body(value: CreateOptions, family: str) -> Dict[str, Any]:
    if value.resume is not None and value.fork is not None:
        raise invalid("resume and fork are mutually exclusive")
    if family == "sdk2-offload-v1":
        valid_id = isinstance(value.request_id, str) and re.fullmatch(r"[A-Za-z0-9_-]{1,128}", value.request_id)
        if value.fork is not None or (value.resume is None and not valid_id):
            raise invalid("offload create requires retained request_id and does not support fork")
    elif value.request_id is not None:
        raise invalid("request_id belongs to offload creation; SDK1 uses request_key")
    body = {}  # type: Dict[str, Any]
    for key, item in (
        ("requestId", value.request_id),
        ("model", value.model),
        ("prompt", value.prompt),
        ("profile", value.profile),
        ("capabilitiesProfile", value.capabilities_profile),
        ("cwd", value.cwd),
    ):
        if item is not None:
            body[key] = text(item)
    if value.budget is not None:
        budget = {}
        if value.budget.max_usd is not None:
            usd = finite_number(value.budget.max_usd)
            if usd < 0:
                raise invalid("budget must be nonnegative")
            budget["maxUsd"] = usd
        if value.budget.max_tokens is not None:
            budget["maxTokens"] = safe_integer(value.budget.max_tokens)
        body["budget"] = budget
    if value.tools is not None:
        if not isinstance(value.tools, (list, tuple)):
            raise invalid("tools must be an array")
        body["tools"] = [text(item, nonempty=True) for item in value.tools]
    if value.client_tools is not None:
        if not isinstance(value.client_tools, (list, tuple)) or not all(
            isinstance(item, dict) for item in value.client_tools
        ):
            raise invalid("client_tools must contain objects")
        body["clientTools"] = list(value.client_tools)
    if value.resume is not None:
        body["resume"] = {"sessionId": text(value.resume.session_id, nonempty=True)}
    if value.fork is not None:
        body["fork"] = {
            "sessionId": text(value.fork.session_id, nonempty=True),
            "checkpointId": text(value.fork.checkpoint_id, nonempty=True),
        }
    return snapshot(body)


def _input_body(value: Input) -> Dict[str, Any]:
    identity = text(value.input_id, nonempty=True, limit=128)
    target = {
        "historyEpoch": text(value.target.history_epoch, nonempty=True, limit=128),
        "turnId": text(value.target.turn_id, nonempty=True, limit=128),
    }
    if value.ack is not None and value.ack not in ("memory", "durable"):
        raise invalid("input ACK must be memory or durable")
    if (value.content.text is None) == (value.content.blocks is None):
        raise invalid("input requires exactly one text form")
    content = {}  # type: Dict[str, Any]
    if value.content.text is not None:
        content = {"text": text(value.content.text, nonempty=True, limit=262144)}
    else:
        content = {"blocks": blocks_json(value.content.blocks, text_only=True)}
    body = {"inputId": identity, "target": target, "content": content}
    if value.ack is not None:
        body["ack"] = value.ack
    return body


def _check_receipt(raw: Any, session_id: str, input_id: str, target: Dict[str, str], ack: Optional[str] = None) -> None:
    if not isinstance(raw, dict) or (
        raw.get("sessionId") != session_id
        or raw.get("inputId") != input_id
        or raw.get("turnId") != target["turnId"]
        or raw.get("historyEpoch") != target["historyEpoch"]
        or raw.get("source") != "strict"
        or raw.get("state") not in ("reserved", "accepted", "consumed", "closed", "cancelled")
        or raw.get("durability") not in ("memory", "durable")
        or (ack is not None and raw.get("durability") != ack)
    ):
        raise contract("input receipt changed identity, state or requested durability")
    safe_integer(raw.get("ordinal"), response=True)
    safe_integer(raw.get("revision"), response=True)


class SessionClient:
    """借用API客户端；本对象不接管API传输的所有权。"""

    def __init__(self, client: Any, cancel: Optional[CancellationToken] = None) -> None:
        if client is None or client.family not in ("sdk1", "sdk2-offload-v1"):
            raise invalid("unsupported session family")
        self.api = client
        self.cancel = cancel

    def _call(self, operation: str, options: CallOptions) -> Any:
        with linked_cancel(self.cancel, options.cancel) as cancel:
            return self.api.call(operation, replace(options, cancel=cancel))

    def _discover_family(self, options: CallOptions) -> None:
        response = self._call(
            "session.capabilities",
            CallOptions(
                query={"protocol": "sdk2-ext-v1"},
                deadline_ms=options.deadline_ms,
                cancel=options.cancel,
            ),
        )
        raw = object_response(response)
        entries = raw.get("contracts")
        if raw.get("protocol") != "sdk2-ext-v1" or not isinstance(entries, list) or len(entries) > 2:
            raise contract("invalid session family discovery")
        found = set()
        for entry in entries:
            family = required_string(entry, "contract")
            availability = required_string(entry, "availability")
            if family in found or (family, availability) not in (
                ("sdk1", "legacy-complete"),
                ("sdk2-offload-v1", "source-required"),
            ):
                raise contract("unsupported or duplicate session family")
            found.add(family)
        if self.api.family not in found:
            raise invalid("selected family unavailable; no write attempted")

    def create(self, options: Optional[CreateOptions] = None, **fields: Any) -> "Session":
        if options is not None and fields:
            raise invalid("pass CreateOptions or keyword fields, not both")
        value = options if options is not None else CreateOptions(**fields)
        if not isinstance(value, CreateOptions):
            raise invalid("CreateOptions required")
        body = _create_body(value, self.api.family)
        call = _write_options(value.write)
        if call.deadline_ms is None:
            call.deadline_ms = self.api.default_deadline_ms()
        self._discover_family(call)
        call.body = body
        response = self._call("session.create", call)
        if response.status not in (200, 201):
            raise contract("unexpected create status")
        raw = object_response(response, response.status)
        identity = required_string(raw, "sessionId")
        seq = safe_integer(raw.get("lastSeq"), response=True)
        if not isinstance(raw.get("resumed"), bool):
            raise contract("create response lacks resumed flag")
        if "resume" in body and body["resume"]["sessionId"] != identity:
            raise contract("resume changed session identity")
        check_family(raw, self.api.family)
        return Session(self, Created(identity, raw["resumed"], seq))

    def attach(
        self, session_id: str, *, cancel: Optional[CancellationToken] = None, deadline_ms: Optional[int] = None
    ) -> "Session":
        result = Session(self, Created(text(session_id, nonempty=True)))
        meta = result.meta(cancel=cancel, deadline_ms=deadline_ms)
        result.created = Created(session_id, False, meta.last_seq)
        return result

    def resume(self, session_id: str, write: Optional[WriteOptions] = None) -> "Session":
        return self.create(CreateOptions(resume=ResumeReference(session_id), write=write or WriteOptions()))

    def list(
        self,
        offset: int = 0,
        limit: int = 100,
        *,
        cancel: Optional[CancellationToken] = None,
        deadline_ms: Optional[int] = None,
    ) -> SessionList:
        safe_integer(offset)
        if safe_integer(limit) == 0:
            raise invalid("session list limit must be positive")
        raw = object_response(
            self._call(
                "session.list",
                CallOptions(
                    query={"offset": str(offset), "limit": str(limit)},
                    cancel=cancel,
                    deadline_ms=deadline_ms,
                ),
            )
        )
        total = safe_integer(raw.get("total"), response=True)
        if not isinstance(raw.get("sessions"), list):
            raise contract("session list lacks sessions")
        return SessionList([read_meta(item, self.api.family) for item in raw["sessions"]], total)


class Session:
    """已知身份的会话句柄；关闭观察流绝不隐式中断服务端会话。"""

    def __init__(self, client: SessionClient, created: Created) -> None:
        self.client = client
        self.created = created

    @property
    def id(self) -> str:
        return self.created.session_id

    @property
    def api(self) -> Any:
        return self.client.api

    def capabilities(
        self, *, cancel: Optional[CancellationToken] = None, deadline_ms: Optional[int] = None
    ) -> CapabilityClosure:
        response = self.client._call(
            "discovery.session.capabilities",
            CallOptions(
                parameters={"id": self.id},
                cancel=cancel,
                deadline_ms=deadline_ms,
            ),
        )
        raw = object_response(response)
        closure = required_string(raw, "closureId")
        if response.capability_closure != closure or not isinstance(raw.get("operations"), dict):
            raise contract("capability closure header differs from body or lacks operations")
        operations = raw["operations"]
        if any(
            not isinstance(key, str) or value not in ("enabled", "disabled", "unavailable")
            for key, value in operations.items()
        ):
            raise contract("invalid capability state")
        return CapabilityClosure(closure, dict(operations), raw)

    def _read(
        self,
        operation: str,
        query: Optional[Dict[str, str]] = None,
        *,
        cancel: Optional[CancellationToken] = None,
        deadline_ms: Optional[int] = None,
    ) -> Any:
        return self.client._call(
            operation,
            CallOptions(
                parameters={"id": self.id},
                query=query or {},
                cancel=cancel,
                deadline_ms=deadline_ms,
            ),
        )

    def _write(
        self,
        operation: str,
        body: Any,
        write: Optional[WriteOptions],
        extra: Optional[Dict[str, str]] = None,
        **fields: Any,
    ) -> Any:
        options = _write_options(write)
        if body is not MISSING:
            options.body = snapshot(body)
        for key, value in fields.items():
            setattr(options, key, value)
        return self._write_call(operation, options, extra)

    def _write_call(self, operation: str, options: CallOptions, extra: Optional[Dict[str, str]] = None) -> Any:
        if options.deadline_ms is None:
            options.deadline_ms = self.api.default_deadline_ms()
        closure = self.capabilities(cancel=options.cancel, deadline_ms=options.deadline_ms)
        if closure.operations.get(operation) != "enabled":
            raise invalid("operation is not enabled; no write attempted")
        options.parameters = dict(extra or {}, id=self.id)
        options.capability_closure = closure.closure_id
        if operation in ("session.audio.speak", "session.audio.transcribe"):
            options.max_response_bytes = MEDIA_BYTES
        return self.client._call(operation, options)

    def _accepted(
        self,
        operation: str,
        body: Any,
        write: Optional[WriteOptions],
        with_session: bool,
        extra: Optional[Dict[str, str]] = None,
    ) -> Accepted:
        response = self._write(operation, body, write, extra)
        return self._acceptance(response, with_session)

    def _acceptance(self, response: Any, with_session: bool) -> Accepted:
        raw = object_response(response, 202 if with_session else 200)
        identity = raw.get("sessionId")
        if raw.get("accepted") is not True or (with_session and identity != self.id):
            raise contract("invalid acceptance receipt")
        if identity is not None and (not isinstance(identity, str) or identity != self.id):
            raise contract("acceptance receipt changed session identity")
        return Accepted(True, identity)

    def meta(
        self,
        *,
        cancel: Optional[CancellationToken] = None,
        deadline_ms: Optional[int] = None,
        include_application_prompt: bool = False,
    ) -> Meta:
        query = {"include": "applicationPrompt"} if include_application_prompt else {}
        result = read_meta(
            object_response(
                self._read(
                    "session.get",
                    query,
                    cancel=cancel,
                    deadline_ms=deadline_ms,
                )
            ),
            self.api.family,
        )
        if result.session_id != self.id:
            raise contract("metadata changed session identity")
        return result

    def application_prompt_meta(
        self, *, cancel: Optional[CancellationToken] = None, deadline_ms: Optional[int] = None
    ) -> Meta:
        return self.meta(cancel=cancel, deadline_ms=deadline_ms, include_application_prompt=True)

    def send(self, prompt: str, write: Optional[WriteOptions] = None) -> Accepted:
        if not text(prompt, nonempty=True).strip():
            raise invalid("prompt must not be blank")
        return self._accepted("session.message.send", {"prompt": prompt}, write, True)

    def send_blocks(self, blocks: list, write: Optional[WriteOptions] = None) -> Accepted:
        return self._accepted("session.message.send", {"blocks": blocks_json(blocks)}, write, True)

    def interrupt(self, write: Optional[WriteOptions] = None) -> Accepted:
        """显式请求远端中断；返回受理，不表示当前轮已经结束。"""
        return self._accepted("session.interrupt", {}, write, True)

    def close(self, write: Optional[WriteOptions] = None) -> Accepted:
        """显式关闭远端会话；不是本地资源清理方法。"""
        options = _write_options(write)
        options.parameters = {"id": self.id}
        return self._acceptance(self.client._call("session.close", options), True)

    def permission(self, ticket: str, digest: str, verdict: str, write: Optional[WriteOptions] = None) -> Accepted:
        text(ticket, nonempty=True)
        text(digest, nonempty=True)
        if verdict not in ("allow", "deny"):
            raise invalid("invalid permission decision")
        return self._accepted(
            "session.permission.decide", {"digest": digest, "verdict": verdict}, write, False, {"ticketId": ticket}
        )

    def answer(self, ticket: str, answers: List[Answer], write: Optional[WriteOptions] = None) -> Accepted:
        text(ticket, nonempty=True)
        if not isinstance(answers, (list, tuple)) or not answers:
            raise invalid("question answers are required")
        result = []
        for answer in answers:
            if not isinstance(answer, Answer) or not isinstance(answer.selected_option_ids, (list, tuple)):
                raise invalid("Answer with selected_option_ids required")
            item = {
                "questionId": text(answer.question_id, nonempty=True),
                "selectedOptionIds": [text(option, nonempty=True) for option in answer.selected_option_ids],
            }
            if answer.free_text is not None:
                item["freeText"] = text(answer.free_text)
            result.append(item)
        return self._accepted("session.question.answer", {"answers": result}, write, False, {"ticketId": ticket})

    def tool_result(self, call_id: str, receipt: Dict[str, Any], write: Optional[WriteOptions] = None) -> Accepted:
        text(call_id, nonempty=True)
        if not isinstance(receipt, dict):
            raise invalid("tool receipt object required")
        return self._accepted("session.tool.result", receipt, write, False, {"targetId": call_id})

    def history(
        self,
        offset: int = 0,
        limit: int = 100,
        *,
        cancel: Optional[CancellationToken] = None,
        deadline_ms: Optional[int] = None,
    ) -> Dict[str, Any]:
        safe_integer(offset)
        safe_integer(limit)  # 0 是合法的历史元信息请求。
        return object_response(
            self._read(
                "session.history.read",
                {"offset": str(offset), "limit": str(limit)},
                cancel=cancel,
                deadline_ms=deadline_ms,
            )
        )

    def input_capabilities(
        self, *, cancel: Optional[CancellationToken] = None, deadline_ms: Optional[int] = None
    ) -> Dict[str, Any]:
        return object_response(self._read("session.input.capabilities", cancel=cancel, deadline_ms=deadline_ms))

    def submit_input(self, value: Input, write: Optional[WriteOptions] = None) -> Dict[str, Any]:
        body = _input_body(value)
        options = _write_options(write)
        if options.deadline_ms is None:
            options.deadline_ms = self.api.default_deadline_ms()
        if body.get("ack") == "durable":
            capabilities = self.input_capabilities(cancel=options.cancel, deadline_ms=options.deadline_ms)
            if capabilities.get("durableAck") is not True:
                raise invalid("durable ACK unavailable; no write attempted")
        options.body = body
        raw = object_response(self._write_call("session.input.submit", options), 202)
        if raw.get("outcome") != "accepted":
            raise contract("input was not accepted")
        _check_receipt(raw.get("receipt"), self.id, body["inputId"], body["target"], body.get("ack"))
        return raw

    def input_status(
        self,
        input_id: str,
        target: InputTarget,
        *,
        cancel: Optional[CancellationToken] = None,
        deadline_ms: Optional[int] = None,
    ) -> Dict[str, Any]:
        text(input_id, nonempty=True, limit=128)
        query = {
            "historyEpoch": text(target.history_epoch, nonempty=True, limit=128),
            "turnId": text(target.turn_id, nonempty=True, limit=128),
        }
        raw = object_response(
            self.client._call(
                "session.input.status",
                CallOptions(
                    parameters={"id": self.id, "targetId": input_id},
                    query=query,
                    cancel=cancel,
                    deadline_ms=deadline_ms,
                ),
            )
        )
        _check_receipt(raw.get("receipt"), self.id, input_id, query)
        return raw

    def compact(self, options: Optional[CompactOptions] = None, write: Optional[WriteOptions] = None) -> Dict[str, Any]:
        value = options or CompactOptions()
        body = {}  # type: Dict[str, Any]
        if value.instructions is not None:
            body["instructions"] = text(value.instructions, limit=4096)
        if value.checkpoint is not None:
            if isinstance(value.checkpoint, bool):
                body["checkpoint"] = value.checkpoint
            elif isinstance(value.checkpoint, LabeledCheckpoint):
                body["checkpoint"] = (
                    {} if value.checkpoint.label is None else {"label": text(value.checkpoint.label, limit=120)}
                )
            else:
                raise invalid("checkpoint must be bool or LabeledCheckpoint")
        raw = object_response(self._write("session.compact", body, write))
        status = raw.get("status")
        if status == "compacted":
            required_string(raw, "compactionId")
            removed = raw.get("removedRange")
            if not isinstance(removed, list) or len(removed) != 2:
                raise contract("compaction range must contain two safe integers")
            if safe_integer(removed[0], response=True) > safe_integer(removed[1], response=True):
                raise contract("invalid compaction range")
        elif status == "rejected":
            if raw.get("reason") not in ("empty_history", "not_configured", "hook_blocked"):
                raise contract("invalid compaction rejection")
        elif status == "failed":
            required_string(raw, "reason")
        else:
            raise contract("invalid compaction status")
        return raw

    def _checkpoint(self, raw: Dict[str, Any]) -> Checkpoint:
        checkpoint_id = required_string(raw, "checkpointId")
        identity = required_string(raw, "sessionId")
        count = safe_integer(raw.get("messageCount"), response=True)
        if identity != self.id:
            raise contract("checkpoint changed session identity")
        return Checkpoint(checkpoint_id, identity, count, raw)

    def checkpoints(
        self, *, cancel: Optional[CancellationToken] = None, deadline_ms: Optional[int] = None
    ) -> List[Checkpoint]:
        raw = object_response(self._read("session.checkpoint.list", cancel=cancel, deadline_ms=deadline_ms))
        if not isinstance(raw.get("checkpoints"), list):
            raise contract("checkpoint list lacks checkpoints")
        return [self._checkpoint(item) for item in raw["checkpoints"]]

    def checkpoint(self, label: str = "", write: Optional[WriteOptions] = None) -> Checkpoint:
        return self._checkpoint(
            object_response(self._write("session.checkpoint.create", {"label": text(label, limit=120)}, write), 201)
        )

    def restore(
        self, checkpoint_id: str, checkpoint: bool = True, write: Optional[WriteOptions] = None
    ) -> Dict[str, Any]:
        text(checkpoint_id, nonempty=True)
        if not isinstance(checkpoint, bool):
            raise invalid("checkpoint must be boolean")
        raw = object_response(
            self._write("session.checkpoint.restore", {"checkpoint": checkpoint}, write, {"targetId": checkpoint_id})
        )
        if raw.get("status") != "restored" or raw.get("checkpointId") != checkpoint_id:
            raise contract("invalid restore receipt")
        safe_integer(raw.get("fromMessages"), response=True)
        safe_integer(raw.get("toMessages"), response=True)
        return raw

    def delete_checkpoint(self, checkpoint_id: str, write: Optional[WriteOptions] = None) -> None:
        text(checkpoint_id, nonempty=True)
        response = self._write("session.checkpoint.delete", MISSING, write, {"targetId": checkpoint_id})
        if response.status != 204 or response.raw_body:
            raise contract("checkpoint deletion must return empty 204")

    def export_checkpoint(
        self, checkpoint_id: str, *, cancel: Optional[CancellationToken] = None, deadline_ms: Optional[int] = None
    ) -> bytes:
        text(checkpoint_id, nonempty=True)
        response = self.client._call(
            "session.checkpoint.export",
            CallOptions(
                parameters={"id": self.id, "targetId": checkpoint_id},
                max_response_bytes=MEDIA_BYTES,
                cancel=cancel,
                deadline_ms=deadline_ms,
            ),
        )
        content_type = response.headers.get("content-type", "").split(";", 1)[0].strip().lower()
        if response.status != 200 or content_type != "application/octet-stream" or not response.raw_body:
            raise contract("invalid binary checkpoint response")
        return bytes(response.raw_body)

    def import_checkpoint(self, data: bytes, label: str = "", write: Optional[WriteOptions] = None) -> Checkpoint:
        if not isinstance(data, bytes) or not 1 <= len(data) <= MEDIA_BYTES:
            raise invalid("checkpoint requires immutable bytes containing 1 to 32 MiB")
        text(label, limit=120)
        response = self._write(
            "session.checkpoint.import",
            MISSING,
            write,
            raw_body=data,
            content_type="application/octet-stream",
            max_response_bytes=MEDIA_BYTES,
            query={"label": label} if label else {},
        )
        return self._checkpoint(object_response(response, 201))

    def set_cwd(self, cwd: str, write: Optional[WriteOptions] = None) -> Dict[str, Any]:
        return object_response(self._write("session.cwd.set", {"cwd": text(cwd, nonempty=True)}, write))

    def transcribe(self, request: TranscriptionRequest, write: Optional[WriteOptions] = None) -> Dict[str, Any]:
        body = {"audio": text(request.audio, nonempty=True)}  # type: Dict[str, Any]
        for key in ("model", "language", "prompt"):
            value = getattr(request, key)
            if value is not None:
                body[key] = text(value)
        if request.diarize is not None:
            if not isinstance(request.diarize, bool):
                raise invalid("diarize must be boolean")
            body["diarize"] = request.diarize
        return object_response(self._write("session.audio.transcribe", body, write))

    def speak(self, request: SpeechRequest, write: Optional[WriteOptions] = None) -> Dict[str, Any]:
        body = {"input": text(request.input, nonempty=True)}  # type: Dict[str, Any]
        for key in ("model", "voice", "format"):
            value = getattr(request, key)
            if value is not None:
                body[key] = text(value)
        if request.format is not None and request.format not in ("mp3", "wav"):
            raise invalid("speech format must be mp3 or wav")
        if request.speed is not None:
            body["speed"] = finite_number(request.speed)
        return object_response(self._write("session.audio.speak", body, write))

    def events(
        self,
        last_event_id: Optional[str] = None,
        *,
        cancel: Optional[CancellationToken] = None,
        deadline_ms: Optional[int] = None,
    ) -> Any:
        from .events import SessionEventStream

        return SessionEventStream.open(self, last_event_id, cancel=cancel, deadline_ms=deadline_ms)
