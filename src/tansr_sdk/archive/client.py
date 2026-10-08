"""只消费生成的统一 API 操作；不另建网络、凭据或重试层。"""

import copy
from typing import Any, Optional
from .. import strict_json
from ..api import CallOptions
from ..api.schema import validate_wire
from ..lifecycle import CancellationToken
from ._validation import (
    PROTOCOL,
    clone,
    equal,
    need,
    sha256,
    unbase64,
    validate,
    verify_binding,
    verify_coverage,
    verify_epoch,
    verify_page,
    verify_rebase,
    verify_rebase_result,
)
from .intent import SavedIntent
from .material import MaterialMethods


class ArchiveClient(MaterialMethods):
    """sdk1/offload 共用高层，会话族沿用注入的 api.Client。"""

    def __init__(self, client: Any) -> None:
        self.api = client

    def default_deadline_ms(self) -> int:
        return self.api.default_deadline_ms()

    def _context(self, options: Optional[CallOptions] = None, **kwargs: Any) -> CallOptions:
        value = copy.copy(options) if options is not None else CallOptions()
        for key, item in kwargs.items():
            need(hasattr(value, key), "invalid_input")
            setattr(value, key, item)
        value.parameters, value.query = dict(value.parameters or {}), dict(value.query or {})
        if value.deadline_ms is None:
            value.deadline_ms = self.default_deadline_ms()
        need(isinstance(value.deadline_ms, int) and not isinstance(value.deadline_ms, bool), "invalid_input")
        value.deadline_ms = int(value.deadline_ms)
        if value.max_response_bytes is not None:
            need(
                isinstance(value.max_response_bytes, int) and not isinstance(value.max_response_bytes, bool),
                "invalid_input",
            )
            value.max_response_bytes = int(value.max_response_bytes)
        if value.cancel is None:
            value.cancel = CancellationToken()
        value.cancel.check(value.deadline_ms)
        return value

    def _call(self, operation: str, definition: str, options: CallOptions, status: int = 200) -> dict:
        response = self.api.call(operation, self._context(options))
        need(response.status == status)
        validate(definition, response.body)
        return clone(response.body)

    def _read(self, identity: str, options: CallOptions) -> CallOptions:
        result = self._context(options)
        if identity:
            result.parameters["id"] = identity
        result.query["protocol"] = PROTOCOL
        return result

    def _mutation(self, identity: str, body: dict, options: CallOptions) -> CallOptions:
        result = self._context(options)
        if identity:
            result.parameters["id"] = identity
        result.body = clone(body)
        result.request_key = body["request"]["requestId"]
        if "expectedRevision" in body:
            result.if_match = '"' + body["expectedRevision"] + '"'
        return result

    def _intent_context(self, intent: SavedIntent, kind: str, options: Any = None, **kwargs: Any) -> CallOptions:
        need(isinstance(intent, SavedIntent) and intent.kind == kind, "invalid_input")
        value = self._context(options, **kwargs)
        value.deadline_ms = min(value.deadline_ms or 0, intent.deadline_ms)
        return self._context(value)

    def capabilities(self, options: Any = None, **kwargs: Any) -> dict:
        value = self._call("archive.capabilities", "CapabilitiesResponse", self._context(options, **kwargs))
        limits = value["limits"]
        need(limits["inflightReserveBytes"] <= limits["pendingBytes"])
        need(
            value["archiveAckFormats"]
            == (["split-receipts-v1"] if "archive-transfer-v1" in value["capabilities"] else [])
        )
        verify_epoch(value["operationEpoch"], limits["epochLifetimeMs"])
        return value

    def binding_target(self, session: str, options: Any = None, **kwargs: Any) -> dict:
        validate("LegacyId", session)
        value = self._call(
            "archive.binding.target", "BindingTargetView", self._read(session, self._context(options, **kwargs))
        )
        need(value["target"]["sessionId"] == session)
        if value["bindingId"] is None:
            need(value["revision"] == "0" and value["operationEpoch"] is None)
        verify_epoch(value["operationEpoch"])
        return value

    def prepare_create(self, session: str, source: str, request_id: str, options: Any = None, **kwargs: Any) -> dict:
        options = self._context(options, **kwargs)
        caps = self.capabilities(options)
        need(
            "archive-transfer-v1" in caps["capabilities"] and "split-receipts-v1" in caps["archiveAckFormats"],
            "capability_unavailable",
        )
        target = self.binding_target(session, options)
        need(target["bindingId"] is None, "binding_exists")
        verify_epoch(caps["operationEpoch"], active=True)
        body = dict(
            protocol=PROTOCOL,
            request=dict(requestId=request_id, operationEpoch=caps["operationEpoch"]["id"]),
            target=target["target"],
            expectedRevision=target["revision"],
            requiredCapabilities=["archive-transfer-v1"],
            optionalCapabilities=["context-materials-v1"] if "context-materials-v1" in caps["capabilities"] else [],
            archive=dict(
                strategy="single-authorized-source",
                sourceId=source,
                durability="source-ack-with-durable-spool",
                delivery="required",
                sessionAvailability="legacy-complete",
                ackFormat="split-receipts-v1",
            ),
        )
        validate("BindingCreateRequest", body)
        return body

    def create_binding(self, intent: SavedIntent, options: Any = None, **kwargs: Any) -> dict:
        options = self._intent_context(intent, "binding-create", options, **kwargs)
        body = intent.body
        validate("BindingCreateRequest", body)
        requested = body["requiredCapabilities"] + body["optionalCapabilities"]
        need(len(set(requested)) == len(requested))
        value = self._call("archive.binding.create", "BindingView", self._mutation("", body, options), 201)
        verify_binding(value)
        decisions = value["acceptedCapabilities"] + [row["capability"] for row in value["rejectedCapabilities"]]
        need(
            equal(value["target"], body["target"])
            and value["sourceId"] == body["archive"]["sourceId"]
            and set(decisions) == set(requested)
            and all(cap in value["acceptedCapabilities"] for cap in body["requiredCapabilities"])
        )
        return value

    def close_binding(self, body: dict, options: Any = None, **kwargs: Any) -> dict:
        validate("BindingCloseRequest", body)
        value = self._call(
            "archive.binding.close",
            "MutationReceipt",
            self._mutation(body["bindingId"], body, self._context(options, **kwargs)),
        )
        need(
            value["bindingId"] == body["bindingId"]
            and equal(value["request"], body["request"])
            and value["operation"] == "binding-close"
        )
        return value

    def binding(self, identity: str, options: Any = None, **kwargs: Any) -> dict:
        validate("Id", identity)
        value = self._call("archive.binding.get", "BindingView", self._read(identity, self._context(options, **kwargs)))
        need(value["bindingId"] == identity)
        verify_binding(value)
        return value

    def status(self, identity: str, options: Any = None, **kwargs: Any) -> dict:
        validate("Id", identity)
        value = self._call("archive.status", "ArchiveStatus", self._read(identity, self._context(options, **kwargs)))
        need(value["bindingId"] == identity)
        for key in ("publishedThroughSequence", "releasableThroughSequence"):
            need(value[key] is None or int(value[key]) > 0)
        coverage = value["acknowledgedCoverage"]
        if coverage is None:
            need(value["releasableThroughSequence"] is None)
        else:
            verify_coverage(coverage)
            need(
                value["publishedThroughSequence"] is not None
                and int(value["publishedThroughSequence"]) >= int(coverage["throughSequence"])
            )
            need(
                value["releasableThroughSequence"] is None
                or int(value["releasableThroughSequence"]) <= int(coverage["throughSequence"])
            )
        return value

    def records(self, binding: dict, after: Optional[str] = None, options: Any = None, **kwargs: Any) -> dict:
        verify_binding(binding)
        options = self._read(binding["bindingId"], self._context(options, **kwargs))
        options.query.update(binding["target"]["generations"])
        options.query.update(limit=str(binding["limits"]["pageRecords"]), maxBytes=str(binding["limits"]["pageBytes"]))
        options.max_response_bytes = binding["limits"]["pageBytes"]
        if after is not None:
            validate("Sequence", after)
            options.query["afterSequence"] = after
        value = self._call("archive.records.read", "ArchivePage", options)
        verify_page(binding, after, value)
        return value

    def artifact(self, binding: dict, reference: dict, options: Any = None, **kwargs: Any) -> bytes:
        options = self._context(options, **kwargs)
        verify_binding(binding)
        validate("ArtifactRef", reference)
        need(
            reference["sourceId"] == binding["sourceId"] and reference["bytes"] <= binding["limits"]["attachmentBytes"]
        )
        output = bytearray()
        while len(output) < reference["bytes"]:
            maximum = min(binding["limits"]["chunkBytes"], reference["bytes"] - len(output))
            need(maximum > 0)
            request = self._read(binding["bindingId"], options)
            request.parameters["targetId"] = reference["artifactId"]
            request.query.update(binding["target"]["generations"])
            request.query.update(offset=str(len(output)), maxBytes=str(maximum))
            request.max_response_bytes = 1 << 20
            chunk = self._call("archive.artifact.read", "ArtifactChunk", request)
            raw = unbase64(chunk["base64"])
            need(all(chunk[key] == reference[key] for key in ("artifactId", "sourceId", "sha256")))
            need(
                chunk["bindingId"] == binding["bindingId"]
                and equal(chunk["generations"], binding["target"]["generations"])
                and chunk["totalBytes"] == reference["bytes"]
                and chunk["offset"] == len(output)
                and 0 < chunk["bytes"] <= maximum
                and len(raw) == chunk["bytes"]
                and sha256(raw) == chunk["chunkSha256"]
            )
            output.extend(raw)
        need(sha256(bytes(output)) == reference["sha256"])
        return bytes(output)

    def acknowledge(self, ack: dict, options: Any = None, **kwargs: Any) -> dict:
        validate("ArchiveAckRequest", ack)
        verify_coverage(ack["coverage"])
        need(int(ack["coverage"]["throughSequence"]) - int(ack["coverage"]["fromSequence"]) < 128)
        value = self._call(
            "archive.ack.commit",
            "MutationReceipt",
            self._mutation(ack["bindingId"], ack, self._context(options, **kwargs)),
        )
        need(
            value["bindingId"] == ack["bindingId"]
            and equal(value["request"], ack["request"])
            and value["operation"] == "archive-ack"
        )
        return value

    def operation(self, binding: str, operation: str, request: dict, options: Any = None, **kwargs: Any) -> dict:
        body = dict(protocol=PROTOCOL, bindingId=binding, operation=operation, request=request)
        validate("OperationStatusRequest", body)
        options = self._context(options, **kwargs)
        options.query = dict(protocol=PROTOCOL, bindingId=binding, operation=operation, **request)
        value = self._call("archive.operation.query", "MutationReceipt", options)
        need(equal(value["request"], request) and value["operation"] == operation and value["bindingId"] == binding)
        return value

    def creation_operation(self, session: str, request: dict, options: Any = None, **kwargs: Any) -> dict:
        body = dict(protocol=PROTOCOL, sessionId=session, operation="binding-create", request=request)
        validate("OperationStatusRequest", body)
        options = self._context(options, **kwargs)
        options.query = dict(protocol=PROTOCOL, sessionId=session, operation="binding-create", **request)
        value = self._call("archive.operation.query", "MutationReceipt", options)
        need(equal(value["request"], request) and value["operation"] == "binding-create")
        return value

    def rebase_acknowledgement(self, body: dict, options: Any = None, **kwargs: Any) -> dict:
        verify_rebase(body)
        options = self._mutation(body["bindingId"], body, self._context(options, **kwargs))
        options.max_response_bytes = 528384
        response = self.api.call("archive.ack.rebase", options)
        need(response.status == 200)
        verify_rebase_result(body, response.body)
        return clone(response.body)

    def events(
        self, binding: dict, cursor: Optional[str] = None, options: Any = None, **kwargs: Any
    ) -> "ArchiveEventStream":
        verify_binding(binding)
        options = self._read(binding["bindingId"], self._context(options, **kwargs))
        options.last_event_id = cursor
        return ArchiveEventStream(self.api.events("archive.events.observe", options), binding)

    def sync_once(self, store: Any, request_id: str, options: Any = None, **kwargs: Any) -> Any:
        from .sync import sync_once

        return sync_once(self, store, request_id, options, **kwargs)

    def recover_pending(self, store: Any, request_id: str, options: Any = None, **kwargs: Any) -> Any:
        from .sync import recover_pending

        return recover_pending(self, store, request_id, options, **kwargs)


class ArchiveEventStream:
    def __init__(self, stream: Any, binding: dict) -> None:
        self._stream, self._binding = iter(stream), clone(binding)
        self._resource = stream
        self._closed = False

    def __iter__(self) -> "ArchiveEventStream":
        return self

    def __next__(self) -> Any:
        if self._closed:
            raise StopIteration
        try:
            frame = next(self._stream)
            envelope = strict_json.loads(frame.data)
            validate_wire("unified-v1", "EventEnvelope", envelope)
            raw = envelope["raw"]
            validate("EventFrame", raw)
            need(
                raw["bindingId"] == self._binding["bindingId"]
                and equal(raw["generations"], self._binding["target"]["generations"])
            )
            return frame
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._resource.close()

    def __enter__(self) -> "ArchiveEventStream":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()
