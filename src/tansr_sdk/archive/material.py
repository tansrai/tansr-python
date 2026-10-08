"""点名材料回供；received 仅代表 Serve 收件，不代表核心消费。"""

import base64
from typing import Any, TYPE_CHECKING
from ..lifecycle import now_ms
from ._validation import PROTOCOL, clone, domain, equal, need, references, sha256, unbase64, validate, verify_record
from .intent import SavedIntent


def verify_upload(value: dict, binding: str, request: str, artifact: str) -> None:
    validate("MaterialUploadStatus", value)
    need(
        value["bindingId"] == binding
        and value["materialRequestId"] == request
        and value["artifact"]["artifactId"] == artifact
    )
    chunk, total = value["chunkBytes"], value["artifact"]["bytes"]
    need(chunk > 0)
    previous, size = -1, 0
    for offset in value["receivedOffsets"]:
        need(previous < offset < total and offset % chunk == 0)
        size += min(chunk, total - offset)
        previous = offset
    need(
        size == value["receivedBytes"]
        and (total + chunk - 1) // chunk <= 16
        and (value["state"] == "committed") == (size == total)
    )


class MaterialMethods:
    # 共享传输钩子由 ArchiveClient 实现；此 mixin 不另建网络引擎。
    if TYPE_CHECKING:
        _context: Any
        _call: Any
        _read: Any
        _mutation: Any
        _intent_context: Any

    def upload_material_chunk(self, body: dict, options: Any = None, **kwargs: Any) -> dict:
        validate("MaterialUploadChunkRequest", body)
        raw = unbase64(body["base64"])
        need(len(raw) == body["bytes"] and sha256(raw) == body["chunkSha256"])
        options = self._context(options, **kwargs)
        options.parameters = dict(id=body["bindingId"], targetId=body["materialRequestId"], uploadId=body["artifactId"])
        options.body = clone(body)
        value = self._call("material.upload.chunk", "MaterialUploadStatus", options)
        verify_upload(value, body["bindingId"], body["materialRequestId"], body["artifactId"])
        offset, chunk, total = body["offset"], value["chunkBytes"], value["artifact"]["bytes"]
        need(
            value["artifact"]["sourceId"] == body["sourceId"]
            and offset % chunk == 0
            and offset < total
            and body["bytes"] == min(chunk, total - offset)
            and offset in value["receivedOffsets"]
        )
        if offset == 0 and body["bytes"] == total:
            need(value["artifact"]["sha256"] == body["chunkSha256"])
        return value

    def material_upload_status(
        self, binding: str, request: str, artifact: str, options: Any = None, **kwargs: Any
    ) -> dict:
        for identity in (binding, request, artifact):
            validate("Id", identity)
        options = self._read(binding, self._context(options, **kwargs))
        options.parameters.update(targetId=request, uploadId=artifact)
        value = self._call("material.upload.status", "MaterialUploadStatus", options)
        verify_upload(value, binding, request, artifact)
        return value

    def prepare_materials(
        self, store: Any, request: SavedIntent, identity: dict, options: Any = None, **kwargs: Any
    ) -> dict:
        """冷恢复读取已保存 material-request，原绝对截止覆盖陈旧 remainingTtlMs。"""
        need(isinstance(request, SavedIntent) and request.kind == "material-request", "invalid_input")
        return self.prepare_materials_before(store, request.body, identity, request.deadline_ms, options, **kwargs)

    def prepare_materials_before(
        self, store: Any, request: dict, identity: dict, original_deadline_ms: int, options: Any = None, **kwargs: Any
    ) -> dict:
        """上传前校验整组原字节和全部额度；调用方须保存返回原响应后再提交。"""
        validate("MaterialRequest", request)
        validate("RequestIdentity", identity)
        need(isinstance(original_deadline_ms, int), "invalid_input")
        options = self._context(options, **kwargs)
        options.deadline_ms = min(options.deadline_ms, original_deadline_ms, now_ms() + request["remainingTtlMs"])
        options = self._context(options)
        store.check_access()
        expected, limits = store.identity(), store.limits()
        limits.validate()
        need(all(equal(request[key], expected[key]) for key in ("bindingId", "sourceId", "sourceGeneration")))
        need(
            request["target"]["sessionId"] == expected["sessionId"]
            and equal(request["target"]["generations"], expected["generations"])
        )
        chunk = request["chunkBytes"]
        need(chunk > 0)
        ids = [item["recordId"] for item in request["requestedRecords"]]
        need(len(ids) == len(set(ids)))
        saved = store.records_by_id(ids)
        need(len(saved) == len(ids))
        records = {}  # type: dict
        refs = {}  # type: dict
        total = 0
        for record in saved:
            verify_record(record)
            need(record["recordId"] not in records)
            records[record["recordId"]] = record
        need(set(records) == set(ids))
        for asked in request["requestedRecords"]:
            record = records[asked["recordId"]]
            need(
                record["recordDigest"] == asked["digest"]
                and equal(record["payload"], asked["payload"])
                and equal(record["attachments"], asked["attachments"])
            )
            need(all(equal(record["target"][key], request["target"][key]) for key in ("sessionId", "generations")))
            for ref in references(record):
                need(ref["sourceId"] == expected["sourceId"] and (ref["bytes"] + chunk - 1) // chunk <= 16, "capacity")
                if ref["artifactId"] in refs:
                    need(equal(refs[ref["artifactId"]], ref))
                else:
                    total += ref["bytes"]
                    refs[ref["artifactId"]] = ref
        need(
            total <= min(request["maxBytes"], limits.max_batch_bytes) and len(refs) <= limits.max_artifacts, "capacity"
        )
        bodies = {}
        for artifact, ref in refs.items():
            options.cancel.check(options.deadline_ms)
            raw = store.body(ref)
            need(isinstance(raw, bytes) and len(raw) == ref["bytes"] and sha256(raw) == ref["sha256"])
            bodies[artifact] = raw
        for record in saved:
            need(domain("tansr.sdk2.payload.v1", bodies[record["payload"]["artifactId"]]) == record["payloadDigest"])

        def chunks(artifact: str) -> Any:
            raw = bodies[artifact]
            for offset in range(0, len(raw), chunk):
                part = raw[offset : offset + chunk]
                yield dict(
                    protocol=PROTOCOL,
                    bindingId=request["bindingId"],
                    materialRequestId=request["materialRequestId"],
                    target=clone(request["target"]),
                    sourceId=request["sourceId"],
                    sourceGeneration=request["sourceGeneration"],
                    artifactId=artifact,
                    offset=offset,
                    bytes=len(part),
                    chunkSha256=sha256(part),
                    base64=base64.b64encode(part).decode("ascii"),
                )

        # 包括分块 schema 在内的首发前完整校验；坏的末块不能造成部分发送。
        for artifact in refs:
            for body in chunks(artifact):
                validate("MaterialUploadChunkRequest", body)
                options.cancel.check(options.deadline_ms)
        uploads = {}
        for artifact, ref in refs.items():
            last = None
            for body in chunks(artifact):
                store.check_access()
                last = self.upload_material_chunk(body, options)
                need(equal(last["artifact"], ref) and last["chunkBytes"] == chunk)
            if last is None or last["state"] != "committed":
                store.check_access()
                last = self.material_upload_status(
                    request["bindingId"], request["materialRequestId"], artifact, options
                )
            need(last["state"] == "committed" and equal(last["artifact"], ref))
            uploads[artifact] = dict(uploadId=last["uploadId"])
        results = [
            dict(
                recordId=asked["recordId"],
                digest=asked["digest"],
                payload=uploads[asked["payload"]["artifactId"]],
                attachments=[uploads[ref["artifactId"]] for ref in asked["attachments"]],
            )
            for asked in request["requestedRecords"]
        ]
        response = dict(
            protocol=PROTOCOL,
            request=clone(identity),
            bindingId=request["bindingId"],
            materialRequestId=request["materialRequestId"],
            target=clone(request["target"]),
            sourceId=request["sourceId"],
            sourceGeneration=request["sourceGeneration"],
            results=results,
        )
        validate("MaterialResponseRequest", response)
        store.check_access()
        options.cancel.check(options.deadline_ms)
        return response

    def submit_materials(self, intent: SavedIntent, options: Any = None, **kwargs: Any) -> dict:
        options = self._intent_context(intent, "material-response", options, **kwargs)
        body = intent.body
        validate("MaterialResponseRequest", body)
        ids = [row["recordId"] for row in body["results"]]
        need(len(set(ids)) == len(ids))
        value = self._call(
            "material.response.submit", "MaterialReceipt", self._mutation(body["bindingId"], body, options), 202
        )
        need(
            value["bindingId"] == body["bindingId"]
            and value["materialRequestId"] == body["materialRequestId"]
            and value["state"] == "received"
            and value["revision"] != "0"
            and len(value["acceptedRecordIds"]) == len(ids)
            and set(value["acceptedRecordIds"]) == set(ids)
        )
        return value

    def material_status(self, binding: str, request: str, options: Any = None, **kwargs: Any) -> dict:
        validate("Id", binding)
        validate("Id", request)
        options = self._read(binding, self._context(options, **kwargs))
        options.parameters["targetId"] = request
        value = self._call("material.status", "MaterialReceipt", options)
        need(value["bindingId"] == binding and value["materialRequestId"] == request)
        need(len(value["acceptedRecordIds"]) == len(set(value["acceptedRecordIds"])))
        if value["state"] == "pending":
            need(value["revision"] == "0" and not value["acceptedRecordIds"])
        return value
