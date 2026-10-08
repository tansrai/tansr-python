"""一次同步和宿主显式恢复；未知副作用不换身份重放。"""

from typing import Any
from ..errors import Error
from ._validation import (
    PROTOCOL,
    domain,
    encode,
    equal,
    identity_from_binding,
    need,
    references,
    sha256,
    validate,
    verify_epoch,
    verify_page,
    verify_receipt,
    verify_rebase_result,
)
from .types import SyncResult


def _matches(error: Error, code: str, domains: tuple, reason: Any = None) -> bool:
    detail = error.detail if isinstance(error.detail, dict) else {}
    return (
        (error.wire_code or error.code) == code
        and detail.get("domainCode") in domains
        and (reason is None or detail.get("reason") == reason)
    )


def _pending_receipt(client: Any, store: Any, pending: dict, options: Any) -> dict:
    store.check_access()
    saved = store.pending_deadline()
    need(saved is not None)
    options = client._context(options, deadline_ms=min(options.deadline_ms, saved))
    receipt = client.acknowledge(pending, options)
    verify_receipt(store.identity(), pending, receipt)
    return receipt


def _resume_rebase(client: Any, store: Any, intent: dict, options: Any) -> SyncResult:
    store.check_access()
    saved = store.pending_deadline()
    need(saved is not None)
    options = client._context(options, deadline_ms=min(options.deadline_ms, saved))
    try:
        result = client.rebase_acknowledgement(intent, options)
    except Error as error:
        if _matches(error, "conflict", ("request_id_conflict",)):
            try:
                receipt = client.operation(intent["bindingId"], "archive-ack", intent["previous"]["request"], options)
            except Error:
                raise error
            verify_receipt(store.identity(), intent["previous"], receipt)
            store.confirm(receipt)
            return SyncResult(recovered=True, receipt=receipt)
        raise
    verify_rebase_result(intent, result)
    verify_receipt(store.identity(), result["next"], result["receipt"])
    store.confirm_rebase(result)
    return SyncResult(recovered=True, receipt=result["receipt"])


def sync_once(client: Any, store: Any, request_id: str, options: Any = None, **kwargs: Any) -> SyncResult:
    options = client._context(options, **kwargs)
    store.check_access()
    intent = store.pending_rebase()
    if intent is not None:
        return _resume_rebase(client, store, intent, options)
    pending = store.pending()
    if pending is not None:
        receipt = _pending_receipt(client, store, pending, options)
        store.confirm(receipt)
        return SyncResult(recovered=True, receipt=receipt)
    identity = store.identity()
    binding, status = client.binding(identity["bindingId"], options), client.status(identity["bindingId"], options)
    need(equal(identity_from_binding(binding, status), identity))
    head = store.head()
    after = head["sequence"] if head else None
    page = client.records(binding, after, options)
    verify_page(binding, after, page)
    if not page["records"]:
        return SyncResult(complete=page["complete"])
    need(status["publishedThroughSequence"] == page["publishedThroughSequence"], "snapshot_changed")
    need(page["records"][0]["predecessorDigest"] == (head["recordDigest"] if head else "0" * 64))
    coverage = status["acknowledgedCoverage"]
    need(
        (head is None and coverage is None)
        or (
            head is not None
            and coverage is not None
            and head["sequence"] == coverage["throughSequence"]
            and head["recordDigest"] == coverage["headDigest"]
        )
    )
    need(binding["operationEpoch"] is not None)
    request = dict(requestId=request_id, operationEpoch=binding["operationEpoch"]["id"])
    validate("RequestIdentity", request)
    refs = {}  # type: dict
    total, limits = 0, store.limits()
    limits.validate()
    for record in page["records"]:
        total += len(encode(record))
        for ref in references(record):
            if ref["artifactId"] not in refs:
                total += ref["bytes"]
            else:
                need(equal(refs[ref["artifactId"]], ref))
            refs[ref["artifactId"]] = ref
    need(total <= limits.max_batch_bytes and len(refs) <= limits.max_artifacts, "capacity")
    bodies = {}
    for artifact, ref in refs.items():
        store.check_access()
        bodies[artifact] = client.artifact(binding, ref, options)
        need(
            isinstance(bodies[artifact], bytes)
            and len(bodies[artifact]) == ref["bytes"]
            and sha256(bodies[artifact]) == ref["sha256"]
        )
    payloads = {}  # type: dict
    attachments = {}  # type: dict
    for record in page["records"]:
        need(domain("tansr.sdk2.payload.v1", bodies[record["payload"]["artifactId"]]) == record["payloadDigest"])
        for index, ref in enumerate(references(record)):
            target = payloads if index == 0 else attachments
            target[ref["artifactId"]] = dict(artifactId=ref["artifactId"], sha256=ref["sha256"], state="durably-stored")
    expected_ack = dict(
        protocol=PROTOCOL,
        request=request,
        bindingId=identity["bindingId"],
        expectedRevision=binding["revision"],
        generations=identity["generations"],
        sourceId=identity["sourceId"],
        sourceGeneration=identity["sourceGeneration"],
        coverage=dict(
            fromSequence=page["records"][0]["sequence"],
            throughSequence=page["records"][-1]["sequence"],
            headDigest=page["records"][-1]["recordDigest"],
        ),
        attachments=list(attachments.values()),
        payloads=list(payloads.values()),
        ackFormat="split-receipts-v1",
    )
    validate("ArchiveAckRequest", expected_ack)
    encode(expected_ack, binding["limits"]["controlBytes"])
    ack = store.receive(binding, status, page, bodies, request, options.deadline_ms)
    # 注入介质不能换掉协议请求；耐久性由 ArchiveStore 合同保证。
    need(equal(ack, expected_ack) and equal(store.pending(), ack) and store.pending_deadline() == options.deadline_ms)
    store.check_access()
    receipt = client.acknowledge(ack, options)
    verify_receipt(identity, ack, receipt)
    store.confirm(receipt)
    return SyncResult(len(page["records"]), page["complete"], False, receipt)


def recover_pending(client: Any, store: Any, request_id: str, options: Any = None, **kwargs: Any) -> SyncResult:
    options = client._context(options, **kwargs)
    store.check_access()
    intent = store.pending_rebase()
    if intent is not None:
        return _resume_rebase(client, store, intent, options)
    pending = store.pending()
    need(pending is not None, "pending_ack")
    try:
        receipt = _pending_receipt(client, store, pending, options)
    except Error as error:
        if not _matches(error, "precondition_failed", ("binding_conflict", "stale_revision"), "if_match_stale"):
            raise
    else:
        store.confirm(receipt)
        return SyncResult(recovered=True, receipt=receipt)
    binding = client.binding(pending["bindingId"], options)
    verify_epoch(binding["operationEpoch"], active=True)
    need(binding["operationEpoch"]["id"] == pending["request"]["operationEpoch"])
    # 新恢复是一个显式新操作；原 ACK 的正文与截止永远保留。
    request = dict(requestId=request_id, operationEpoch=pending["request"]["operationEpoch"])
    intent = store.prepare_rebase(request, options.deadline_ms)
    need(equal(intent["request"], request) and equal(intent["previous"], pending))
    return _resume_rebase(client, store, intent, options)
