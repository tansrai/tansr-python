"""档案语义校验；正文始终按原 bytes 验证，不作 JSON 重编码。"""

import base64
import copy
import datetime
import hashlib
from typing import Any, Dict, Optional

from .. import canonical
from ..api.schema import validate_wire
from ..errors import Error
from ..lifecycle import now_ms

PROTOCOL = "sdk2-ext-v1"
RECOVERY = "sdk2-archive-recovery-v1"


def need(condition: bool, code: str = "contract") -> None:
    if not condition:
        raise Error(code)


def clone(value: Any) -> Any:
    return copy.deepcopy(value)


def validate(name: str, value: Any) -> None:
    validate_wire(PROTOCOL, name, value)


def encode(value: Any, maximum: int = 2 << 20) -> bytes:
    raw = canonical.encode(value)
    need(len(raw) <= maximum, "capacity")
    return raw


def equal(first: Any, second: Any) -> bool:
    return encode(first) == encode(second)


def sha256(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def domain(name: str, body: bytes) -> str:
    return sha256(name.encode("ascii") + b"\x00" + body)


def unbase64(value: str) -> bytes:
    try:
        raw = base64.b64decode(value, validate=True)
        need(base64.b64encode(raw).decode("ascii") == value)
        return raw
    except (ValueError, TypeError):
        raise Error("contract") from None


def timestamp(value: str) -> int:
    try:
        parsed = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
        need(parsed.tzinfo is not None)
        return int(parsed.timestamp() * 1000)
    except (ValueError, TypeError, OverflowError):
        raise Error("contract") from None


def verify_epoch(epoch: Optional[Dict[str, Any]], maximum: int = 0, active: bool = False) -> None:
    if epoch is None:
        need(not active)
        return
    start, end = timestamp(epoch["issuedAt"]), timestamp(epoch["expiresAt"])
    need(end > start and (not maximum or end - start <= maximum))
    if active:
        need(epoch["state"] == "active" and start <= now_ms() < end)


def verify_binding(binding: Dict[str, Any]) -> None:
    validate("BindingView", binding)
    limits = binding["limits"]
    verify_epoch(binding["operationEpoch"], limits["epochLifetimeMs"])
    decisions = binding["acceptedCapabilities"] + [row["capability"] for row in binding["rejectedCapabilities"]]
    need(len(set(decisions)) == len(decisions))
    need(limits["inflightReserveBytes"] <= limits["pendingBytes"])
    need(
        ("archive-transfer-v1" in binding["acceptedCapabilities"])
        == (binding["archiveAckFormat"] == "split-receipts-v1")
    )


def verify_identity(identity: Dict[str, Any]) -> None:
    need(
        isinstance(identity, dict)
        and set(identity)
        == {"applicationScopeId", "endUserId", "bindingId", "sessionId", "generations", "sourceId", "sourceGeneration"}
    )
    for name in ("applicationScopeId", "bindingId", "sourceId", "sourceGeneration"):
        validate("Id", identity[name])
    for name in ("endUserId", "sessionId"):
        validate("LegacyId", identity[name])
    validate("Generations", identity["generations"])


def identity_from_binding(binding: Dict[str, Any], status: Dict[str, Any]) -> Dict[str, Any]:
    verify_binding(binding)
    validate("ArchiveStatus", status)
    need(all(equal(binding[key], status[key]) for key in ("bindingId", "sourceId", "revision", "state")))
    need(binding["state"] != "closed" and equal(binding["target"]["generations"], status["generations"]))
    return clone(
        dict(
            applicationScopeId=binding["scope"]["applicationScopeId"],
            endUserId=binding["scope"]["endUserId"],
            bindingId=binding["bindingId"],
            sessionId=binding["target"]["sessionId"],
            generations=binding["target"]["generations"],
            sourceId=binding["sourceId"],
            sourceGeneration=status["sourceGeneration"],
        )
    )


def verify_coverage(coverage: Dict[str, Any]) -> None:
    validate("Coverage", coverage)
    need(0 < int(coverage["fromSequence"]) <= int(coverage["throughSequence"]))


def references(record: Dict[str, Any]) -> list:
    return [record["payload"]] + record["attachments"]


def verify_record(record: Dict[str, Any], maximum: int = 262144) -> None:
    validate("ArchiveRecord", record)
    encode(record, maximum)
    semantic = {key: value for key, value in record.items() if key != "recordDigest"}
    need(domain("tansr.sdk2.record.v1", encode(semantic)) == record["recordDigest"])
    if "sourceEventRange" in record:
        need(record["sourceEventRange"]["firstSeq"] <= record["sourceEventRange"]["lastSeq"])
    if "projection" in record:
        verify_coverage(record["projection"]["coverage"])


def verify_page(binding: Dict[str, Any], after: Optional[str], page: Dict[str, Any]) -> None:
    validate("ArchivePage", page)
    need(page["bindingId"] == binding["bindingId"] and equal(page["generations"], binding["target"]["generations"]))
    limits, records = binding["limits"], page["records"]
    need(len(records) <= limits["pageRecords"])
    encode(page, limits["pageBytes"])
    if after is not None:
        validate("Sequence", after)
    previous, prior = int(after or "0"), None
    ids = set()
    refs = {}  # type: Dict[str, Any]
    for record in records:
        need(int(record["sequence"]) == previous + 1 and record["recordId"] not in ids)
        ids.add(record["recordId"])
        need(all(equal(record["target"][key], binding["target"][key]) for key in ("sessionId", "generations")))
        need(previous != 0 or record["predecessorDigest"] == "0" * 64)
        need(prior is None or record["predecessorDigest"] == prior)
        verify_record(record, limits["recordBytes"])
        for ref in references(record):
            need(ref["sourceId"] == binding["sourceId"] and ref["bytes"] <= limits["attachmentBytes"])
            need(ref["artifactId"] not in refs or equal(refs[ref["artifactId"]], ref))
            refs[ref["artifactId"]] = ref
        previous, prior = int(record["sequence"]), record["recordDigest"]
    need(page["nextAfterSequence"] == (records[-1]["sequence"] if records else after))
    published = page["publishedThroughSequence"]
    if published is None:
        need(not records and after is None and page["complete"])
    else:
        need(0 < int(published) and previous <= int(published))
        need(page["complete"] == (previous == int(published)) and (page["complete"] or records))


def verify_receipt(identity: Dict[str, Any], ack: Dict[str, Any], receipt: Dict[str, Any]) -> None:
    validate("MutationReceipt", receipt)
    need(receipt["state"] == "completed" and receipt["operation"] == "archive-ack")
    need(
        receipt["bindingId"] == identity["bindingId"] == ack["bindingId"]
        and equal(receipt["request"], ack["request"])
        and int(receipt["revision"]) > int(ack["expectedRevision"])
    )
    frame = {
        "scope": [identity["applicationScopeId"], identity["endUserId"]],
        "operation": "archive-ack",
        "semantic": {key: value for key, value in ack.items() if key != "request"},
    }
    need(domain("tansr.sdk2.operation.v1", encode(frame)) == receipt["semanticDigest"])


def verify_rebase(value: Dict[str, Any]) -> None:
    validate_wire(RECOVERY, "AckRebaseRequest", value)
    old = value["previous"]
    verify_coverage(old["coverage"])
    need(
        value["bindingId"] == old["bindingId"]
        and not equal(value["request"], old["request"])
        and value["request"]["operationEpoch"] == old["request"]["operationEpoch"]
    )
    encode(old, 262144)
    encode(value, 263168)


def verify_rebase_result(intent: Dict[str, Any], result: Dict[str, Any]) -> None:
    verify_rebase(intent)
    validate_wire(RECOVERY, "AckRebaseReceipt", result)
    need(all(equal(intent[key], result[key]) for key in ("protocol", "bindingId", "previous", "request")))
    need(
        equal(result["next"]["request"], intent["request"])
        and int(result["next"]["expectedRevision"]) > int(intent["previous"]["expectedRevision"])
    )
    old = clone(result["next"])
    old["request"], old["expectedRevision"] = intent["previous"]["request"], intent["previous"]["expectedRevision"]
    need(equal(old, intent["previous"]))
    encode(result["next"], 262144)
