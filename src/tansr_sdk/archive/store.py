"""加密单端档案事务，原字节与原 ACK 在同一耐久提交中保存。"""

import base64
import os
import threading
from contextlib import contextmanager
from typing import Any, Callable, Dict, Iterator, Optional

from ..errors import Error
from ..lifecycle import now_ms
from ..storage import EncryptedStore, PrivateDirectory
from ._validation import (
    PROTOCOL,
    clone,
    domain,
    encode,
    equal,
    identity_from_binding,
    need,
    references,
    sha256,
    unbase64,
    validate,
    verify_coverage,
    verify_identity,
    verify_page,
    verify_rebase,
    verify_rebase_result,
    verify_receipt,
    verify_record,
)
from .types import ArchiveStore, StoreLimits

FORMAT = "tansr-python-archive-v1"
REBASE_RESERVE = 528384


def _head(state: dict) -> Optional[dict]:
    if not state["records"]:
        return None
    last = state["records"][-1]
    return {"sequence": last["sequence"], "recordDigest": last["recordDigest"]}


def _pending_row(state: dict) -> Optional[dict]:
    return next((row for row in state["rebases"] if row["result"] is None and row["originalReceipt"] is None), None)


def _reserved(state: dict, request: dict) -> bool:
    if state["lastReceipt"] is not None and equal(state["lastReceipt"]["request"], request):
        return True
    return any(
        equal(row["intent"]["request"], request) or equal(row["intent"]["previous"]["request"], request)
        for row in state["rebases"]
    )


class FileStore(ArchiveStore):
    """打开宿主配置的私有绝对路径，密钥与当前身份必须由宿主提供。

    每次事务重写有界快照；不兼容其它 SDK 的私有磁盘格式。文件错误保持原件，
    不自动清空。close 等待当前事务结束后才释放跨进程锁。
    """

    def __init__(
        self,
        path: str,
        key: bytes,
        key_id: str,
        identity: dict,
        check_access: Callable[[dict], Any],
        limits: Optional[StoreLimits] = None,
        *,
        max_encryptions: int = 1 << 20,
        commit_hook: Any = None,
    ) -> None:
        need(os.path.isabs(path) and callable(check_access), "invalid_input")
        verify_identity(identity)
        validate("Id", key_id)
        self._identity = clone(identity)
        self._limits = limits or StoreLimits()
        self._limits.validate()
        self._access = check_access
        self._mutex = threading.RLock()
        self._entered = False
        self._closed = False
        self._uncertain = False
        self._hook = commit_hook
        self._directory = None  # type: Any
        self._encrypted = None  # type: Any
        self._state = {}  # type: dict
        self._authorize()
        maximum = self._limits.max_stored_bytes * 2 + (4 << 20)
        try:
            self._directory = PrivateDirectory(
                os.path.dirname(path),
                check_access=self._authorize,
                max_file_bytes=maximum + 8192,
                max_total_bytes=maximum * 3,
            )
            self._encrypted = EncryptedStore(
                self._directory, os.path.basename(path), key, key_id, max_bytes=maximum, max_encryptions=max_encryptions
            )
            saved = self._encrypted.load()
            if saved is None:
                saved = dict(
                    format=FORMAT,
                    identity=self._identity,
                    limits=self._limits.as_dict(),
                    records=[],
                    artifacts={},
                    pending=None,
                    pendingDeadline=None,
                    coverage=None,
                    lastReceipt=None,
                    confirmedAck=None,
                    rebases=[],
                )
                self._save(saved, replace=False)
            else:
                self._validate_state(saved)
                self._state = saved
            self._authorize()
        except BaseException:
            self.close()
            raise

    @classmethod
    def open(cls, *args: Any, **kwargs: Any) -> "FileStore":
        return cls(*args, **kwargs)

    def _authorize(self) -> None:
        need(not self._closed, "closed")
        need(not self._uncertain, "storage_unknown")
        need(self._access(clone(self._identity)) is not False, "permission")

    @contextmanager
    def _run(self) -> Iterator[None]:
        with self._mutex:
            need(not self._entered, "reentrant")
            self._entered = True
            try:
                self._authorize()
                yield
                self._authorize()
            finally:
                self._entered = False

    def _validate_state(self, state: dict) -> None:
        try:
            self._validate_state_inner(state)
        except (KeyError, TypeError, ValueError, IndexError, AttributeError):
            raise Error("contract") from None

    def _validate_state_inner(self, state: dict) -> None:
        need(
            set(state)
            == {
                "format",
                "identity",
                "limits",
                "records",
                "artifacts",
                "pending",
                "pendingDeadline",
                "coverage",
                "lastReceipt",
                "confirmedAck",
                "rebases",
            }
        )
        need(
            state["format"] == FORMAT
            and equal(state["identity"], self._identity)
            and equal(state["limits"], self._limits.as_dict())
        )
        records, artifacts = state["records"], state["artifacts"]
        need(isinstance(records, list) and isinstance(artifacts, dict) and isinstance(state["rebases"], list))
        need(len(records) <= self._limits.max_records and len(artifacts) <= self._limits.max_artifacts, "capacity")
        previous, ids, seen, total = "0" * 64, set(), set(), 0
        for index, record in enumerate(records):
            verify_record(record)
            need(
                int(record["sequence"]) == index + 1
                and record["predecessorDigest"] == previous
                and record["recordId"] not in ids
            )
            ids.add(record["recordId"])
            need(
                record["target"]["sessionId"] == self._identity["sessionId"]
                and equal(record["target"]["generations"], self._identity["generations"])
            )
            previous = record["recordDigest"]
            total += len(encode(record))
            for ref in references(record):
                saved = artifacts[ref["artifactId"]]
                need(
                    set(saved) == {"reference", "body"}
                    and equal(saved["reference"], ref)
                    and ref["sourceId"] == self._identity["sourceId"]
                )
                body = unbase64(saved["body"])
                need(len(body) == ref["bytes"] and sha256(body) == ref["sha256"])
                if ref["artifactId"] not in seen:
                    total += len(body)
                    seen.add(ref["artifactId"])
            body = unbase64(artifacts[record["payload"]["artifactId"]]["body"])
            need(domain("tansr.sdk2.payload.v1", body) == record["payloadDigest"])
        need(seen == set(artifacts))
        coverage, pending = state["coverage"], state["pending"]
        if coverage is None:
            need(state["lastReceipt"] is None and state["confirmedAck"] is None)
        else:
            verify_coverage(coverage)
            end = int(coverage["throughSequence"])
            need(end <= len(records) and records[end - 1]["recordDigest"] == coverage["headDigest"])
            validate("ArchiveAckRequest", state["confirmedAck"])
            verify_receipt(self._identity, state["confirmedAck"], state["lastReceipt"])
            need(equal(state["confirmedAck"]["coverage"], coverage))
        if pending is not None:
            validate("ArchiveAckRequest", pending)
            verify_coverage(pending["coverage"])
            need(
                all(
                    equal(pending[key], self._identity[key])
                    for key in ("bindingId", "sourceId", "sourceGeneration", "generations")
                )
            )
            need(
                int(pending["coverage"]["throughSequence"]) == len(records)
                and pending["coverage"]["headDigest"] == previous
                and int(pending["coverage"]["fromSequence"])
                == (int(coverage["throughSequence"]) if coverage else 0) + 1
            )
            need(isinstance(state["pendingDeadline"], int) and state["pendingDeadline"] > 0)
            self._verify_ack_objects(pending, records)
        else:
            need(
                state["pendingDeadline"] is None
                and (not records or coverage is not None and int(coverage["throughSequence"]) == len(records))
            )
        requests, unresolved = set(), 0
        for row in state["rebases"]:
            need(set(row) == {"intent", "deadline", "result", "originalReceipt"})
            intent, old = row["intent"], row["intent"]["previous"]
            verify_rebase(intent)
            need(isinstance(row["deadline"], int) and row["deadline"] > 0)
            for request in (intent["request"], old["request"]):
                raw = encode(request)
                need(raw not in requests)
                requests.add(raw)
            need(
                all(
                    equal(old[key], self._identity[key])
                    for key in ("bindingId", "sourceId", "sourceGeneration", "generations")
                )
            )
            end = int(old["coverage"]["throughSequence"])
            need(0 < end <= len(records) and records[end - 1]["recordDigest"] == old["coverage"]["headDigest"])
            self._verify_ack_objects(old, records)
            total += len(encode(row))
            if row["result"] is not None:
                need(row["originalReceipt"] is None)
                verify_rebase_result(intent, row["result"])
                verify_receipt(self._identity, row["result"]["next"], row["result"]["receipt"])
            elif row["originalReceipt"] is not None:
                verify_receipt(self._identity, old, row["originalReceipt"])
            else:
                unresolved += 1
                need(equal(pending, old))
                total += REBASE_RESERVE
            if row["result"] is not None or row["originalReceipt"] is not None:
                need(coverage is not None and int(coverage["throughSequence"]) >= end)
        need(unresolved <= 1)
        # 控制与回执也计入总量，恢复未决项另预留最大服务端结果。
        for key in ("pending", "lastReceipt", "confirmedAck"):
            if state[key] is not None:
                total += len(encode(state[key]))
        need(total <= self._limits.max_stored_bytes, "capacity")

    @staticmethod
    def _verify_ack_objects(ack: dict, records: list) -> None:
        payloads = {}  # type: dict
        attachments = {}  # type: dict
        first, last = int(ack["coverage"]["fromSequence"]), int(ack["coverage"]["throughSequence"])
        for record in records[first - 1 : last]:
            for index, ref in enumerate(references(record)):
                target = payloads if index == 0 else attachments
                target[ref["artifactId"]] = {
                    "artifactId": ref["artifactId"],
                    "sha256": ref["sha256"],
                    "state": "durably-stored",
                }
        need(ack["ackFormat"] == "split-receipts-v1")
        for key, expected in (("payloads", payloads), ("attachments", attachments)):
            need(len(ack[key]) == len(expected) and {row["artifactId"]: row for row in ack[key]} == expected)

    def _save(self, state: dict, replace: bool = True) -> None:
        self._authorize()
        self._validate_state(state)
        try:
            self._encrypted.save(state, replace=replace, hook=self._hook)
        except BaseException:
            self._uncertain = True
            raise
        self._state = state

    def identity(self) -> dict:
        return clone(self._identity)

    def limits(self) -> StoreLimits:
        return self._limits

    def check_access(self) -> None:
        with self._run():
            self._directory.check_access()

    def head(self) -> Optional[dict]:
        with self._run():
            return clone(_head(self._state))

    def coverage(self) -> Optional[dict]:
        with self._run():
            return clone(self._state["coverage"])

    def pending(self) -> Optional[dict]:
        with self._run():
            return clone(self._state["pending"])

    def pending_deadline(self) -> Optional[int]:
        with self._run():
            row = _pending_row(self._state)
            return row["deadline"] if row is not None else self._state["pendingDeadline"]

    def records_by_id(self, ids: list) -> list:
        with self._run():
            need(0 < len(ids) <= 128 and len(set(ids)) == len(ids))
            for identity in ids:
                validate("Id", identity)
            records = {row["recordId"]: row for row in self._state["records"]}
            need(all(identity in records for identity in ids), "not_found")
            return clone([records[identity] for identity in ids])

    def body(self, reference: dict) -> bytes:
        with self._run():
            validate("ArtifactRef", reference)
            need(reference["sourceId"] == self._identity["sourceId"])
            saved = self._state["artifacts"].get(reference["artifactId"])
            need(saved is not None and equal(saved["reference"], reference), "not_found")
            body = unbase64(saved["body"])
            need(len(body) == reference["bytes"] and sha256(body) == reference["sha256"])
            return body

    def receive(
        self, binding: dict, status: dict, page: dict, bodies: Dict[str, bytes], request: dict, deadline_ms: int
    ) -> dict:
        with self._run():
            need(self._state["pending"] is None, "pending_ack")
            need(deadline_ms > now_ms(), "timeout")
            need(equal(identity_from_binding(binding, status), self._identity))
            validate("RequestIdentity", request)
            need(binding["operationEpoch"] is not None and binding["operationEpoch"]["id"] == request["operationEpoch"])
            need(
                "archive-transfer-v1" in binding["acceptedCapabilities"]
                and binding["archiveAckFormat"] == "split-receipts-v1"
            )
            need(status["publishedThroughSequence"] == page["publishedThroughSequence"], "snapshot_changed")
            need(not _reserved(self._state, request), "request_id_conflict")
            head = _head(self._state)
            coverage = status["acknowledgedCoverage"]
            if head is None:
                need(coverage is None)
            else:
                need(
                    coverage is not None
                    and head["sequence"] == coverage["throughSequence"]
                    and head["recordDigest"] == coverage["headDigest"]
                )
            verify_page(binding, head["sequence"] if head else None, page)
            records = page["records"]
            need(bool(records) and records[0]["predecessorDigest"] == (head["recordDigest"] if head else "0" * 64))
            refs = {}  # type: dict
            payloads = {}  # type: dict
            attachments = {}  # type: dict
            total = 0
            for record in records:
                total += len(encode(record))
                for index, ref in enumerate(references(record)):
                    if ref["artifactId"] not in refs:
                        total += ref["bytes"]
                    refs[ref["artifactId"]] = ref
                    target = payloads if index == 0 else attachments
                    target[ref["artifactId"]] = dict(
                        artifactId=ref["artifactId"], sha256=ref["sha256"], state="durably-stored"
                    )
            need(total <= self._limits.max_batch_bytes, "capacity")
            need(set(refs) == set(bodies))
            next_state = clone(self._state)
            for identity, ref in refs.items():
                body = bodies[identity]
                need(isinstance(body, bytes) and len(body) == ref["bytes"] and sha256(body) == ref["sha256"])
                prior = next_state["artifacts"].get(identity)
                need(prior is None or equal(prior["reference"], ref))
                next_state["artifacts"][identity] = dict(
                    reference=clone(ref), body=base64.b64encode(body).decode("ascii")
                )
            ack = dict(
                protocol=PROTOCOL,
                request=clone(request),
                bindingId=self._identity["bindingId"],
                expectedRevision=binding["revision"],
                generations=clone(self._identity["generations"]),
                sourceId=self._identity["sourceId"],
                sourceGeneration=self._identity["sourceGeneration"],
                coverage=dict(
                    fromSequence=records[0]["sequence"],
                    throughSequence=records[-1]["sequence"],
                    headDigest=records[-1]["recordDigest"],
                ),
                attachments=list(attachments.values()),
                ackFormat="split-receipts-v1",
                payloads=list(payloads.values()),
            )
            validate("ArchiveAckRequest", ack)
            encode(ack, binding["limits"]["controlBytes"])
            next_state["records"].extend(clone(records))
            next_state["pending"], next_state["pendingDeadline"] = ack, deadline_ms
            self._save(next_state)
            return clone(ack)

    def confirm(self, receipt: dict) -> None:
        with self._run():
            pending = self._state["pending"]
            if pending is None:
                need(self._state["lastReceipt"] is not None and equal(self._state["lastReceipt"], receipt))
                return
            verify_receipt(self._identity, pending, receipt)
            next_state = clone(self._state)
            next_state.update(
                coverage=clone(pending["coverage"]),
                confirmedAck=clone(pending),
                pending=None,
                pendingDeadline=None,
                lastReceipt=clone(receipt),
            )
            for row in next_state["rebases"]:
                if (
                    row["result"] is None
                    and row["originalReceipt"] is None
                    and equal(row["intent"]["previous"], pending)
                ):
                    row["originalReceipt"] = clone(receipt)
            self._save(next_state)

    def pending_rebase(self) -> Optional[dict]:
        with self._run():
            row = _pending_row(self._state)
            return clone(row["intent"]) if row else None

    def prepare_rebase(self, request: dict, deadline_ms: int) -> dict:
        with self._run():
            validate("RequestIdentity", request)
            need(deadline_ms > now_ms(), "timeout")
            row = _pending_row(self._state)
            if row is not None:
                need(equal(row["intent"]["request"], request), "pending_ack")
                return clone(row["intent"])
            need(self._state["pending"] is not None and not _reserved(self._state, request), "request_id_conflict")
            intent = dict(
                protocol=PROTOCOL,
                bindingId=self._identity["bindingId"],
                previous=clone(self._state["pending"]),
                request=clone(request),
            )
            verify_rebase(intent)
            next_state = clone(self._state)
            next_state["rebases"].append(dict(intent=intent, deadline=deadline_ms, result=None, originalReceipt=None))
            self._save(next_state)
            return clone(intent)

    def confirm_rebase(self, result: dict) -> None:
        with self._run():
            next_state = clone(self._state)
            row = next(
                (item for item in next_state["rebases"] if equal(item["intent"]["request"], result.get("request"))),
                None,
            )
            if row is None:
                raise Error("contract")
            verify_rebase_result(row["intent"], result)
            verify_receipt(self._identity, result["next"], result["receipt"])
            if row["result"] is not None:
                need(equal(row["result"], result))
                return
            need(row["originalReceipt"] is None and equal(next_state["pending"], row["intent"]["previous"]))
            row["result"] = clone(result)
            next_state.update(
                coverage=clone(result["next"]["coverage"]),
                confirmedAck=clone(result["next"]),
                lastReceipt=clone(result["receipt"]),
                pending=None,
                pendingDeadline=None,
            )
            self._save(next_state)

    def rotate_key(self, key: bytes, key_id: str) -> None:
        with self._run():
            validate("Id", key_id)
            try:
                self._encrypted.rotate_key(key, key_id, hook=self._hook)
            except BaseException:
                self._uncertain = True
                raise

    def close(self) -> None:
        with self._mutex:
            need(not self._entered, "reentrant")
            if not self._closed:
                try:
                    if self._encrypted is not None:
                        self._encrypted.close()
                finally:
                    if self._directory is not None:
                        self._directory.close()
                    self._closed = True
                    self._state = {}

    def __enter__(self) -> "FileStore":
        self.check_access()
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()
