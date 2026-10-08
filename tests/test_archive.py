"""真实加密介质与受控 API 的 archive 事务/恢复反例；不代称真实 Serve 验收。"""

import asyncio
import base64
import copy
import datetime
import os
import tempfile
import threading
import time
import unittest
from unittest import mock

from tansr_sdk import canonical, strict_json
from tansr_sdk.api import ApiResponse
from tansr_sdk.archive import (
    ArchiveClient,
    AsyncArchiveClient,
    FileStore,
    SavedIntent,
    StoreLimits,
    identity_from_binding,
)
from tansr_sdk.errors import Error
from tansr_sdk.lifecycle import AsyncBridge, now_ms
from tansr_sdk.storage import PrivateDirectory


def fixture():
    now = datetime.datetime.now(datetime.timezone.utc)
    epoch = dict(
        id="epoch-1",
        issuedAt=(now - datetime.timedelta(seconds=2)).isoformat(),
        expiresAt=(now + datetime.timedelta(seconds=50)).isoformat(),
        state="active",
    )
    limits = dict(
        controlBytes=262144,
        recordBytes=262144,
        pageRecords=128,
        pageBytes=1048576,
        attachmentBytes=33554432,
        chunkBytes=1024,
        materialConcurrent=2,
        materialQueue=16,
        materialCandidates=32,
        materialBytes=1048576,
        materialDeadlineMs=30000,
        pendingRecords=4096,
        pendingBytes=67108864,
        inflightReserveBytes=1048576,
        offlineMs=1000,
        eventRetentionMs=1000,
        eventRetentionFrames=1,
        eventRetentionBytes=1024,
        terminalReceiptRetentionMs=60000,
        epochLifetimeMs=60000,
        materialChunkBytes=65536,
    )
    generations = dict(historyEpoch="h-1", deletionGeneration="0", projectionRevision="1")
    target = dict(sessionId="session-1", generations=generations, sourceSnapshotDigest="0" * 64)
    binding = dict(
        protocol="sdk2-ext-v1",
        bindingId="binding-1",
        scope=dict(applicationScopeId="app-1", endUserId="user-1", authorizationRevision="1"),
        target=target,
        revision="1",
        state="active",
        sourceId="source-1",
        acceptedCapabilities=["archive-transfer-v1", "context-materials-v1"],
        rejectedCapabilities=[],
        availability="legacy-complete",
        operationEpoch=epoch,
        archiveAckFormat="split-receipts-v1",
        limits=limits,
    )
    bodies = {"payload-1": b'{ "n": 1.0, "body" : "synthetic" }\n', "attachment-1": b"raw\x00binary\xff"}
    import hashlib

    refs = {
        name: dict(
            artifactId=name,
            sourceId="source-1",
            bytes=len(raw),
            sha256=hashlib.sha256(raw).hexdigest(),
            mediaType="application/octet-stream",
        )
        for name, raw in bodies.items()
    }
    record = dict(
        recordId="record-1",
        sequence="1",
        target=target,
        turnId="turn-1",
        recordKind="turn",
        turnState="completed",
        predecessorDigest="0" * 64,
        payload=refs["payload-1"],
        attachments=[refs["attachment-1"]],
        payloadDigest=canonical.digest_bytes("tansr.sdk2.payload.v1", bodies["payload-1"]),
    )
    record["recordDigest"] = canonical.digest("tansr.sdk2.record.v1", record)
    status = dict(
        protocol="sdk2-ext-v1",
        bindingId="binding-1",
        revision="1",
        generations=generations,
        sourceId="source-1",
        sourceGeneration="source-generation-1",
        publishedThroughSequence="1",
        acknowledgedCoverage=None,
        releasableThroughSequence=None,
        pendingBytes=sum(len(body) for body in bodies.values()),
        pendingRecords=1,
        sessionPersistence="unchanged",
        state="active",
    )
    page = dict(
        protocol="sdk2-ext-v1",
        bindingId="binding-1",
        generations=generations,
        records=[record],
        nextAfterSequence="1",
        complete=True,
        publishedThroughSequence="1",
    )
    return binding, status, page, bodies


def request(identity="original"):
    return dict(requestId=identity, operationEpoch="epoch-1")


def receipt(identity, ack, revision="2"):
    frame = dict(
        scope=[identity["applicationScopeId"], identity["endUserId"]],
        operation="archive-ack",
        semantic={key: value for key, value in ack.items() if key != "request"},
    )
    return dict(
        protocol="sdk2-ext-v1",
        request=copy.deepcopy(ack["request"]),
        bindingId=ack["bindingId"],
        operation="archive-ack",
        semanticDigest=canonical.digest("tansr.sdk2.operation.v1", frame),
        state="completed",
        revision=revision,
        outcomeRef="ack-result",
    )


class FakeAPI:
    def __init__(self, data):
        self.binding, self.status, self.page, self.bodies = data
        self.identity = identity_from_binding(self.binding, self.status)
        self.calls = []
        self.ack_error = None
        self.rebase_error = None
        self.store = None
        self.ack = None
        self.material_request = None
        self.material_state = "received"

    def default_deadline_ms(self):
        return now_ms() + 30000

    def call(self, operation, options):
        options.cancel.check(options.deadline_ms)
        self.calls.append((operation, copy.copy(options)))
        status = 200
        if operation == "archive.binding.get":
            body = self.binding
        elif operation == "archive.status":
            body = self.status
        elif operation == "archive.records.read":
            body = self.page
        elif operation == "archive.artifact.read":
            identity = options.parameters["targetId"]
            ref = next(
                ref
                for ref in [self.page["records"][0]["payload"]] + self.page["records"][0]["attachments"]
                if ref["artifactId"] == identity
            )
            offset, size = int(options.query["offset"]), int(options.query["maxBytes"])
            raw = self.bodies[identity][offset : offset + size]
            import hashlib

            body = dict(
                protocol="sdk2-ext-v1",
                bindingId="binding-1",
                artifactId=identity,
                sourceId="source-1",
                generations=self.binding["target"]["generations"],
                offset=offset,
                bytes=len(raw),
                totalBytes=ref["bytes"],
                sha256=ref["sha256"],
                chunkSha256=hashlib.sha256(raw).hexdigest(),
                base64=base64.b64encode(raw).decode("ascii"),
            )
        elif operation == "archive.ack.commit":
            if self.store is not None:
                if self.store.pending() != options.body:
                    raise AssertionError("ACK transmitted before durable pending")
            self.ack = copy.deepcopy(options.body)
            if self.ack_error is not None:
                raise self.ack_error
            body = receipt(self.identity, options.body)
        elif operation == "archive.ack.rebase":
            intent = options.body
            if self.store is not None and self.store.pending_rebase() != intent:
                raise AssertionError("rebase transmitted before durable intent")
            if self.rebase_error is not None:
                raise self.rebase_error
            new = copy.deepcopy(intent["previous"])
            new["request"], new["expectedRevision"] = intent["request"], "2"
            body = dict(
                protocol="sdk2-ext-v1",
                bindingId="binding-1",
                previous=intent["previous"],
                request=intent["request"],
                next=new,
                receipt=receipt(self.identity, new, "3"),
            )
        elif operation == "archive.operation.query":
            body = receipt(self.identity, self.ack)
        elif operation == "material.upload.chunk":
            item = options.body
            asked = self.material_request["requestedRecords"][0]
            ref = next(
                row for row in [asked["payload"]] + asked["attachments"] if row["artifactId"] == item["artifactId"]
            )
            body = dict(
                protocol="sdk2-ext-v1",
                bindingId="binding-1",
                materialRequestId="material-1",
                uploadId="upload-" + item["artifactId"],
                artifact=ref,
                state="committed",
                chunkBytes=65536,
                receivedOffsets=[0],
                receivedBytes=ref["bytes"],
                remainingTtlMs=10000,
            )
        elif operation in ("material.response.submit", "material.status"):
            status = 202 if operation.endswith("submit") else 200
            body = dict(
                protocol="sdk2-ext-v1",
                bindingId="binding-1",
                materialRequestId="material-1",
                state=self.material_state,
                revision="2",
                acceptedRecordIds=["record-1"],
            )
        else:
            raise AssertionError(operation)
        return ApiResponse(status=status, body=copy.deepcopy(body), content_type="application/json")


class ArchiveTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="tansr-py-archive-")
        self.directory = os.path.join(self.temp.name, "私有 archive")
        with PrivateDirectory(self.directory, create=True):
            pass
        self.path = os.path.join(self.directory, "archive.bin")
        self.data = fixture()
        self.identity = identity_from_binding(*self.data[:2])
        self.access = True
        self.handles = []

    def tearDown(self):
        for handle in reversed(self.handles):
            handle.close()
        self.temp.cleanup()

    def open(self, **kwargs):
        store = FileStore(self.path, b"k" * 32, "key-1", self.identity, lambda identity: self.access, **kwargs)
        self.handles.append(store)
        return store

    def receive(self, store):
        return store.receive(*self.data, request(), now_ms() + 30000)

    def test_raw_roundtrip_pending_and_scope_receipt(self):
        store = self.open()
        ack = self.receive(store)
        deadline = store.pending_deadline()
        self.assertEqual(store.head()["sequence"], "1")
        self.assertIsNone(store.coverage())
        with open(self.path, "rb") as encrypted:
            self.assertNotIn(self.data[3]["payload-1"], encrypted.read())
        store.close()
        store = self.open()
        self.assertEqual(store.pending(), ack)
        self.assertEqual(store.pending_deadline(), deadline)
        self.assertEqual(store.body(self.data[2]["records"][0]["payload"]), self.data[3]["payload-1"])
        foreign = dict(self.identity, endUserId="other")
        with self.assertRaises(Error):
            store.confirm(receipt(foreign, ack))
        self.assertIsNone(store.coverage())
        store.confirm(receipt(self.identity, ack))
        self.assertEqual(store.coverage(), ack["coverage"])
        self.assertIsNone(store.pending())
        store.close()
        self.assertEqual(self.open().coverage(), ack["coverage"])

    def test_bad_page_or_bytes_never_prepares_ack(self):
        store = self.open()
        for kind in ("chain", "sequence", "record", "payload", "body", "extra", "publication"):
            with self.subTest(kind=kind):
                binding, status, page, bodies = copy.deepcopy(self.data)
                if kind == "chain":
                    page["records"][0]["predecessorDigest"] = "1" * 64
                elif kind == "sequence":
                    page["records"][0]["sequence"] = "2"
                elif kind == "record":
                    page["records"][0]["recordDigest"] = "1" * 64
                elif kind == "payload":
                    row = page["records"][0]
                    row["payloadDigest"] = "1" * 64
                    row["recordDigest"] = canonical.digest(
                        "tansr.sdk2.record.v1", {k: v for k, v in row.items() if k != "recordDigest"}
                    )
                elif kind == "body":
                    bodies["payload-1"] = b"{}"
                elif kind == "extra":
                    bodies["unknown"] = b"x"
                else:
                    status["publishedThroughSequence"] = "2"
                with self.assertRaises(Error):
                    store.receive(binding, status, page, bodies, request(), now_ms() + 30000)
                self.assertIsNone(store.pending())

    def test_revoked_access_and_mutable_return_do_not_authorize(self):
        store = self.open()
        ack = self.receive(store)
        ack["request"]["requestId"] = "replacement"
        self.assertEqual(store.pending()["request"]["requestId"], "original")
        self.access = False
        for action in (store.pending, store.head, store.check_access):
            with self.assertRaises(Error):
                action()

    def test_no_second_process_style_open_and_wrong_key(self):
        store = self.open()
        with self.assertRaises(Error):
            self.open()
        store.close()
        with self.assertRaises(Error):
            FileStore(self.path, b"q" * 32, "key-1", self.identity, lambda identity: True)
        self.assertIsNone(self.open().pending())

    def test_capacity_failure_keeps_old_state(self):
        store = self.open(limits=StoreLimits(max_stored_bytes=2048, max_batch_bytes=500))
        with self.assertRaises(Error):
            self.receive(store)
        self.assertIsNone(store.head())

    def test_write_fault_never_sends_ack_and_reopen_reads_old_state(self):
        fired = [False]

        def hook(stage):
            if fired[0] and stage == "before_replace":
                raise OSError("controlled failure")

        store = self.open(commit_hook=hook)
        fired[0] = True
        api = FakeAPI(self.data)
        api.store = store
        with self.assertRaises(Error):
            ArchiveClient(api).sync_once(store, "original")
        self.assertFalse(any(name == "archive.ack.commit" for name, _ in api.calls))
        store.close()
        self.assertIsNone(self.open().head())

    def test_lost_ack_replays_original_deadline_and_identity(self):
        store = self.open()
        api = FakeAPI(self.data)
        api.store = store
        api.ack_error = Error("network")
        client = ArchiveClient(api)
        with self.assertRaises(Error):
            client.sync_once(store, "original")
        ack, deadline = store.pending(), store.pending_deadline()
        store.close()
        api.store = store = self.open()
        api.ack_error = None
        result = client.sync_once(store, "must-not-replace", deadline_ms=deadline + 99999)
        self.assertTrue(result.recovered)
        self.assertFalse(result.complete)
        self.assertEqual(api.calls[-1][1].body, ack)
        self.assertEqual(api.calls[-1][1].deadline_ms, deadline)

    def test_expired_pending_sends_nothing(self):
        store = self.open()
        self.receive(store)
        api = FakeAPI(self.data)
        with mock.patch("tansr_sdk.lifecycle.now_ms", return_value=store.pending_deadline() + 1):
            with self.assertRaises(Error):
                ArchiveClient(api).sync_once(store, "new", deadline_ms=store.pending_deadline() + 60000)
        self.assertEqual(api.calls, [])

    def test_explicit_rebase_lost_response_and_identity_reuse(self):
        store = self.open()
        original = self.receive(store)
        api = FakeAPI(self.data)
        api.store = store
        api.ack_error = Error(
            "precondition_failed",
            wire_code="precondition_failed",
            detail=dict(domainCode="binding_conflict", reason="if_match_stale"),
        )
        api.rebase_error = Error("network")
        client = ArchiveClient(api)
        with self.assertRaises(Error):
            client.recover_pending(store, "recovery-1")
        intent = store.pending_rebase()
        self.assertEqual(intent["previous"], original)
        with self.assertRaises(Error):
            store.prepare_rebase(request("replacement"), now_ms() + 30000)
        store.close()
        api.store = store = self.open()
        api.rebase_error = None
        result = client.sync_once(store, "new")
        self.assertEqual(result.receipt["request"], request("recovery-1"))
        self.assertEqual(api.calls[-1][1].body, intent)
        self.assertEqual(store.coverage(), original["coverage"])
        self.assertIsNone(store.pending_rebase())

    def test_only_exact_stale_triggers_rebase(self):
        store = self.open()
        self.receive(store)
        api = FakeAPI(self.data)
        client = ArchiveClient(api)
        for code, detail in (
            ("precondition_failed", {}),
            ("conflict", dict(domainCode="binding_conflict", reason="if_match_stale")),
            ("precondition_failed", dict(domainCode="binding_conflict", reason="if_match_missing")),
        ):
            api.calls = []
            api.ack_error = Error(code, wire_code=code, detail=detail)
            with self.assertRaises(Error):
                client.recover_pending(store, "recovery")
            self.assertIsNone(store.pending_rebase())
            self.assertEqual([name for name, _ in api.calls], ["archive.ack.commit"])

    def test_rebase_original_receipt_wins_race(self):
        store = self.open()
        ack = self.receive(store)
        store.prepare_rebase(request("recovery"), now_ms() + 30000)
        api = FakeAPI(self.data)
        api.ack = ack
        api.rebase_error = Error("conflict", detail=dict(domainCode="request_id_conflict"))
        result = ArchiveClient(api).sync_once(store, "unused")
        self.assertEqual(result.receipt["request"], ack["request"])
        self.assertIsNone(store.pending_rebase())

    def test_saved_intent_no_overwrite_and_immutable_body(self):
        store = self.open()
        self.receive(store)
        material = self.material_request()
        with PrivateDirectory(self.directory) as directory:
            deadline = now_ms() + 5000
            intent = SavedIntent.save(directory, "request.json", "material-request", material, deadline)
            with self.assertRaises((Error, FileExistsError)):
                SavedIntent.save(directory, "request.json", "material-request", material, deadline + 5000)
            material["remainingTtlMs"] = 1
            loaded = SavedIntent.load(directory, "request.json")
            self.assertEqual(loaded.deadline_ms, intent.deadline_ms)
            self.assertNotEqual(loaded.body["remainingTtlMs"], 1)

    def material_request(self):
        record = self.data[2]["records"][0]
        return dict(
            protocol="sdk2-ext-v1",
            bindingId="binding-1",
            materialRequestId="material-1",
            target=self.data[0]["target"],
            sourceId="source-1",
            sourceGeneration="source-generation-1",
            requestedRecords=[
                dict(
                    recordId=record["recordId"],
                    digest=record["recordDigest"],
                    payload=record["payload"],
                    attachments=record["attachments"],
                )
            ],
            purpose="context-recall",
            maxBytes=1048576,
            remainingTtlMs=30000,
            chunkBytes=65536,
        )

    def test_material_all_bytes_preflight_and_received_not_consumed(self):
        store = self.open()
        self.receive(store)
        api = FakeAPI(self.data)
        material = api.material_request = self.material_request()
        client = ArchiveClient(api)
        with PrivateDirectory(self.directory) as directory:
            saved = SavedIntent.save(directory, "request.json", "material-request", material, now_ms() + 10000)
            response = client.prepare_materials(store, saved, request("response-1"))
            outgoing = SavedIntent.save(directory, "response.json", "material-response", response, saved.deadline_ms)
            result = client.submit_materials(outgoing)
            self.assertEqual(result["state"], "received")
            self.assertIsNone(store.coverage())
            self.assertIsNotNone(store.pending())
            self.assertEqual(api.calls[-1][1].deadline_ms, saved.deadline_ms)
            self.assertEqual(client.material_status("binding-1", "material-1")["state"], "received")

    def test_material_bad_digest_capacity_or_expiry_zero_uploads(self):
        store = self.open()
        self.receive(store)
        api = FakeAPI(self.data)
        client = ArchiveClient(api)
        for mode in ("digest", "bytes", "chunks", "deadline"):
            body = copy.deepcopy(self.material_request())
            deadline = now_ms() + 30000
            if mode == "digest":
                body["requestedRecords"][0]["digest"] = "1" * 64
            elif mode == "bytes":
                body["maxBytes"] = 1
            elif mode == "chunks":
                body["chunkBytes"] = 1
            else:
                deadline = now_ms() - 1
            with self.assertRaises(Error):
                client.prepare_materials_before(store, body, request("response"), deadline)
            self.assertEqual(api.calls, [])

    def test_async_sync_uses_same_engine(self):
        store = self.open()
        api = FakeAPI(self.data)
        api.store = store

        async def run():
            async with AsyncArchiveClient(api) as client:
                result = await client.sync_once(store, "async-ack")
                self.assertEqual(result.records, 1)
                self.assertEqual(store.coverage()["throughSequence"], "1")

        asyncio.run(run())

    def test_wire_numeric_limits_and_cold_deadline_are_native_transport_ints(self):
        api = FakeAPI(self.data)
        client = ArchiveClient(api)
        parsed_binding = strict_json.loads(strict_json.dumps(self.data[0]))
        client.records(parsed_binding)
        self.assertIs(type(api.calls[-1][1].max_response_bytes), int)
        store = self.open()
        self.receive(store)
        store.close()
        store = self.open()
        client.sync_once(store, "unused")
        self.assertIs(type(api.calls[-1][1].deadline_ms), int)

    def test_rebase_capacity_and_epoch_guards_preserve_pending(self):
        store = self.open(limits=StoreLimits(max_stored_bytes=65536, max_batch_bytes=8192))
        ack = self.receive(store)
        with self.assertRaises(Error):
            store.prepare_rebase(request("recovery"), now_ms() + 30000)
        self.assertEqual(store.pending(), ack)
        self.assertIsNone(store.pending_rebase())
        api = FakeAPI(copy.deepcopy(self.data))
        api.binding["operationEpoch"]["id"] = "epoch-replaced"
        api.ack_error = Error(
            "precondition_failed", detail=dict(domainCode="binding_conflict", reason="if_match_stale")
        )
        with self.assertRaises(Error):
            ArchiveClient(api).recover_pending(store, "recovery")
        self.assertIsNone(store.pending_rebase())
        self.assertFalse(any(name == "archive.ack.rebase" for name, _ in api.calls))

    def test_injected_store_cannot_replace_ack_or_bypass_payload_domain(self):
        store = self.open()
        bad = copy.deepcopy(self.data)
        record = bad[2]["records"][0]
        record["payloadDigest"] = "f" * 64
        record["recordDigest"] = canonical.digest(
            "tansr.sdk2.record.v1", {key: value for key, value in record.items() if key != "recordDigest"}
        )
        with mock.patch.object(store, "receive") as receive:
            with self.assertRaises(Error):
                ArchiveClient(FakeAPI(bad)).sync_once(store, "bad-payload")
            receive.assert_not_called()
        api = FakeAPI(self.data)
        real_receive = store.receive

        def corrupt(*args):
            ack = real_receive(*args)
            ack["request"]["requestId"] = "changed"
            return ack

        with mock.patch.object(store, "receive", side_effect=corrupt):
            with self.assertRaises(Error):
                ArchiveClient(api).sync_once(store, "original")
        self.assertFalse(any(name == "archive.ack.commit" for name, _ in api.calls))

    def test_cold_reopen_validates_confirmed_scope_digest(self):
        store = self.open()
        ack = self.receive(store)
        store.confirm(receipt(self.identity, ack))
        state = copy.deepcopy(store._state)
        state["lastReceipt"]["semanticDigest"] = "f" * 64
        store._encrypted.save(state)
        store.close()
        with self.assertRaises(Error):
            self.open()

    def test_async_queue_snapshots_ack_and_expires_without_sending(self):
        store = self.open()
        ack = self.receive(store)
        api = FakeAPI(self.data)
        release, started = threading.Event(), threading.Event()

        def blocked():
            started.set()
            release.wait(10)

        async def run():
            bridge = AsyncBridge(workers=1, max_pending=3)
            client = AsyncArchiveClient(api, bridge)
            first = asyncio.ensure_future(bridge.run(blocked))
            while not started.is_set():
                await asyncio.sleep(0.005)
            snapshot = copy.deepcopy(ack)
            pending = asyncio.ensure_future(client.acknowledge(ack))
            await asyncio.sleep(0.01)
            ack["request"]["requestId"] = "caller-mutated"
            expired = asyncio.ensure_future(client.acknowledge(snapshot, deadline_ms=now_ms() + 20))
            with self.assertRaises(Error) as caught:
                await expired
            self.assertEqual(caught.exception.code, "timeout")
            self.assertEqual(api.calls, [])
            release.set()
            await first
            result = await pending
            self.assertEqual(result["request"], snapshot["request"])
            self.assertEqual(len(api.calls), 1)
            self.assertTrue(await client.aclose())
            self.assertTrue(await bridge.aclose())

        try:
            asyncio.run(run())
        finally:
            release.set()

    def test_async_cancel_keeps_real_pending_transaction(self):
        entered, release = threading.Event(), threading.Event()
        pause = [False]

        def hook(stage):
            if pause[0] and stage == "before_replace":
                entered.set()
                if not release.wait(10):
                    raise RuntimeError("test timed out")

        store = self.open(commit_hook=hook)
        pause[0] = True
        api = FakeAPI(self.data)

        async def run():
            client = AsyncArchiveClient(api)
            task = asyncio.ensure_future(client.sync_once(store, "async-pending"))
            while not entered.is_set():
                await asyncio.sleep(0.005)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertFalse(await client.aclose(timeout=0.01))
            release.set()
            self.assertTrue(await client.aclose(timeout=10))

        try:
            asyncio.run(run())
        finally:
            release.set()
        self.assertEqual(store.pending()["request"]["requestId"], "async-pending")
        self.assertFalse(any(name == "archive.ack.commit" for name, _ in api.calls))

    def test_async_long_archive_streams_do_not_starve_control(self):
        # 合成同步入口仅隔离调度边界；实际协议与介质仍由本文件及实接驱动验证。
        entered, release = threading.Event(), threading.Event()
        controls = []

        class Stream:
            def __iter__(self):
                return self

            def __next__(self):
                entered.set()
                release.wait(3)
                raise StopIteration

            def close(self):
                pass  # 非合作读尚未退出时，门面不得假称静止。

        class Peer(ArchiveClient):
            def __init__(self):
                pass

            def default_deadline_ms(self):
                return now_ms() + 5000

            def events(self, binding, cursor=None, **options):
                return Stream()

            def binding(self, identity, **options):
                controls.append(identity)
                return {"bindingId": identity}

        async def run():
            bridge = AsyncBridge(workers=1, max_pending=8)
            client = AsyncArchiveClient(Peer(), bridge)
            tasks = []
            try:
                streams = [await client.events({}) for _ in range(4)]
                tasks = [asyncio.ensure_future(stream.__anext__()) for stream in streams]
                limit = time.monotonic() + 1
                while not entered.is_set():
                    self.assertLess(time.monotonic(), limit)
                    await asyncio.sleep(0.001)
                response = await client.binding("short-control", deadline_ms=now_ms() + 500)
                self.assertEqual(response, {"bindingId": "short-control"})
                self.assertEqual(controls, ["short-control"])
                self.assertTrue(all(not task.done() for task in tasks))
                self.assertFalse(await client.aclose(timeout=0.01))
            finally:
                release.set()
                try:
                    await asyncio.gather(*tasks, return_exceptions=True)
                    self.assertTrue(await client.aclose(timeout=1))
                    self.assertEqual(await bridge.run(lambda: "host-still-usable"), "host-still-usable")
                finally:
                    self.assertTrue(await bridge.aclose(timeout=1))
        asyncio.run(run())

    def test_async_slow_archive_transactions_do_not_starve_control(self):
        entered, release = threading.Event(), threading.Event()
        controls = []

        class Peer(ArchiveClient):
            def __init__(self):
                pass

            def default_deadline_ms(self):
                return now_ms() + 5000

            def sync_once(self, store, request_id, **options):
                entered.set()
                release.wait(3)
                return request_id

            def binding(self, identity, **options):
                controls.append(identity)
                return {"bindingId": identity}

        async def run():
            bridge = AsyncBridge(workers=1, max_pending=8)
            client = AsyncArchiveClient(Peer(), bridge)
            tasks = [asyncio.ensure_future(client.sync_once(object(), "write-" + str(index)))
                     for index in range(4)]
            try:
                limit = time.monotonic() + 1
                while not entered.is_set():
                    self.assertLess(time.monotonic(), limit)
                    await asyncio.sleep(0.001)
                response = await client.binding("short-control", deadline_ms=now_ms() + 500)
                self.assertEqual(response, {"bindingId": "short-control"})
                self.assertEqual(controls, ["short-control"])
                self.assertTrue(all(not task.done() for task in tasks))
                self.assertFalse(await client.aclose(timeout=0.01))
            finally:
                release.set()
                try:
                    await asyncio.gather(*tasks, return_exceptions=True)
                    self.assertTrue(await client.aclose(timeout=1))
                    self.assertEqual(await bridge.run(lambda: "host-still-usable"), "host-still-usable")
                finally:
                    self.assertTrue(await bridge.aclose(timeout=1))
        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
