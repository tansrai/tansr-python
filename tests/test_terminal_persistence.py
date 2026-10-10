"""新 profile 真实加密快照、共同根/永久索引、原键恢复与引用回收。"""
import base64
import hashlib
import json
from pathlib import Path

import pytest

from tansr_sdk import Error, canonical
from tansr_sdk.terminal_persistence import FileStore, Host, TOOL_NAME, DEFINITION_DIGEST
from tansr_sdk.terminal_persistence._state import DEFAULT_LIMITS, Engine, Measurements, digest, hash_value, initial
from tansr_sdk.terminal_persistence._wire import validate
from test_memory_publication import owner
from test_executor import SCOPE, operation
from tansr_sdk.executor import operation_digest, validate_operation

IDENTITY = dict(applicationScopeId=SCOPE["applicationScopeId"], endUserId=SCOPE["endUserId"],
                sourceId="source", sourceGeneration="1", domainKey="memory")
KEY = b"n" * 32


def request(action, **fields):
    return dict(contract="terminal-persistence-v1", action=action,
                **{key: IDENTITY[key] for key in ("sourceId", "sourceGeneration", "domainKey")}, **fields)


def store_at(tmp_path, **options):
    values = dict(mode="create")
    values.update(options)
    return FileStore(str(tmp_path / "persistence" / "data.enc"), KEY, "persistence-key", IDENTITY,
                     values.pop("read_context", lambda: SCOPE), **values)


def plan(body=b"private body", entries=(), transfer="transfer", root=None, added=None):
    objects = {}
    def obj(kind, raw):
        sha = digest(raw)
        objects[(kind, sha)] = raw
        return dict(sha256=sha, byteLength=len(raw))
    refs = [obj("body-block", body[i:i + 12288]) for i in range(0, len(body), 12288)]
    body_pages = [obj("body-page", canonical.encode(dict(version=1, kind="body-page", index=i // 64,
        refs=refs[i:i + 64])))["sha256"] for i in range(0, len(refs), 64)]
    values = []
    for primary, secondary, raw in entries:
        values.append(dict(primaryKey=primary, secondaryKey=secondary, value=obj("receipt-value", raw)))
    values.sort(key=lambda entry: entry["primaryKey"])
    index_pages = [obj("index-page", canonical.encode(dict(version=1, kind="index-page", index=i // 32,
        entries=values[i:i + 32])))["sha256"] for i in range(0, len(values), 32)]
    expected = None if root is None else dict(commitRoot=root["commitRoot"], generation=root["generation"],
        bodyEtag=root["body"]["sha256"], indexRoot=root["index"]["root"], indexCount=root["index"]["count"])
    begin = request("begin", transferId=transfer, expected=expected,
        body=dict(byteLength=len(body), sha256=digest(body), blockCount=len(refs), pageHashes=body_pages),
        index=dict(entryCount=len(values), addedCount=len(values) if added is None else added, pageHashes=index_pages),
        declared=dict(objects=len(objects), bytes=sum(len(raw) for raw in objects.values())))
    begin["intentSha256"] = hash_value(begin)
    puts = [request("put", transferId=transfer, intentSha256=begin["intentSha256"], kind=kind,
        sha256=sha, byteLength=len(raw), base64=base64.b64encode(raw).decode())
        for (kind, sha), raw in sorted(objects.items(), key=lambda item: (0 if item[0][0].endswith("page") else 1, item[0]))]
    commit = request("commit", transferId=transfer, intentSha256=begin["intentSha256"])
    return begin, puts, commit


def publish(store, prepared):
    begin, puts, commit = prepared
    store.execute(begin, owner())
    for put in puts:
        store.execute(put, owner())
    return store.execute(commit, owner())["transfer"]["result"]


def test_frozen_schema_positive_negative_and_digest():
    directory = Path(__file__).resolve().parents[1] / "contract-persistence"
    golden = json.loads((directory / "terminal-persistence-v1.golden.json").read_bytes())
    assert hashlib.sha256((directory / "terminal-persistence-v1.schema.json").read_bytes()).hexdigest() == golden["schemaSha256"]
    assert len(golden["positive"]) == 38 and len(golden["negative"]) == 32
    for vector in golden["positive"]:
        validate(vector["definition"], vector["value"])
    for vector in golden["negative"]:
        with pytest.raises(Error):
            validate(vector["definition"], vector["value"])


def test_root_lookup_range_reopen_and_permanent_original_result(tmp_path):
    pkey, skey = digest(b"primary"), digest(b"secondary")
    first = plan(entries=[(pkey, skey, b"opaque receipt")])
    with store_at(tmp_path) as store:
        root = publish(store, first)
        assert store.execute(first[2], owner())["transfer"]["result"] == root
        for kind, key in (("primary", pkey), ("secondary", skey)):
            got = store.execute(request("lookup", commitRoot=root["commitRoot"], key=dict(kind=kind, digest=key)), owner())
            assert base64.b64decode(got["entry"]["base64"]) == b"opaque receipt"
        result = store.execute(request("read", part="body", commitRoot=root["commitRoot"], offset=2, length=3), owner())
        assert base64.b64decode(result["base64"]) == b"iva"
    with store_at(tmp_path, mode="reopen") as store:
        newer = publish(store, plan(b"new body", transfer="next", root=root))
        assert newer != root
        assert store.execute(dict(first[2], action="query"), owner())["transfer"]["result"] == root
        with pytest.raises(Error, match="revision_conflict"):
            store.execute(request("read", part="body", commitRoot=root["commitRoot"], offset=0, length=1), owner())
        assert store.capacity()["used"]["objects"] == 3
    blob = (tmp_path / "persistence/data.enc").read_bytes()
    assert b"private body" not in blob and b"opaque receipt" not in blob


@pytest.mark.parametrize("stage", ["before_replace", "replaced", "directory_synced"])
def test_unknown_commit_requires_original_query_and_never_recreates(tmp_path, stage):
    prepared = plan()
    with store_at(tmp_path) as store:
        store.execute(prepared[0], owner())
        for put in prepared[1]:
            store.execute(put, owner())
        def fault(current):
            if current == stage:
                raise OSError("injected original commit loss")
        store._hook = fault
        with pytest.raises(Error, match="storage_unknown"):
            store.execute(prepared[2], owner())
        with pytest.raises(Error, match="storage_unknown"):
            store.execute(request("head"), owner())
    with store_at(tmp_path, mode="reopen") as store:
        got = store.execute(dict(prepared[2], action="query"), owner())["transfer"]
        assert got["status"] == ("staging" if stage == "before_replace" else "committed")
        assert store.capacity()["used"]["transferFacts"] == 1


def test_protected_base_survives_other_commit_and_stale_transfer_rejects(tmp_path):
    with store_at(tmp_path) as store:
        root = publish(store, plan(b"old"))
        waiting = plan(b"old", transfer="waiting", root=root)
        store.execute(waiting[0], owner())
        newer = publish(store, plan(b"new", transfer="winner", root=root))
        assert store.capacity()["used"]["objects"] == 4
        with pytest.raises(Error, match="revision_conflict"):
            store.execute(waiting[2], owner())
        assert store.capacity()["used"]["objects"] == 2
        assert store.execute(request("head"), owner())["root"] == newer
        assert store.execute(dict(waiting[2], action="query"), owner())["transfer"]["status"] == "rejected"


def test_owner_recovery_is_query_only_and_reserved_host_checks_original_profile(tmp_path):
    prepared = plan()
    other = owner()
    other["binding"]["target"]["connectionRevision"] = "2"
    with store_at(tmp_path, authorize_recovery=lambda proof: True) as store:
        store.execute(prepared[0], owner())
        assert store.execute(dict(prepared[2], action="query"), other)["transfer"]["status"] == "staging"
        with pytest.raises(Error, match="request_conflict"):
            store.execute(prepared[2], other)
        adapter = Host(store)
        assert adapter.registration() == dict(name=TOOL_NAME, definitionDigest=DEFINITION_DIGEST)
        op = operation()
        op["toolName"] = "MemoryPublication"
        op["request"]["args"] = dict(name=TOOL_NAME, definitionDigest=DEFINITION_DIGEST,
                                      argsJson=json.dumps(request("head")))
        op["digest"] = operation_digest(op)
        validate_operation(op)
        op["request"]["args"]["definitionDigest"] = "0" * 64
        op["digest"] = operation_digest(op)
        with pytest.raises(Error, match="conflict"):
            validate_operation(op)


def test_repeated_generations_reclaim_body_keep_index_and_historical_tickets():
    state = initial(IDENTITY, DEFAULT_LIMITS)
    measurements = Measurements()
    who = owner()
    root = None
    for i in range(513):
        entry = (digest(str(i).encode()), digest(("secondary" + str(i)).encode()), str(i).encode())
        prepared = plan(("body" + str(i)).encode(), [entry], str(i), root)
        for req in (prepared[0], *prepared[1], prepared[2]):
            result = Engine(state, validate, measurements).execute(req, who)
        root = result["transfer"]["result"]
        assert len(state["objects"]) == i + 3
    Engine(state, validate).audit(IDENTITY, DEFAULT_LIMITS)
    assert root["index"]["count"] == "513" and len(state["transfers"]) == 513
    assert Engine(state, validate).check_capacity()["used"]["retainedBytes"] < 2 << 20


def test_four_mib_opaque_body_one_block_delta_and_no_change_reuse(tmp_path):
    body = bytes([255]) * (4 << 20)
    with store_at(tmp_path) as store:
        root = publish(store, plan(body))
        changed = body[:12288] + bytes([254]) * 12288 + body[24576:]
        prepared = plan(changed, transfer="changed", root=root)
        started = store.execute(prepared[0], owner())["transfer"]["progress"]
        assert started["receivedBytes"] == 0
        page = next(put for put in prepared[1] if put["kind"] == "body-page" and
                    put["sha256"] != root["body"]["pageHashes"][0] and
                    put["sha256"] == prepared[0]["body"]["pageHashes"][0])
        store.execute(page, owner())
        block = next(put for put in prepared[1] if put["kind"] == "body-block" and put["sha256"] == digest(bytes([254]) * 12288))
        store.execute(block, owner())
        committed = store.execute(prepared[2], owner())["transfer"]
        assert committed["status"] == "committed" and committed["result"]["body"]["sha256"] == digest(changed)
        root = committed["result"]
        same_plan = plan(changed, transfer="same", root=root)
        assert store.execute(same_plan[0], owner())["transfer"]["progress"]["receivedBytes"] == 0
        newest = store.execute(same_plan[2], owner())["transfer"]["result"]
        assert newest["body"] == root["body"]
    with store_at(tmp_path, mode="reopen") as store:
        result = store.execute(request("read", part="body", commitRoot=newest["commitRoot"], offset=12288, length=12288), owner())
        assert base64.b64decode(result["base64"]) == bytes([254]) * 12288


def test_capacity_and_physical_key_budget_reserve_before_begin(tmp_path):
    limits = dict(DEFAULT_LIMITS, retainedBytes=280000)
    with store_at(tmp_path, limits=limits) as store:
        first = plan(b"a", transfer="first")
        store.execute(first[0], owner())
        with pytest.raises(Error, match="capacity_exceeded"):
            store.execute(plan(b"b", transfer="second")[0], owner())
        assert store.capacity()["used"]["transferFacts"] == 1
    with store_at(tmp_path, mode="reopen", limits=limits) as store:
        # 原加密容器持有的当前钥剩余写数不足时，不接受另一个 begin。
        store._encrypted._uses = (1 << 20) - 1
        with pytest.raises(Error, match="capacity_exceeded"):
            store.execute(plan(b"b", transfer="third")[0], owner())
        assert store.execute(dict(first[2], action="query"), owner())["transfer"]["status"] == "staging"


@pytest.mark.parametrize("damage", ["wrong-key", "bit", "truncate", "wrong-domain"])
def test_encrypted_reopen_rejects_without_modifying_source(tmp_path, damage):
    with store_at(tmp_path) as store:
        publish(store, plan())
    path = tmp_path / "persistence/data.enc"
    original = path.read_bytes()
    supplied = original
    if damage == "bit":
        supplied = original[:-1] + bytes([original[-1] ^ 1])
    elif damage == "truncate":
        supplied = original[:-15]
    path.write_bytes(supplied)
    identity = dict(IDENTITY, domainKey="wrong") if damage == "wrong-domain" else IDENTITY
    with pytest.raises(Error):
        FileStore(str(path), b"z" * 32 if damage == "wrong-key" else KEY, "persistence-key", identity,
                  lambda: SCOPE, mode="reopen")
    assert path.read_bytes() == supplied


def test_canonical_size_cache_is_exact_bounded_and_not_authority():
    meter = Measurements()
    for i in range(4200):
        value = dict(index=i, owner=["中文", True, None, str(i)], base={"generation":str(i)})
        assert meter.size(value) == len(canonical.encode(value))
        value["owner"][0] = "changed"
        assert meter.size(value) == len(canonical.encode(value))
    assert len(meter._cache) == 4096
    meter.clear()
    assert not meter._cache
