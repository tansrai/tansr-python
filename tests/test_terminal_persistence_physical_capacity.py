"""配置内可预知的快照容量必须在接纳票据前预留；原票据不被逐出。"""
import hashlib
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from tansr_sdk import Error
from test_terminal_persistence import owner, plan, store_at
from tansr_sdk.terminal_persistence._state import Engine
from tansr_sdk.terminal_persistence._wire import validate


def body(blocks, seed):
    return b"".join(hashlib.sha256(bytes((seed, i))).digest() * 384 for i in range(blocks))


def test_physical_capacity_rejects_begin_without_writing(tmp_path):
    prepared = plan(body(100, 1), transfer="too-large")
    path = tmp_path / "persistence/data.enc"
    with store_at(tmp_path, max_snapshot_bytes=1 << 20) as store:
        before = path.read_bytes()
        with pytest.raises(Error, match="capacity_exceeded"):
            store.execute(prepared[0], owner())
        assert path.read_bytes() == before
        assert store.execute(dict(prepared[2], action="query"), owner())["transfer"]["status"] == "unknown"


def test_physical_reservations_two_concurrent_tickets_reopen_and_finish(tmp_path):
    plans = [plan(body(22, i + 1), transfer=str(i)) for i in range(2)]
    path = tmp_path / "persistence/data.enc"
    with store_at(tmp_path, max_snapshot_bytes=1 << 20) as store:
        start = threading.Barrier(2)
        def begin(prepared):
            start.wait()
            return store.execute(prepared[0], owner())
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(begin, plans))
        assert all(result["transfer"]["status"] == "staging" for result in results)
        store.execute(plans[0][1][0], owner())
        pending = store.execute(dict(plans[0][2], action="query"), owner())
    with store_at(tmp_path, mode="reopen", max_snapshot_bytes=1 << 20) as store:
        assert store.execute(dict(plans[0][2], action="query"), owner()) == pending
        third = plan(body(22, 3), transfer="third")
        before = path.read_bytes()
        with pytest.raises(Error, match="capacity_exceeded"):
            store.execute(third[0], owner())
        assert path.read_bytes() == before
        for prepared in plans:
            for put in prepared[1]:
                store.execute(put, owner())
        root = store.execute(plans[0][2], owner())["transfer"]["result"]
        with pytest.raises(Error, match="revision_conflict"):
            store.execute(plans[1][2], owner())
        assert store.execute(dict(plans[0][2], action="query"), owner())["transfer"]["result"] == root
        assert store.execute(dict(plans[1][2], action="query"), owner())["transfer"]["status"] == "rejected"


def test_previously_admitted_ticket_remains_queryable_and_continuable(tmp_path):
    prepared = plan(body(100, 9), transfer="legacy-pending")
    with store_at(tmp_path, max_snapshot_bytes=1 << 20) as store:
        # Reproduce the previous release's engine/save admission; format is unchanged.
        with store._run():
            state = store._unpack(store._encrypted.load())
            Engine(state, validate, store._measurements).execute(prepared[0], owner())
            store._save(state)
        before = store.execute(dict(prepared[2], action="query"), owner())
    with store_at(tmp_path, mode="reopen", max_snapshot_bytes=1 << 20) as store:
        assert store.execute(dict(prepared[2], action="query"), owner()) == before
        store.execute(prepared[0], owner())
        store.execute(prepared[1][0], owner())
        assert store.execute(dict(prepared[2], action="query"), owner())["transfer"]["progress"]["receivedBytes"] > 0
