"""单消费者事件流与当前轮状态，不以202、EOF或旧轮终态判成功。"""

import threading
from dataclasses import dataclass
from typing import Any, Dict, Optional

from ..api import CallOptions
from ..api.schema import validate_wire
from ..errors import Error
from ..lifecycle import CancellationToken
from ..strict_json import loads, snapshot
from ._validation import contract, finite_number, parse_sequence, safe_integer, text
from .types import Outcome


@dataclass(frozen=True)
class SessionEvent:
    envelope: Dict[str, Any]

    @property
    def kind(self) -> str:
        kind = self.envelope.get("type")
        return kind if isinstance(kind, str) else ""

    @property
    def raw(self) -> Dict[str, Any]:
        return self.envelope["raw"]

    def turn_outcome(self) -> Optional[Outcome]:
        terminal = self.envelope.get("terminalStatus")
        status = None
        if self.kind == "turn.completed" and terminal == "completed":
            status = "completed"
        elif self.kind == "turn.aborted" and terminal == "aborted":
            status = "aborted"
        elif self.kind == "turn.error" and terminal == "aborted" and self.raw.get("recoverable") is False:
            status = "failed"
        elif self.kind == "session.ended" and terminal == "completed":
            status = "session_ended"
        elif self.kind.startswith("turn.") and terminal == "unknown":
            status = "unknown"
        if status is None:
            return None
        turn = self.raw.get("turnId")
        reason = self.raw.get("reason")
        return Outcome(status, turn if isinstance(turn, str) else None, reason if isinstance(reason, str) else None)


class TurnTracker:
    """after_seq是宿主已处理水位；gap后须恢复可信状态再新建跟踪器。"""

    def __init__(self, after_seq: int) -> None:
        self.after_seq = safe_integer(after_seq)
        self.active_turn_id = None  # type: Optional[str]
        self.needs_reconciliation = False
        self._replaying_prefix = False
        self._finished = False

    @classmethod
    def create(cls, after_seq: int) -> "TurnTracker":
        return cls(after_seq)

    @classmethod
    def resume(cls, after_seq: int, turn_id: str) -> "TurnTracker":
        result = cls(after_seq)
        result.active_turn_id = text(turn_id, nonempty=True, limit=128)
        return result

    @classmethod
    def from_replay(cls, after_seq: int) -> "TurnTracker":
        result = cls(after_seq)
        result._replaying_prefix = True
        return result

    def observe(self, event: SessionEvent) -> Optional[Outcome]:
        if self._finished or self.needs_reconciliation:
            return None
        if event.kind == "server.replay.gap":
            self.needs_reconciliation = True
            return None
        identity = event.envelope.get("eventId")
        try:
            sequence = parse_sequence(identity)
        except Error:
            return None
        turn = event.raw.get("turnId")
        if not isinstance(turn, str) or not turn:
            turn = None
        outcome = event.turn_outcome()
        if sequence <= self.after_seq:
            if self._replaying_prefix:
                if event.kind == "turn.started":
                    self.active_turn_id = turn
                elif outcome is not None and (
                    outcome.status == "session_ended"
                    or (self.active_turn_id is not None and turn == self.active_turn_id)
                ):
                    self.active_turn_id = None
            return None
        self._replaying_prefix = False
        if event.kind == "turn.started" and self.active_turn_id is None:
            self.active_turn_id = turn
            return None
        if outcome is None:
            return None
        if outcome.status == "session_ended":
            self._finished = True
            return outcome
        if self.active_turn_id is None or turn != self.active_turn_id:
            return None
        self._finished = True
        return outcome


class SessionEventStream:
    """显式上下文管理、单读者、无自动重连，close只关闭本地观察。"""

    def __init__(
        self,
        inner: Any,
        session_id: str,
        last_event_id: Optional[str],
        cancel: CancellationToken,
        unregister: list,
        deadline_ms: Optional[int],
    ) -> None:
        self._inner = inner
        self._session_id = session_id
        self._last_event_id = last_event_id
        self._last_seq = parse_sequence(last_event_id) if last_event_id is not None else None
        self._cancel = cancel
        self._unregister = unregister
        self._deadline_ms = deadline_ms
        self._reader = threading.Lock()
        self._state = threading.Lock()
        self._closed = False

    @classmethod
    def open(
        cls,
        session: Any,
        last_event_id: Optional[str] = None,
        *,
        cancel: Optional[CancellationToken] = None,
        deadline_ms: Optional[int] = None,
    ) -> "SessionEventStream":
        if last_event_id == "":
            last_event_id = None
        if last_event_id is not None:
            parse_sequence(last_event_id)
        local = CancellationToken()
        unregister = []
        try:
            for token in (session.client.cancel, cancel):
                if token is not None:
                    unregister.append(token.register(local.cancel))
            local.check(deadline_ms)
            inner = session.api.events(
                "session.events.observe",
                CallOptions(
                    parameters={"id": session.id},
                    last_event_id=last_event_id,
                    cancel=local,
                    deadline_ms=deadline_ms,
                ),
            )
            return cls(inner, session.id, last_event_id, local, unregister, deadline_ms)
        except BaseException:
            local.cancel()
            for remove in unregister:
                remove()
            raise

    @property
    def last_event_id(self) -> Optional[str]:
        return self._last_event_id

    @property
    def closed(self) -> bool:
        with self._state:
            return self._closed

    def close(self) -> None:
        with self._state:
            if self._closed:
                return
            self._closed = True
        self._cancel.cancel()
        try:
            self._inner.close()
        finally:
            for remove in self._unregister:
                remove()
            self._unregister = []

    shutdown = close

    def __enter__(self) -> "SessionEventStream":
        if self.closed:
            raise Error("closed", "session event stream is closed")
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.close()

    def __iter__(self) -> "SessionEventStream":
        return self

    def __next__(self) -> SessionEvent:
        event = self.next()
        if event is None:
            raise StopIteration
        return event

    def next(self, cancel: Optional[CancellationToken] = None) -> Optional[SessionEvent]:
        if not self._reader.acquire(False):
            raise Error("reentrant", "session event stream has one consumer")
        unregister = cancel.register(self._cancel.cancel) if cancel is not None else None
        try:
            if self.closed:
                return None
            while True:
                self._cancel.check(self._deadline_ms)
                try:
                    frame = next(self._inner)
                except StopIteration:
                    self.close()
                    return None
                envelope = loads(frame.data)
                validate_wire("unified-v1", "EventEnvelope", envelope)
                frame_id = frame.id if frame.id else None
                if envelope["eventId"] != frame_id or envelope["cursorSet"]["eventCursor"] != frame_id:
                    raise contract("event frame and envelope cursor differ")
                if frame.event and frame.event != envelope.get("type"):
                    raise contract("event frame and envelope type differ")
                event = self._decode(envelope)
                self._cancel.check(self._deadline_ms)
                if event is not None:
                    # 此处是交付水位，不是宿主持久化处理水位。
                    if event.envelope.get("eventId") is not None:
                        self._last_event_id = event.envelope["eventId"]
                        self._last_seq = parse_sequence(self._last_event_id)
                    return event
        except BaseException:
            self.close()
            raise
        finally:
            if unregister is not None:
                unregister()
            self._reader.release()

    def _decode(self, envelope: Dict[str, Any]) -> Optional[SessionEvent]:
        if envelope.get("domain") != "session" or not isinstance(envelope.get("raw"), dict):
            raise contract("session event has no matching object envelope")
        kind = envelope.get("type") or ""
        raw = envelope["raw"]
        cursor = envelope["cursorSet"]["eventCursor"]
        if kind == "server.replay.gap":
            if (
                envelope.get("eventId") is not None
                or cursor is not None
                or raw.get("sessionId") != self._session_id
                or raw.get("type") != kind
            ):
                raise contract("replay gap changed identity or cursor")
            if raw.get("reason") == "ahead_of_log":
                self._last_seq = None
                self._last_event_id = None
            return SessionEvent(snapshot(envelope))
        sequence = parse_sequence(envelope.get("eventId"))
        if kind.startswith("server."):
            try:
                finite_number(raw.get("ts"))
            except Error:
                raise contract("control event has no finite timestamp") from None
            if "sessionId" in raw and raw["sessionId"] != self._session_id:
                raise contract("control event changed session identity")
        else:
            raw_seq = safe_integer(raw.get("seq"), response=True)
            if raw.get("type") != kind or raw.get("sessionId") != self._session_id or raw_seq != sequence:
                raise contract("kernel event identity differs from its session stream")
        if self._last_seq is not None and sequence <= self._last_seq:
            return None
        return SessionEvent(snapshot(envelope))
