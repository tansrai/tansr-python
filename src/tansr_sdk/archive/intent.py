"""先耐久、后变更：创建与材料请求保存原身份、精确正文和绝对截止。"""

from typing import Any
from .. import strict_json
from ..lifecycle import now_ms
from ._validation import clone, encode, need, validate

_KINDS = {
    "binding-create": "BindingCreateRequest",
    "material-response": "MaterialResponseRequest",
    "material-request": "MaterialRequest",
}
_SAVED = object()


class SavedIntent:
    def __init__(self, kind: str, body: dict, deadline_ms: int, *, _proof: Any = None) -> None:
        need(_proof is _SAVED, "invalid_input")
        self._kind, self._body, self._deadline = kind, clone(body), int(deadline_ms)

    @property
    def kind(self) -> str:
        return self._kind

    @property
    def body(self) -> dict:
        return clone(self._body)

    @property
    def deadline_ms(self) -> int:
        return self._deadline

    @classmethod
    def save(cls, directory: Any, filename: str, kind: str, body: dict, deadline_ms: int) -> "SavedIntent":
        need(kind in _KINDS and isinstance(deadline_ms, int), "invalid_input")
        body = clone(body)
        validate(_KINDS[kind], body)
        if kind == "material-request":
            deadline_ms = min(deadline_ms, now_ms() + body["remainingTtlMs"])
        need(deadline_ms > now_ms(), "timeout")
        value = dict(format="tansr-python-intent-v1", kind=kind, body=body, deadlineMs=deadline_ms)
        with directory.lock(filename + ".intent-lock"):
            directory.write(filename, encode(value), replace=False, deadline_ms=deadline_ms)
        return cls(kind, body, deadline_ms, _proof=_SAVED)

    @classmethod
    def load(cls, directory: Any, filename: str) -> "SavedIntent":
        saved = strict_json.loads(directory.read(filename, max_bytes=2 << 20))
        need(isinstance(saved, dict) and set(saved) == {"format", "kind", "body", "deadlineMs"})
        need(saved["format"] == "tansr-python-intent-v1" and saved["kind"] in _KINDS)
        validate(_KINDS[saved["kind"]], saved["body"])
        need(isinstance(saved["deadlineMs"], int) and saved["deadlineMs"] > 0)
        return cls(saved["kind"], saved["body"], saved["deadlineMs"], _proof=_SAVED)
