"""私有目录中的耐久认领/回执；旧事实永不按时间淘汰。"""
import hashlib
import threading
from typing import Any, Dict, Optional

from .. import canonical, strict_json
from ..errors import Error
from ..lifecycle import CancellationToken
from ._common import cancellation, equal, snapshot, validate_operation, validate_receipt


class Claim:
    def __init__(self, claimed: bool, receipt: Optional[Dict[str, Any]] = None) -> None:
        self.claimed = claimed
        self.receipt = snapshot(receipt)


def journal_key(operation: Dict[str, Any]) -> str:
    validate_operation(operation)
    scope, target = operation["scope"], operation["binding"]["target"]
    # 授权版本和连接租约变化不能给同一个 operationId 开放第二次副作用。
    identity = [scope["applicationScopeId"], scope["endUserId"],
                target["executorId"], operation["operationId"]]
    return hashlib.sha256(canonical.encode(identity)).hexdigest()


class FileJournal:
    def __init__(self, directory: Any) -> None:
        from ..storage import PrivateDirectory
        self._owned = isinstance(directory, (str, bytes)) or hasattr(directory, "__fspath__")
        self._directory = PrivateDirectory(directory, create=True) if self._owned else directory
        self._mutex = threading.RLock()
        self._closed = False

    def _read(self, name: str, operation: Dict[str, Any]) -> Dict[str, Any]:
        try:
            value = strict_json.loads(self._directory.read(name, max_bytes=524288))
            if (not isinstance(value, dict) or set(value) != {"format", "version", "digest", "receipt"}
                    or value["format"] != "tansr-python-execution-journal"
                    or not isinstance(value["version"], int) or isinstance(value["version"], bool)
                    or value["version"] != 1):
                raise Error("outcome_unknown", "corrupt execution journal")
            if value["digest"] != operation["digest"]:
                raise Error("conflict", "journal operation changed")
            if value["receipt"] is not None:
                validate_receipt(operation, value["receipt"])
            return value
        except Error:
            raise
        except (ValueError, OSError, TypeError, KeyError):
            raise Error("outcome_unknown", "execution journal unreadable")

    def claim(self, operation: Dict[str, Any], cancel: Optional[CancellationToken] = None) -> Claim:
        operation = snapshot(operation)
        key = journal_key(operation)
        cancellation(cancel).check()
        with self._mutex:
            if self._closed:
                raise Error("closed")
            with self._directory.lock("executor-journal.lock"):
                cancellation(cancel).check()
                name = key + ".execution.json"
                if self._directory.exists(name):
                    return Claim(False, self._read(name, operation)["receipt"])
                value = {"format": "tansr-python-execution-journal", "version": 1,
                         "digest": operation["digest"], "receipt": None}
                # write 必须完成原子替换及耐久提交，返回前不得运行 handler。
                self._directory.write(name, canonical.encode(value), replace=False)
                return Claim(True)

    def complete(self, operation: Dict[str, Any], receipt: Dict[str, Any],
                 cancel: Optional[CancellationToken] = None) -> None:
        operation, receipt = snapshot(operation), snapshot(receipt)
        validate_receipt(operation, receipt)
        key = journal_key(operation)
        cancellation(cancel).check()
        with self._mutex:
            if self._closed:
                raise Error("closed")
            with self._directory.lock("executor-journal.lock"):
                name = key + ".execution.json"
                if not self._directory.exists(name):
                    raise Error("conflict", "receipt has no durable claim")
                value = self._read(name, operation)
                if value["receipt"] is not None:
                    if not equal(value["receipt"], receipt):
                        raise Error("conflict", "immutable execution receipt")
                    return
                value["receipt"] = receipt
                self._directory.write(name, canonical.encode(value))

    def close(self) -> None:
        with self._mutex:
            if not self._closed:
                self._closed = True
                if self._owned:
                    self._directory.close()

    def __enter__(self) -> "FileJournal":
        if self._closed:
            raise Error("closed")
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()
