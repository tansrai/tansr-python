"""档案公共配置；逻辑容量不等于磁盘配额。"""

from dataclasses import dataclass
from typing import Any, Dict, Optional
from ._validation import need


@dataclass(frozen=True)
class StoreLimits:
    max_records: int = 4096
    max_artifacts: int = 16384
    max_stored_bytes: int = 64 << 20
    max_batch_bytes: int = 8 << 20

    def validate(self) -> None:
        need(
            all(
                isinstance(value, int) and not isinstance(value, bool)
                for value in (self.max_records, self.max_artifacts, self.max_stored_bytes, self.max_batch_bytes)
            ),
            "capacity",
        )
        need(
            0 < self.max_records <= 1000000
            and 0 < self.max_artifacts <= 1000000
            and 0 < self.max_batch_bytes <= self.max_stored_bytes <= 64 << 20,
            "capacity",
        )

    def as_dict(self) -> Dict[str, int]:
        return dict(
            maxRecords=self.max_records,
            maxArtifacts=self.max_artifacts,
            maxStoredBytes=self.max_stored_bytes,
            maxBatchBytes=self.max_batch_bytes,
        )


@dataclass(frozen=True)
class SyncResult:
    records: int = 0
    complete: bool = False
    recovered: bool = False
    receipt: Optional[Dict[str, Any]] = None


class ArchiveStore:
    """宿主适配器须兑现原子耐久、当前授权、原 ACK/截止及独立覆盖语义。

    receive 返回前须保存整页及全部原字节和 ACK；confirm 只接受精确 completed
    回执。取消不得回滚已提交事实。历史身份只用于匹配，不能授予当前访问权。
    """

    def identity(self) -> Dict[str, Any]:
        raise NotImplementedError

    def limits(self) -> StoreLimits:
        raise NotImplementedError

    def check_access(self) -> None:
        raise NotImplementedError

    def head(self) -> Optional[Dict[str, Any]]:
        raise NotImplementedError

    def coverage(self) -> Optional[Dict[str, Any]]:
        raise NotImplementedError

    def pending(self) -> Optional[Dict[str, Any]]:
        raise NotImplementedError

    def pending_deadline(self) -> Optional[int]:
        raise NotImplementedError

    def receive(
        self, binding: dict, status: dict, page: dict, bodies: Dict[str, bytes], request: dict, deadline_ms: int
    ) -> dict:
        raise NotImplementedError

    def confirm(self, receipt: dict) -> None:
        raise NotImplementedError

    def records_by_id(self, ids: list) -> list:
        raise NotImplementedError

    def body(self, reference: dict) -> bytes:
        raise NotImplementedError

    def pending_rebase(self) -> Optional[dict]:
        raise NotImplementedError

    def prepare_rebase(self, request: dict, deadline_ms: int) -> dict:
        raise NotImplementedError

    def confirm_rebase(self, result: dict) -> None:
        raise NotImplementedError
