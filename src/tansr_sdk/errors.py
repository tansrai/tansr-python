"""公开错误默认只打印安全字段，不打印原始服务端 detail。"""

from typing import Any, Optional


class Error(Exception):
    def __init__(
        self,
        code: str,
        message: str = "",
        *,
        http_status: int = 0,
        wire_code: Optional[str] = None,
        retry_action: Optional[str] = None,
        request_id: Optional[str] = None,
        retry_after_ms: Optional[int] = None,
        detail: Any = None,
    ) -> None:
        self.code = code
        self.message = message
        self.http_status = http_status
        self.wire_code = wire_code
        self.retry_action = retry_action
        self.request_id = request_id
        self.retry_after_ms = retry_after_ms
        self.detail = detail
        super().__init__(code)

    def __str__(self) -> str:
        return "Tansr error: {} (HTTP {})".format(self.code, self.http_status)

    def __repr__(self) -> str:
        return "Error(code={!r}, http_status={!r})".format(self.code, self.http_status)


class _Missing:
    def __copy__(self):
        return self

    def __deepcopy__(self, memo):
        return self

    def __repr__(self) -> str:
        return "MISSING"


MISSING = _Missing()
