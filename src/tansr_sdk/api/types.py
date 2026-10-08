"""统一 API 的显式请求选项，不把缺席 body 与 JSON null 混同。"""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional
from ..errors import MISSING
from ..lifecycle import CancellationToken


@dataclass(frozen=True)
class AuthToken:
    value: str = field(repr=False)
    principal: str


@dataclass
class CallOptions:
    parameters: Dict[str, str] = field(default_factory=dict)
    query: Dict[str, str] = field(default_factory=dict)
    body: Any = MISSING
    raw_body: Optional[bytes] = field(default=None, repr=False)
    content_type: Optional[str] = None
    if_match: Optional[str] = None
    request_key: Optional[str] = None
    capability_closure: Optional[str] = None
    last_event_id: Optional[str] = None
    deadline_ms: Optional[int] = None
    cancel: Optional[CancellationToken] = field(default=None, repr=False)
    max_response_bytes: Optional[int] = None


@dataclass
class ApiResponse:
    status: int
    body: Any = None
    raw_body: bytes = field(default=b"", repr=False)
    headers: Dict[str, str] = field(default_factory=dict)
    etag: Optional[str] = None
    capability_closure: Optional[str] = None
    content_type: str = ""
    domain: str = ""
