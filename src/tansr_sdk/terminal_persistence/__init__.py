"""显式 TansrTerminalPersistenceV1 原子存储与受信宿主。"""
from .host import DEFINITION_DIGEST, TOOL_NAME, Host
from .store import FileStore
from ._state import StorageError

__all__ = ["DEFINITION_DIGEST", "TOOL_NAME", "Host", "FileStore", "StorageError"]
