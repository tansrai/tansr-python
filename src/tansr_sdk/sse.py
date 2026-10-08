"""严格、增量且有界的 SSE 解码；解析位置不冒充业务已处理水位。"""

from dataclasses import dataclass
from typing import Iterable, Iterator, Optional

from .errors import Error


@dataclass(frozen=True)
class Frame:
    event: Optional[str]
    data: str
    id: Optional[str]
    retry: Optional[int]


def frames(byte_iterable: Iterable[bytes], max_frame_bytes: int = 1048576) -> Iterator[Frame]:
    if type(max_frame_bytes) is not int or max_frame_bytes < 1:
        raise Error("invalid_argument")
    line = bytearray()
    prefix = bytearray()
    bom_checked = False
    skip_lf = False
    count = 0
    event = None
    event_id = None
    retry = None
    data = []  # type: list
    source = iter(byte_iterable)
    try:
        for chunk in source:
            if not isinstance(chunk, bytes):
                raise Error("contract", "SSE requires bytes")
            for byte in chunk:
                if not bom_checked:
                    prefix.append(byte)
                    if b"\xef\xbb\xbf".startswith(prefix):
                        if len(prefix) < 3:
                            continue
                        bom_checked = True
                        prefix.clear()
                        continue
                    bom_checked = True
                    pending = bytes(prefix)
                    prefix.clear()
                else:
                    pending = bytes((byte,))
                for current in pending:
                    if skip_lf:
                        skip_lf = False
                        if current == 10:
                            continue
                    count += 1
                    if count > max_frame_bytes:
                        raise Error("resource_limit", "SSE frame too large")
                    if current not in (10, 13):
                        line.append(current)
                        continue
                    skip_lf = current == 13
                    try:
                        text = line.decode("utf-8", "strict")
                    except UnicodeError:
                        raise Error("contract", "Invalid SSE UTF-8") from None
                    line.clear()
                    if not text:
                        if data:
                            yield Frame(event, "\n".join(data), event_id, retry)
                        event = event_id = retry = None
                        data = []
                        count = 0
                        continue
                    if text.startswith(":"):
                        continue
                    field, separator, value = text.partition(":")
                    if value.startswith(" "):
                        value = value[1:]
                    if field == "data":
                        data.append(value)
                    elif field == "event":
                        event = value
                    elif field == "id" and "\x00" not in value:
                        event_id = value
                    elif field == "retry" and value and all("0" <= c <= "9" for c in value):
                        # 与冻结实现的 unsigned 64-bit 范围一致，不解析任意长数词。
                        if len(value.lstrip("0")) <= 20:
                            parsed = int(value.lstrip("0") or "0")
                            if parsed <= 18446744073709551615:
                                retry = parsed
        if prefix:
            line.extend(prefix)
        try:
            line.decode("utf-8", "strict")
        except UnicodeError:
            raise Error("contract", "Incomplete SSE UTF-8") from None
        if line or data:
            raise Error("contract", "SSE ended inside an incomplete frame")
    finally:
        close = getattr(source, "close", None)
        if close is not None:
            close()
