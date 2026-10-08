"""公开会话类型；受理回执与当前轮结果分别表达。"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union

from ..lifecycle import CancellationToken


@dataclass
class WriteOptions:
    request_key: Optional[str] = None
    deadline_ms: Optional[int] = None
    cancel: Optional[CancellationToken] = None


@dataclass
class ResumeReference:
    session_id: str


@dataclass
class ForkReference:
    session_id: str
    checkpoint_id: str


@dataclass
class Budget:
    max_usd: Optional[float] = None
    max_tokens: Optional[int] = None


@dataclass
class CreateOptions:
    request_id: Optional[str] = None
    model: Optional[str] = None
    prompt: Optional[str] = None
    profile: Optional[str] = None
    budget: Optional[Budget] = None
    tools: Optional[List[str]] = None
    client_tools: Optional[List[Dict[str, Any]]] = None
    capabilities_profile: Optional[str] = None
    cwd: Optional[str] = None
    resume: Optional[ResumeReference] = None
    fork: Optional[ForkReference] = None
    write: WriteOptions = field(default_factory=WriteOptions)


@dataclass(frozen=True)
class Created:
    session_id: str
    resumed: bool = False
    last_seq: int = 0


@dataclass(frozen=True)
class Meta:
    session_id: str
    status: str
    live: bool
    last_seq: int
    raw: Dict[str, Any]


@dataclass(frozen=True)
class SessionList:
    sessions: List[Meta]
    total: int


@dataclass(frozen=True)
class Accepted:
    accepted: bool
    session_id: Optional[str] = None


@dataclass
class TextBlock:
    text: str


@dataclass
class ImageBlock:
    mime: str
    data: str


Block = Union[TextBlock, ImageBlock]


@dataclass
class Answer:
    question_id: str
    selected_option_ids: List[str] = field(default_factory=list)
    free_text: Optional[str] = None


@dataclass
class InputTarget:
    history_epoch: str
    turn_id: str


@dataclass
class InputContent:
    text: Optional[str] = None
    blocks: Optional[List[Block]] = None


@dataclass
class Input:
    input_id: str
    target: InputTarget
    content: InputContent
    ack: Optional[str] = None


@dataclass(frozen=True)
class CapabilityClosure:
    closure_id: str
    operations: Dict[str, str]
    raw: Dict[str, Any]


@dataclass(frozen=True)
class Checkpoint:
    checkpoint_id: str
    session_id: str
    message_count: int
    raw: Dict[str, Any]


@dataclass
class LabeledCheckpoint:
    label: Optional[str] = None


@dataclass
class CompactOptions:
    instructions: Optional[str] = None
    checkpoint: Optional[Union[bool, LabeledCheckpoint]] = None


@dataclass
class TranscriptionRequest:
    audio: str
    model: Optional[str] = None
    language: Optional[str] = None
    diarize: Optional[bool] = None
    prompt: Optional[str] = None


@dataclass
class SpeechRequest:
    input: str
    model: Optional[str] = None
    voice: Optional[str] = None
    format: Optional[str] = None
    speed: Optional[float] = None


@dataclass(frozen=True)
class Outcome:
    status: str
    turn_id: Optional[str] = None
    reason: Optional[str] = None
