# GWT/messages.py
from dataclasses import dataclass, field
from typing import Any
import time


@dataclass(frozen=True)
class Message:
    # core
    source: str
    content: Any
    priority: float
    timestamp: float = field(default_factory=time.time)
    # new additions
    semantic_tags: list[str] = field(default_factory=list)
    metadata: dict = field(default_factory=dict)
    attempts: int = 0 


@dataclass
class GWTConfig:
    expiry_seconds: float = 2.0
    tick_rate: float = 0.05
    decay_rate: float = 0.1
    # 0.72 was picked cause it is selective without making workspace ignition extremely rare.
    ignition_threshold: float = 0.72
    max_retries: int = 3 