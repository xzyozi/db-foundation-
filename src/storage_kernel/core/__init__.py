"""core: storage_kernel の制御エンジン本体。"""

from __future__ import annotations

from .event_bus import EventDispatcher, PatternMatcher
from .ingestion import WritePipeline, compute_payload_hash
from .kernel import StorageKernel
from .ordering import SequenceCoordinator
from .projection import ProjectionRunner
from .reliability import DeadLetterCoordinator

__all__ = [
    "DeadLetterCoordinator",
    "EventDispatcher",
    "PatternMatcher",
    "ProjectionRunner",
    "SequenceCoordinator",
    "StorageKernel",
    "WritePipeline",
    "compute_payload_hash",
]
