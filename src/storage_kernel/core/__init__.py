"""core: storage_kernel の制御エンジン本体。"""

from __future__ import annotations

from .event_bus import EventDispatcher, PatternMatcher
from .ingestion import WritePipeline, compute_payload_hash
from .kernel import StorageKernel
from .ordering import SequenceCoordinator
from .projection import ProjectionRunner
from .reliability import DeadLetterCoordinator
from .sync_engine import CursorSyncEngine, UpcastPipeline

__all__ = [
    "CursorSyncEngine",
    "DeadLetterCoordinator",
    "EventDispatcher",
    "PatternMatcher",
    "ProjectionRunner",
    "SequenceCoordinator",
    "StorageKernel",
    "UpcastPipeline",
    "WritePipeline",
    "compute_payload_hash",
]
