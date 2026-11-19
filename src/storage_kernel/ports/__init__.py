"""ports: 親プロジェクト（DAO / Adapter）が実装する SPI Protocol 群。"""

from __future__ import annotations

from .codec import PayloadCodec, PayloadUpcaster
from .dao import DeadLetterPort, MetaDAOPort, ProjectionPort, ShardDAOPort, ShardTxPort
from .maintenance import MaintenanceHooks
from .router import PartitionRouterPort

__all__ = [
    "DeadLetterPort",
    "MaintenanceHooks",
    "MetaDAOPort",
    "PartitionRouterPort",
    "PayloadCodec",
    "PayloadUpcaster",
    "ProjectionPort",
    "ShardDAOPort",
    "ShardTxPort",
]
