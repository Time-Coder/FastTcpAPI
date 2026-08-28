"""Server-side state for connected clients."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple


@dataclass(eq=False)
class ClientConnection:
    """State and connection-local metadata for one TCP client."""

    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    metadata: Optional[Dict[str, Any]] = None

    def __post_init__(self) -> None:
        if self.metadata is None:
            self.metadata = {}

    @property
    def address(self) -> Optional[Tuple[Any, ...]]:
        return self.writer.get_extra_info("peername")
