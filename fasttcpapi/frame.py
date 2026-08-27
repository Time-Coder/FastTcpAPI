"""The frame abstraction used by Server."""

from __future__ import annotations

import abc
import asyncio
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple


@dataclass(frozen=True)
class Param:
    """One handler parameter made available to ``Frame.parse_args``."""

    name: str
    type: Any
    has_default: bool = False
    default: Any = None




class Frame(abc.ABC):
    """Decode a request and encode its response using command/argument fields.

``decode`` must assign ``self.command`` to the value registered
    by ``@app.command(...)``. Conversion methods transform a handler result or
    exception into the response's ``command``, ``args``, and ``kwargs``. A
    frame can retain arbitrary request metadata, such as session/device IDs,
    for use when encoding the response.
    """

    command: Any = None
    session_id: Any = None
    args: Tuple[Any, ...]
    kwargs: Dict[str, Any]

    def __init__(self) -> None:
        self.command = None
        self.args = ()
        self.kwargs = {}
        self.session_id = None

    @abc.abstractmethod
    async def decode(self, reader: asyncio.StreamReader) -> None:
        """Read one request frame and assign its command, args, and kwargs."""

    @abc.abstractmethod
    def parse_args(self, param_list: List[Param]) -> None:
        """Populate ``self.args`` and/or ``self.kwargs`` from decoded data."""

    @abc.abstractmethod
    def set_result(self, result: Any, request: "Frame") -> None:
        """Transform a handler result into response command, args, and kwargs."""

    @abc.abstractmethod
    def set_exception(self, exception: Exception, request: "Frame") -> None:
        """Transform a handler exception into response command, args, and kwargs."""

    @abc.abstractmethod
    def result(self) -> Any:
        """Return this response's value or raise the represented exception."""

    @abc.abstractmethod
    def encode(self) -> bytes:
        """Encode the current command, args, and kwargs into one response."""

    def validate(self) -> None:
        if self.command is None:
            raise ValueError("frame.command must be set before encoding")
