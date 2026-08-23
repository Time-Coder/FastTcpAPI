"""The frame abstraction used by Server."""

from __future__ import annotations

import abc
import asyncio
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Param:
    """One handler parameter made available to ``Frame.parse_args``."""

    name: str
    type: Any


class Frame(abc.ABC):
    """Decode a request and encode its response using command/argument fields.

    ``decode_from_reader`` must assign ``self.command`` to the value registered
    by ``@app.command(...)``. Conversion methods transform a handler result or
    exception into the response's ``command``, ``args``, and ``kwargs``. A
    frame can retain arbitrary request metadata, such as session/device IDs,
    for use when encoding the response.
    """

    command: Any = None
    args: tuple[Any, ...]
    kwargs: dict[str, Any]

    def __init__(self) -> None:
        self.args = ()
        self.kwargs = {}

    @abc.abstractmethod
    async def decode_from_reader(self, reader: asyncio.StreamReader) -> None:
        """Read one request frame and assign its command, args, and kwargs."""

    @abc.abstractmethod
    def parse_args(self, param_list: list[Param]) -> None:
        """Populate ``self.args`` and/or ``self.kwargs`` from decoded data."""

    @abc.abstractmethod
    def decode_from_result(self, result: Any) -> None:
        """Transform a handler result into response command, args, and kwargs."""

    @abc.abstractmethod
    def decode_from_exception(self, exception: Exception) -> None:
        """Transform a handler exception into response command, args, and kwargs."""

    @abc.abstractmethod
    def result(self) -> Any:
        """Return this response's value or raise the represented exception."""

    @abc.abstractmethod
    def encode(self) -> bytes:
        """Encode the current command, args, and kwargs into one response."""
