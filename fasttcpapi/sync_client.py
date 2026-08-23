"""Synchronous client for Server's default JSON frame protocol."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, Callable

from .async_client import AsyncClient
from .exceptions import RemoteError


class SyncClient:
    """Blocking facade over :class:`AsyncClient`."""

    def __init__(self, ip: str, port: int) -> None:
        self._async = AsyncClient(ip, port)

    def __getattr__(self, command: str) -> Callable[..., Any]:
        if command.startswith("_"):
            raise AttributeError(command)

        def invoke(*args: Any, **kwargs: Any) -> Any:
            return self.call(command, *args, **kwargs)

        return invoke

    def connect(self) -> None:
        asyncio.run(self._async.connect())

    def call(self, command: str, *args: Any, **kwargs: Any) -> Any:
        return asyncio.run(self._async.call(command, *args, **kwargs))

    @property
    def stub_path(self) -> Path:
        return Path(__file__).with_name("sync_client.pyi")


__all__ = ["RemoteError", "SyncClient"]
