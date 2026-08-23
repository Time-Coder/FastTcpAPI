"""Unified synchronous/asynchronous client facade."""

from __future__ import annotations

import asyncio
import threading
from concurrent.futures import Future
from typing import Any, Optional

from ._client_core import _ClientCore

class _Command:
    def __init__(self, client: "Client", name: str) -> None:
        self._client = client
        self.name = name

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """Invoke this command using the client's current execution mode."""
        if self._client.sync:
            return self._client.sync_call(self.name, *args, **kwargs)
        return self._client.async_call(self.name, *args, **kwargs)

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self.name!r}>"


class Client(_ClientCore):
    """One client supporting blocking and non-blocking command invocation.

    ``sync`` can be changed at any time. The command proxy returned by
    ``client.command_name`` reads the current value for every call.
    """

    def __init__(self, ip: str, port: int, *, sync: bool = False, push_queue_size: int = 100) -> None:
        super().__init__(ip, port, push_queue_size=push_queue_size)
        self.sync = sync
        self._submit_loop: Optional[asyncio.AbstractEventLoop] = None
        self._submit_thread: Optional[threading.Thread] = None
        self._submit_ready = threading.Event()

    def __getattr__(self, command: str) -> _Command:
        if command.startswith("_"):
            raise AttributeError(command)
        if self._schema is not None and not any(
            item.get("command") == command for item in self._schema
        ):
            raise AttributeError(f"server has no command {command!r}")
        return _Command(self, command)

    def call(self, command: str, *args: Any, **kwargs: Any) -> Any:
        """Invoke using the current value of ``self.sync``."""
        if self.sync:
            return self.sync_call(command, *args, **kwargs)
        return self.async_call(command, *args, **kwargs)

    def sync_call(self, command: str, *args: Any, **kwargs: Any) -> Any:
        """Invoke synchronously and return the decoded result."""
        self._ensure_submit_loop()
        assert self._submit_loop is not None
        return asyncio.run_coroutine_threadsafe(
            _ClientCore.call(self, command, *args, **kwargs), self._submit_loop
        ).result()

    def async_call(self, command: str, *args: Any, **kwargs: Any) -> asyncio.Future[Any]:
        """Schedule an invocation on the running loop and return its Future."""
        self._ensure_submit_loop()
        assert self._submit_loop is not None
        caller_loop = asyncio.get_running_loop()
        concurrent_future = asyncio.run_coroutine_threadsafe(
            _ClientCore.call(self, command, *args, **kwargs), self._submit_loop
        )
        return asyncio.wrap_future(concurrent_future, loop=caller_loop)

    def submit(self, command: str, *args: Any, **kwargs: Any) -> Future[Any]:
        """Run an invocation in a background thread and return its Future."""
        self._ensure_submit_loop()
        assert self._submit_loop is not None
        return asyncio.run_coroutine_threadsafe(
            _ClientCore.call(self, command, *args, **kwargs), self._submit_loop
        )

    def _ensure_submit_loop(self) -> None:
        if self._submit_loop is not None and self._submit_thread is not None:
            return
        def runner() -> None:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self._submit_loop = loop
            self._submit_ready.set()
            loop.run_forever()
        self._submit_thread = threading.Thread(target=runner, name="fasttcpapi-submit", daemon=True)
        self._submit_thread.start()
        self._submit_ready.wait()

    async def connect(self) -> None:
        """Connect through the client's dedicated I/O event loop."""
        self._ensure_submit_loop()
        assert self._submit_loop is not None
        if threading.current_thread() is self._submit_thread:
            await _ClientCore.connect(self)
            return
        await asyncio.wrap_future(
            asyncio.run_coroutine_threadsafe(_ClientCore.connect(self), self._submit_loop)
        )

    async def close(self) -> None:
        """Close the client's persistent connection."""
        self._ensure_submit_loop()
        assert self._submit_loop is not None
        await asyncio.wrap_future(
            asyncio.run_coroutine_threadsafe(_ClientCore.close(self), self._submit_loop)
        )


__all__ = ["Client"]
