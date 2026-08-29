"""Unified synchronous/asynchronous client facade."""

from __future__ import annotations

import asyncio
import threading
from concurrent.futures import Future
from typing import Any, Optional, Type

from ._client_core import _ClientCore
from .json_frame import JsonFrame
from .frame import Frame
from .loggers import ClientLogger, DefaultClientLogger

class _Command:
    def __init__(self, client: Client, name: str) -> None:
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

    def __init__(self, server_host: str, server_port: int, *, self_host: Optional[str] = None,
                 self_port: int = 0, frame_type: Type[Frame] = JsonFrame,
                 sync: bool = False, push_queue_size: int = 100,
                 strict_type_check: bool = True,
                 logger: Optional[Type[ClientLogger]] = DefaultClientLogger) -> None:
        super().__init__(server_host, server_port, self_host=self_host, self_port=self_port,
                         frame_type=frame_type, push_queue_size=push_queue_size,
                         strict_type_check=strict_type_check, logger=logger)
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

    def connect(self, host: Optional[str] = None, port: Optional[int] = None,
                *, wait: bool = False):
        """Start background connection/reconnection; optionally wait for success."""
        self._ensure_submit_loop()
        assert self._submit_loop is not None
        if host is not None:
            self.server_host = host
        if port is not None:
            self.server_port = port
        self._stop_reconnect = False
        pending = asyncio.run_coroutine_threadsafe(self._start_reconnect_loop(), self._submit_loop)
        if wait:
            if self.sync:
                pending.result()
                return self.wait_for_connected()
            async def wait_for_start() -> None:
                await asyncio.wrap_future(pending)
                await self._wait_connected_external()
            return asyncio.ensure_future(wait_for_start())
        if not self.sync:
            return asyncio.wrap_future(pending)

    async def _wait_connected_external(self) -> None:
        pending = asyncio.run_coroutine_threadsafe(
            self._wait_connected_internal(), self._submit_loop
        )
        await asyncio.wrap_future(pending)

    def wait_for_connected(self):
        self._ensure_submit_loop()
        assert self._submit_loop is not None
        if self._stop_reconnect:
            raise ConnectionError("Client is disconnected; call connect() first")
        if threading.current_thread() is self._submit_thread:
            if self._connected_event is None:
                asyncio.create_task(self._start_reconnect_loop())
            return self._wait_connected_internal()
        pending = asyncio.run_coroutine_threadsafe(
            self._wait_connected_internal(), self._submit_loop
        )
        if self.sync:
            return pending.result()
        return asyncio.wrap_future(pending)

    def disconnect(self, *, wait: bool = False):
        self._ensure_submit_loop()
        assert self._submit_loop is not None
        self._stop_reconnect = True
        pending = asyncio.run_coroutine_threadsafe(_ClientCore.close(self), self._submit_loop)
        if wait:
            if self.sync:
                pending.result()
                return self.wait_for_disconnected()
            async def wait_for_close() -> None:
                await asyncio.wrap_future(pending)
                await self._wait_disconnected_external()
            return asyncio.ensure_future(wait_for_close())
        if not self.sync:
            return asyncio.wrap_future(pending)

    async def _wait_disconnected_external(self) -> None:
        pending = asyncio.run_coroutine_threadsafe(
            self._wait_disconnected_internal(), self._submit_loop
        )
        await asyncio.wrap_future(pending)

    def wait_for_disconnected(self):
        self._ensure_submit_loop()
        assert self._submit_loop is not None
        if threading.current_thread() is self._submit_thread:
            if self._disconnected_event is not None:
                return self._disconnected_event.wait()
            return None
        pending = asyncio.run_coroutine_threadsafe(
            self._wait_disconnected_internal(), self._submit_loop
        )
        if self.sync:
            return pending.result()
        return asyncio.wrap_future(pending)

__all__ = ["Client"]
