"""Unified synchronous/asynchronous client facade."""

from __future__ import annotations

import asyncio
import threading
from concurrent.futures import Future
from typing import Any, Optional, Type

from ._client_core import _ClientCore
from .json_frame import JsonFrame
from .frame import Frame

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

    def __init__(self, ip: str, port: int, *, frame_type: Type[Frame] = JsonFrame,
                 sync: bool = False, push_queue_size: int = 100,
                 strict_type_check: bool = True) -> None:
        super().__init__(ip, port, frame_type=frame_type, push_queue_size=push_queue_size,
                         strict_type_check=strict_type_check)
        self.sync = sync
        self._submit_loop: Optional[asyncio.AbstractEventLoop] = None
        self._submit_thread: Optional[threading.Thread] = None
        self._submit_ready = threading.Event()
        self._auto_task = None
        self._connected_event = None
        self._disconnected_event = None
        # A newly created client may connect implicitly on its first call.
        self._stop_reconnect = False
        self._host = ip
        self._port = port

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

    def connect(self, host: Optional[str] = None, port: Optional[int] = None):
        """Start background connection/reconnection and return immediately."""
        self._ensure_submit_loop()
        assert self._submit_loop is not None
        if host is not None:
            self._host = self.ip = host
        if port is not None:
            self._port = self.port = port
        self._stop_reconnect = False
        pending = asyncio.run_coroutine_threadsafe(self._start_reconnect_loop(), self._submit_loop)
        if not self.sync:
            return asyncio.wrap_future(pending)

    async def _start_reconnect_loop(self) -> None:
        if self._auto_task is None or self._auto_task.done():
            self._connected_event = asyncio.Event()
            self._disconnected_event = asyncio.Event()
            self._disconnected_event.set()
            self._auto_task = asyncio.create_task(self._reconnect_loop())

    async def _reconnect_loop(self) -> None:
        while not self._stop_reconnect:
            try:
                if self._writer is None or self._writer.is_closing():
                    if self._schema is None:
                        await _ClientCore.connect(self)
                    else:
                        await self._ensure_connection()
                    if self._connected_event is not None:
                        self._connected_event.set()
                    if self._disconnected_event is not None:
                        self._disconnected_event.clear()
                await asyncio.sleep(0.5)
            except Exception:
                if self._connected_event is not None:
                    self._connected_event.clear()
                await asyncio.sleep(3.0)

    async def _wait_connected_internal(self) -> None:
        if self._connected_event is None:
            await self._start_reconnect_loop()
        await self._connected_event.wait()

    async def _connect_for_call(self) -> None:
        if self._stop_reconnect:
            raise ConnectionError("Client is disconnected; call connect() first")
        self.connect()
        result = self.wait_for_connected()
        if result is not None:
            await result

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

    def disconnect(self):
        self._ensure_submit_loop()
        assert self._submit_loop is not None
        self._stop_reconnect = True
        pending = asyncio.run_coroutine_threadsafe(_ClientCore.close(self), self._submit_loop)
        if not self.sync:
            return asyncio.wrap_future(pending)

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

    async def _wait_disconnected_internal(self) -> None:
        if self._disconnected_event is not None:
            await self._disconnected_event.wait()

    async def close(self) -> None:
        """Close the client's persistent connection."""
        result = self.disconnect()
        if result is not None:
            await result


__all__ = ["Client"]
