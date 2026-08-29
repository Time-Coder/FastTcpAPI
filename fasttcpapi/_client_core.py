"""Dynamic client for Server's built-in JSON frame protocol."""

from __future__ import annotations

import asyncio
import inspect
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Type, Union

from .json_frame import JsonFrame
from .frame import Frame
from .loggers import ClientLogger, DefaultClientLogger


class _ClientCore:
    """Lazy async client for a default ``Server`` server.

    The first command call connects, retrieves the command schema, and writes a
    ``.pyi`` cache file. Use ``await client.call("command.name", ...)`` for
    command names that are not valid Python attributes.
    """

    def __init__(self, server_host: str, server_port: int, *, self_host: Optional[str] = None,
                 self_port: int = 0, frame_type: Type[Frame] = JsonFrame,
                 push_queue_size: int = 100, strict_type_check: bool = True,
                 logger: Optional[Type[ClientLogger]] = DefaultClientLogger) -> None:
        if not issubclass(frame_type, Frame):
            raise TypeError("frame_type must be a Frame subclass")
        self.server_host = server_host
        self.server_port = server_port
        self.self_host = self_host
        self.self_port = self_port
        self.frame_type = frame_type
        self.strict_type_check = strict_type_check
        if logger is None:
            self.logger: Optional[ClientLogger] = None
        elif isinstance(logger, type) and issubclass(logger, ClientLogger):
            self.logger = logger(self)
        else:
            raise TypeError("logger must be a ClientLogger subclass")
        self._schema: Optional[List[Dict[str, Any]]] = None
        self._lock = asyncio.Lock()
        self._connection_lock = asyncio.Lock()
        self._write_lock = asyncio.Lock()
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._reader_task: Optional[asyncio.Task] = None
        self._pending: Dict[Any, asyncio.Queue] = {}
        self._none_pending: List[asyncio.Queue] = []
        self._none_pending_remaining: Dict[asyncio.Queue, int] = {}
        if push_queue_size < 1:
            raise ValueError("push_queue_size must be positive")
        self._pushes: asyncio.Queue = asyncio.Queue(maxsize=push_queue_size)
        self._early_frames: List[Frame] = []
        self._push_queue_size = push_queue_size
        self._push_commands: Set[str] = set()
        self._on_request: List[Callable[..., Any]] = []
        self._on_response: List[Callable[..., Any]] = []
        self._on_push: List[Callable[..., Any]] = []
        self._on_connected: List[Callable[..., Any]] = []
        self._on_disconnected: List[Callable[..., Any]] = []
        self._on_retry_connect: List[Callable[..., Any]] = []
        self._auto_task = None
        self._connected_event = None
        self._disconnected_event = None
        self._stop_reconnect = False

    @property
    def server_address(self) -> Tuple[str, int]:
        """Configured remote server address."""
        return self.server_host, self.server_port

    @property
    def self_address(self) -> Tuple[Optional[str], int]:
        """Current local socket address, or the configured local bind address."""
        if self._writer is not None and not self._writer.is_closing():
            address = self._writer.get_extra_info("sockname")
            if isinstance(address, tuple) and len(address) >= 2:
                return address[0], address[1]
        return self.self_host, self.self_port

    def _decorator(self, callbacks, callback=None):
        def register(function):
            async def decorated(*args: Any) -> Any:
                value = function(*args[1:])
                if inspect.isawaitable(value):
                    return await value
                return value
            callbacks.append(decorated)
            return function
        return register if callback is None else register(callback)

    def on_request(self, function=None): return self._decorator(self._on_request, function)
    def on_response(self, function=None): return self._decorator(self._on_response, function)
    def on_push(self, function=None): return self._decorator(self._on_push, function)
    def on_connected(self, function=None): return self._decorator(self._on_connected, function)
    def on_disconnected(self, function=None): return self._decorator(self._on_disconnected, function)
    def on_retry_connect(self, function=None): return self._decorator(self._on_retry_connect, function)

    def add_request_callback(self, callback: Callable[..., Any]) -> None:
        self._on_request.append(callback)

    def add_response_callback(self, callback: Callable[..., Any]) -> None:
        self._on_response.append(callback)

    def add_push_callback(self, callback: Callable[..., Any]) -> None:
        self._on_push.append(callback)

    def add_connected_callback(self, callback): self._on_connected.append(callback)
    def add_disconnected_callback(self, callback): self._on_disconnected.append(callback)

    def add_retry_connect_callback(self, callback: Callable[..., Any]) -> None:
        self._on_retry_connect.append(callback)

    async def _callbacks(self, callbacks: List[Callable[..., Any]], *args: Any) -> None:
        for callback in callbacks:
            try:
                try:
                    value = callback(*args)
                except TypeError:
                    # Keep compatibility with callbacks written before the
                    # client instance became the first argument.
                    value = callback(*args[1:]) if args else callback()
                if inspect.isawaitable(value):
                    await value
            except Exception:
                continue

    async def _logger_call(self, name: str, *args: Any) -> None:
        if self.logger is None:
            return
        try:
            value = getattr(self.logger, name)(*args)
            if inspect.isawaitable(value):
                await value
        except Exception:
            pass

    def __getattr__(self, command: str) -> Callable[..., Any]:
        if command.startswith("_"):
            raise AttributeError(command)

        async def invoke(*args: Any, **kwargs: Any) -> Any:
            return await self.call(command, *args, **kwargs)

        return invoke

    async def connect(self) -> None:
        """Retrieve server definitions and generate the local type-stub cache."""
        if self._schema is not None:
            return
        async with self._lock:
            if self._schema is not None:
                return
            if self.frame_type is not JsonFrame:
                raise RuntimeError("custom frame clients require set_service_definition()")
            schema = await self._request("__fasttcpapi__.schema", response_frames=1, timeout=30.0)
            if not isinstance(schema, list):
                raise RuntimeError("server returned an invalid command schema")
            self.set_service_definition(schema)
            for frame in self._early_frames:
                if frame.command in self._push_commands:
                    await self._put_push(frame)
            self._early_frames.clear()

    def set_service_definition(self, schema: List[Dict[str, Any]]) -> None:
        """Set or replace the server command definition used by this client."""
        if not isinstance(schema, list):
            raise TypeError("service definition must be a list")
        for item in schema:
            if not isinstance(item, dict) or not isinstance(item.get("command"), str):
                raise TypeError("each service definition must contain a string command")
            if item.get("push") is True:
                continue
            if not isinstance(item.get("parameters", []), list):
                raise TypeError("service definition parameters must be a list")
            if not isinstance(item.get("response_frames", 1), int) or item.get("response_frames", 1) < 1:
                raise ValueError("response_frames must be a positive integer")
        self._schema = schema
        self._push_commands = {item["command"] for item in schema if item.get("push") is True}
        self._write_stub(schema)

    async def _ensure_connection(self) -> None:
        async with self._connection_lock:
            if self._writer is not None and not self._writer.is_closing():
                return
            local_addr = (self.self_host, self.self_port) if self.self_host is not None else None
            self._reader, self._writer = await asyncio.open_connection(
                self.server_host, self.server_port, local_addr=local_addr
            )
            self._reader_task = asyncio.create_task(self._reader_loop())
            await self._callbacks(self._on_connected, self)
            await self._logger_call("on_connected")

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
                    await self._callbacks(self._on_retry_connect, self)
                    await self._logger_call("on_retry_connect")
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

    async def _wait_disconnected_internal(self) -> None:
        if self._disconnected_event is not None:
            await self._disconnected_event.wait()

    async def _reader_loop(self) -> None:
        try:
            assert self._reader is not None
            while True:
                try:
                    frame = await self._read_frame(self._reader)
                except (asyncio.IncompleteReadError, ConnectionError, OSError):
                    raise
                except Exception:
                    # Frame codecs may reject corrupt data and resynchronise on
                    # their next decode attempt.
                    if self._reader is not None and self._reader.at_eof():
                        raise ConnectionError("connection closed while decoding frame")
                    continue
                if frame.command in self._push_commands:
                    await self._put_push(frame)
                else:
                    queue = (self._none_pending[0] if frame.session_id is None and self._none_pending
                             else self._pending.get(frame.session_id))
                    is_schema_response = frame.command == "response" or (
                        isinstance(frame.command, str) and frame.command.startswith("response.")
                    )
                    if queue is not None and (self._schema is not None or is_schema_response):
                        await self._callbacks(self._on_response, self, frame)
                        await self._logger_call("on_response", frame)
                        await queue.put(frame)
                        if frame.session_id is None and queue in self._none_pending_remaining:
                            self._none_pending_remaining[queue] -= 1
                            if self._none_pending_remaining[queue] <= 0:
                                self._none_pending_remaining.pop(queue, None)
                                if queue in self._none_pending:
                                    self._none_pending.remove(queue)
                    elif self._schema is None:
                        if len(self._early_frames) == self._push_queue_size:
                            self._early_frames.pop(0)
                        self._early_frames.append(frame)
                    elif self._schema is None:
                        if len(self._early_frames) == self._push_queue_size:
                            self._early_frames.pop(0)
                        self._early_frames.append(frame)
        except BaseException as exc:
            for queue in list(self._pending.values()) + list(self._none_pending):
                await queue.put(exc)
        finally:
            await self._callbacks(self._on_disconnected, self)
            await self._logger_call("on_disconnected")
            event = getattr(self, "_disconnected_event", None)
            if event is not None:
                event.set()
            event = getattr(self, "_connected_event", None)
            if event is not None:
                event.clear()
            self._reader = None
            self._writer = None

    async def _put_push(self, frame: Frame) -> None:
        await self._callbacks(self._on_push, self, frame)
        await self._logger_call("on_push", frame)
        if self._pushes.full():
            self._pushes.get_nowait()
        self._pushes.put_nowait(frame)

    async def next_push(self) -> Frame:
        """Wait for the next unsolicited server push frame."""
        await self.connect()
        return await self._pushes.get()

    async def close(self) -> None:
        """Close the persistent TCP connection and wake waiting calls."""
        if self._reader_task is not None:
            self._reader_task.cancel()
        if self._writer is not None:
            self._writer.close()
        error = ConnectionError("Client connection closed")
        for queue in list(self._pending.values()) + list(self._none_pending):
            await queue.put(error)
        self._pending.clear()
        self._none_pending.clear()
        self._none_pending_remaining.clear()
        self._reader = None
        self._writer = None

    async def call(self, command: str, *args: Any, **kwargs: Any) -> Any:
        """Call a command. One response returns a value; multiple return a list."""
        connector = getattr(self, "_connect_for_call", None)
        if connector is not None:
            await connector()
        else:
            await self.connect()
        definition = next((item for item in self._schema or [] if item["command"] == command), None)
        if definition is None:
            raise AttributeError(f"server has no command {command!r}")
        args, kwargs = self._validate_call(definition, args, kwargs)
        return await self._request(command, *args, response_frames=definition["response_frames"],
                                   timeout=definition.get("timeout", 30.0), **kwargs)

    async def _request(self, command: str, *args: Any, response_frames: int,
                       timeout: Union[float, List[float]], **kwargs: Any) -> Any:
        request_frame = self.frame_type()
        request_frame.command = command
        request_frame.args = args
        request_frame.kwargs = kwargs
        request_frame.validate()
        await self._ensure_connection()
        assert self._writer is not None
        writer = self._writer
        session_id = request_frame.session_id
        queue: asyncio.Queue[Frame | BaseException] = asyncio.Queue()
        if session_id is None:
            self._none_pending.append(queue)
            self._none_pending_remaining[queue] = response_frames
        else:
            self._pending[session_id] = queue
        try:
            async with self._write_lock:
                await self._callbacks(self._on_request, self, request_frame)
                await self._logger_call("on_request", request_frame)
                await self._write_frame(writer, request_frame)
            results: List[Any] = []
            timeout_values = timeout if isinstance(timeout, list) else [timeout] * response_frames
            started = asyncio.get_running_loop().time()
            for index in range(response_frames):
                remaining = started + timeout_values[index] - asyncio.get_running_loop().time()
                if remaining <= 0:
                    raise TimeoutError(f"timed out waiting for response to {command!r}")
                response = await asyncio.wait_for(queue.get(), remaining)
                if isinstance(response, BaseException):
                    raise response
                results.append(response.result())
            return results[0] if response_frames == 1 else results
        finally:
            if session_id is not None:
                self._pending.pop(session_id, None)
            if session_id is None and queue in self._none_pending:
                self._none_pending.remove(queue)
            self._none_pending_remaining.pop(queue, None)

    async def _write_frame(self, writer: asyncio.StreamWriter, frame: Frame) -> None:
        writer.write(frame.encode())
        await writer.drain()

    async def _read_frame(self, reader: asyncio.StreamReader) -> Frame:
        frame = self.frame_type()
        await frame.decode(reader)
        frame.parse_args([])
        return frame

    def _write_stub(self, schema: List[Dict[str, Any]]) -> None:
        template = Path(__file__).with_name("client.pyi.in")
        try:
            unified_lines = template.read_text(encoding="utf-8").splitlines()
        except OSError:
            # Stub generation is optional when package data is unavailable
            # (for example, an incorrectly configured frozen application).
            return
        wrote_method = False
        for command in schema:
            name = command["command"]
            if not isinstance(name, str) or not name.isidentifier() or command.get("push"):
                continue
            parameters = []
            for parameter in command["parameters"]:
                annotation = _stub_type(parameter["type"])
                kind = parameter["kind"]
                if kind == "KEYWORD_ONLY" and "*" not in parameters:
                    parameters.append("*")
                default = " = ..." if parameter["has_default"] else ""
                parameters.append(f"{parameter['name']}: {annotation}{default}")
            signature = ", ".join(["self", *parameters])
            unified_lines.append(f"    def {name}({signature}) -> Any: ...")
            wrote_method = True
        if not wrote_method:
            unified_lines.append("    pass")
        package_dir = Path(__file__).parent
        (package_dir / "client.pyi").write_text("\n".join(unified_lines) + "\n", encoding="utf-8")

    def _validate_call(self, definition: Dict[str, Any], args: Tuple[Any, ...], kwargs: Dict[str, Any]):
        parameters = []
        for item in definition["parameters"]:
            kind = getattr(inspect.Parameter, item["kind"])
            default = item["default"] if item["has_default"] else inspect.Parameter.empty
            parameters.append(inspect.Parameter(item["name"], kind, default=default))
        signature = inspect.Signature(parameters)
        try:
            bound = signature.bind(*args, **kwargs)
        except TypeError as exc:
            raise TypeError(_call_error(definition["command"], str(exc))) from None
        converted = bound.arguments
        for item in definition["parameters"]:
            if item["name"] not in bound.arguments:
                continue
            expected = _runtime_type(item["type"])
            value = bound.arguments[item["name"]]
            if expected is None or expected is Any:
                continue
            if type(value) is expected:
                continue
            if self.strict_type_check:
                raise TypeError(
                    f"{definition['command']}() argument '{item['name']}' must be {item['type']}, "
                    f"not {type(value).__name__}"
                )
            try:
                converted[item["name"]] = expected(value)
            except (TypeError, ValueError, OverflowError) as exc:
                raise TypeError(
                    f"{definition['command']}() argument '{item['name']}' could not be converted to "
                    f"{item['type']}"
                ) from exc
        return tuple(bound.args), dict(bound.kwargs)


def _stub_type(type_name: str) -> str:
    return type_name if type_name in {"Any", "None", "bool", "bytearray", "bytes", "float", "int", "str"} else "Any"


def _runtime_type(type_name: str) -> Optional[type]:
    return {"bool": bool, "bytearray": bytearray, "bytes": bytes,
            "float": float, "int": int, "str": str}.get(type_name)


def _call_error(command: str, message: str) -> str:
    if message.startswith("missing a required argument: "):
        name = message[len("missing a required argument: "):]
        return f"{command}() missing 1 required positional argument: {name}"
    if message == "too many positional arguments":
        return f"{command}() takes fewer positional arguments than were given"
    if message.startswith("got an unexpected keyword argument"):
        return f"{command}() {message}"
    if message.startswith("multiple values for argument"):
        return f"{command}() got {message}"
    return f"{command}() {message}"
