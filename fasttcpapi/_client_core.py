"""Dynamic client for Server's built-in JSON frame protocol."""

from __future__ import annotations

import asyncio
import inspect
import json
import struct
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

from .default_frame import JsonLengthPrefixFrame


class _ClientCore:
    """Lazy async client for a default ``Server`` server.

    The first command call connects, retrieves the command schema, and writes a
    ``.pyi`` cache file. Use ``await client.call("command.name", ...)`` for
    command names that are not valid Python attributes.
    """

    def __init__(self, ip: str, port: int, *, push_queue_size: int = 100) -> None:
        self.ip = ip
        self.port = port
        self._schema: Optional[List[Dict[str, Any]]] = None
        self._lock = asyncio.Lock()
        self._connection_lock = asyncio.Lock()
        self._write_lock = asyncio.Lock()
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._reader_task: Optional[asyncio.Task] = None
        self._pending: Dict[Any, asyncio.Queue] = {}
        if push_queue_size < 1:
            raise ValueError("push_queue_size must be positive")
        self._pushes: asyncio.Queue[JsonLengthPrefixFrame] = asyncio.Queue(maxsize=push_queue_size)
        self._early_frames: List[JsonLengthPrefixFrame] = []
        self._push_queue_size = push_queue_size
        self._push_commands: Set[str] = set()
        self._on_request: List[Callable[..., Any]] = []
        self._on_response: List[Callable[..., Any]] = []
        self._on_push: List[Callable[..., Any]] = []
        self._on_connected: List[Callable[..., Any]] = []
        self._on_disconnected: List[Callable[..., Any]] = []

    def _decorator(self, callbacks, callback=None):
        def register(function):
            callbacks.append(function)
            return function
        return register if callback is None else register(callback)

    def on_request(self, function=None): return self._decorator(self._on_request, function)
    def on_response(self, function=None): return self._decorator(self._on_response, function)
    def on_push(self, function=None): return self._decorator(self._on_push, function)
    def on_connected(self, function=None): return self._decorator(self._on_connected, function)
    def on_disconnected(self, function=None): return self._decorator(self._on_disconnected, function)

    def add_request_callback(self, callback: Callable[..., Any]) -> None:
        self._on_request.append(callback)

    def add_response_callback(self, callback: Callable[..., Any]) -> None:
        self._on_response.append(callback)

    def add_push_callback(self, callback: Callable[..., Any]) -> None:
        self._on_push.append(callback)

    def add_connected_callback(self, callback): self._on_connected.append(callback)
    def add_disconnected_callback(self, callback): self._on_disconnected.append(callback)

    async def _callbacks(self, callbacks: List[Callable[..., Any]], *args: Any) -> None:
        for callback in callbacks:
            try:
                value = callback(*args)
                if inspect.isawaitable(value):
                    await value
            except Exception:
                continue

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
            self._reader, self._writer = await asyncio.open_connection(self.ip, self.port)
            self._reader_task = asyncio.create_task(self._reader_loop())
            await self._callbacks(self._on_connected)

    async def _reader_loop(self) -> None:
        try:
            assert self._reader is not None
            while True:
                frame = await self._read_frame(self._reader)
                if frame.command in self._push_commands:
                    await self._put_push(frame)
                else:
                    queue = self._pending.get(frame.session_id)
                    if queue is not None:
                        await self._callbacks(self._on_response, frame)
                        await queue.put(frame)
                    elif self._schema is None:
                        if len(self._early_frames) == self._push_queue_size:
                            self._early_frames.pop(0)
                        self._early_frames.append(frame)
        except BaseException as exc:
            for queue in list(self._pending.values()):
                await queue.put(exc)
        finally:
            await self._callbacks(self._on_disconnected)
            self._reader = None
            self._writer = None

    async def _put_push(self, frame: JsonLengthPrefixFrame) -> None:
        await self._callbacks(self._on_push, frame)
        if self._pushes.full():
            self._pushes.get_nowait()
        self._pushes.put_nowait(frame)

    async def next_push(self) -> JsonLengthPrefixFrame:
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
        for queue in list(self._pending.values()):
            await queue.put(error)
        self._pending.clear()
        self._reader = None
        self._writer = None

    async def call(self, command: str, *args: Any, **kwargs: Any) -> Any:
        """Call a command. One response returns a value; multiple return a list."""
        await self.connect()
        definition = next((item for item in self._schema or [] if item["command"] == command), None)
        if definition is None:
            raise AttributeError(f"server has no command {command!r}")
        self._validate_call(definition, args, kwargs)
        return await self._request(command, *args, response_frames=definition["response_frames"],
                                   timeout=definition.get("timeout", 30.0), **kwargs)

    async def _request(self, command: str, *args: Any, response_frames: int,
                       timeout: Union[float, List[float]], **kwargs: Any) -> Any:
        await self._ensure_connection()
        assert self._writer is not None
        writer = self._writer
        request_frame = JsonLengthPrefixFrame()
        session_id = request_frame.session_id
        queue: asyncio.Queue[JsonLengthPrefixFrame | BaseException] = asyncio.Queue()
        self._pending[session_id] = queue
        try:
            async with self._write_lock:
                await self._callbacks(self._on_request, command, args, kwargs)
                await self._write_frame(writer, {"command": command, "args": args, "kwargs": kwargs,
                                                  "session_id": session_id})
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
            self._pending.pop(session_id, None)

    async def _write_frame(self, writer: asyncio.StreamWriter, payload: Dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        writer.write(struct.pack("!I", len(body)) + body)
        await writer.drain()

    async def _read_frame(self, reader: asyncio.StreamReader) -> JsonLengthPrefixFrame:
        frame = JsonLengthPrefixFrame()
        await frame.decode_from_reader(reader)
        frame.parse_args([])
        return frame

    @property
    def stub_path(self) -> Path:
        return Path(__file__).with_name("client.pyi")

    def _write_stub(self, schema: List[Dict[str, Any]]) -> None:
        unified_lines = [
            "from concurrent.futures import Future",
            "from typing import Any",
            "import asyncio",
            "",
            "class Command:",
            "    def __call__(self, *args: Any, **kwargs: Any) -> Any: ...",
            "",
            "class Client:",
            "    def __init__(self, ip: str, port: int, *, sync: bool = ..., push_queue_size: int = ...) -> None: ...",
            "    sync: bool",
            "    def __getattr__(self, command: str) -> Command: ...",
            "    def call(self, command: str, *args: Any, **kwargs: Any) -> Any: ...",
            "    def sync_call(self, command: str, *args: Any, **kwargs: Any) -> Any: ...",
            "    def async_call(self, command: str, *args: Any, **kwargs: Any) -> asyncio.Future[Any]: ...",
            "    def submit(self, command: str, *args: Any, **kwargs: Any) -> Future[Any]: ...",
            "    def add_request_callback(self, callback: Any) -> None: ...",
            "    def add_response_callback(self, callback: Any) -> None: ...",
            "    def add_push_callback(self, callback: Any) -> None: ...",
            "    def add_connected_callback(self, callback: Any) -> None: ...",
            "    def add_disconnected_callback(self, callback: Any) -> None: ...",
            "    async def next_push(self) -> Any: ...",
            "    async def close(self) -> None: ...",
            "    def set_service_definition(self, schema: Any) -> None: ...",
        ]
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

    @staticmethod
    def _validate_call(definition: Dict[str, Any], args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> None:
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
        for item in definition["parameters"]:
            if item["name"] not in bound.arguments:
                continue
            expected = _runtime_type(item["type"])
            value = bound.arguments[item["name"]]
            if expected is not None and type(value) is not expected:
                raise TypeError(
                    f"{definition['command']}() argument '{item['name']}' must be {item['type']}, "
                    f"not {type(value).__name__}"
                )


def _stub_type(type_name: str) -> str:
    return type_name if type_name in {"Any", "None", "bool", "bytes", "float", "int", "str"} else "Any"


def _runtime_type(type_name: str) -> Optional[type]:
    return {"bool": bool, "bytes": bytes, "float": float, "int": int, "str": str}.get(type_name)


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
