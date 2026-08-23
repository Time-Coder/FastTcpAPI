"""Dynamic client for Server's built-in JSON frame protocol."""

from __future__ import annotations

import asyncio
import inspect
import json
import re
import struct
from pathlib import Path
from typing import Any, Callable

from .default_frame import JsonLengthPrefixFrame


class AsyncClient:
    """Lazy async client for a default ``Server`` server.

    The first command call connects, retrieves the command schema, and writes a
    ``.pyi`` cache file. Use ``await client.call("command.name", ...)`` for
    command names that are not valid Python attributes.
    """

    def __init__(self, ip: str, port: int, *, cache_dir: str | Path = ".fasttcpapi") -> None:
        self.ip = ip
        self.port = port
        self.cache_dir = Path(cache_dir)
        self._schema: list[dict[str, Any]] | None = None
        self._lock = asyncio.Lock()

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
            schema = await self._request("__fasttcpapi__.schema", response_frames=1)
            if not isinstance(schema, list):
                raise RuntimeError("server returned an invalid command schema")
            self._schema = schema
            self._write_stub(schema)

    async def call(self, command: str, *args: Any, **kwargs: Any) -> Any:
        """Call a command. One response returns a value; multiple return a list."""
        await self.connect()
        definition = next((item for item in self._schema or [] if item["command"] == command), None)
        if definition is None:
            raise AttributeError(f"server has no command {command!r}")
        self._validate_call(definition, args, kwargs)
        return await self._request(command, *args, response_frames=definition["response_frames"], **kwargs)

    async def _request(self, command: str, *args: Any, response_frames: int, **kwargs: Any) -> Any:
        reader, writer = await asyncio.open_connection(self.ip, self.port)
        try:
            await self._write_frame(writer, {"command": command, "args": args, "kwargs": kwargs})
            results: list[Any] = []
            for _ in range(response_frames):
                response = await self._read_frame(reader)
                results.append(response.result())
            return results[0] if response_frames == 1 else results
        finally:
            writer.close()
            await writer.wait_closed()

    async def _write_frame(self, writer: asyncio.StreamWriter, payload: dict[str, Any]) -> None:
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
        return Path(__file__).with_name("async_client.pyi")

    def _write_stub(self, schema: list[dict[str, Any]]) -> None:
        async_lines = [
            "from pathlib import Path",
            "from typing import Any",
            "from .exceptions import RemoteError",
            "",
            "class AsyncClient:",
            "    def __init__(self, ip: str, port: int, *, cache_dir: str | Path = ...) -> None: ...",
            "    async def connect(self) -> None: ...",
            "    async def call(self, command: str, *args: Any, **kwargs: Any) -> Any: ...",
            "    @property",
            "    def stub_path(self) -> Path: ...",
        ]
        sync_lines = [
            "from pathlib import Path",
            "from typing import Any",
            "from .exceptions import RemoteError",
            "",
            "class SyncClient:",
            "    def __init__(self, ip: str, port: int, *, cache_dir: str | Path = ...) -> None: ...",
            "    def connect(self) -> None: ...",
            "    def call(self, command: str, *args: Any, **kwargs: Any) -> Any: ...",
            "    @property",
            "    def stub_path(self) -> Path: ...",
        ]
        wrote_method = False
        for command in schema:
            name = command["command"]
            if not isinstance(name, str) or not name.isidentifier():
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
            async_lines.append(f"    async def {name}({signature}) -> Any: ...")
            sync_lines.append(f"    def {name}({signature}) -> Any: ...")
            wrote_method = True
        if not wrote_method:
            async_lines.append("    pass")
            sync_lines.append("    pass")
        package_dir = Path(__file__).parent
        (package_dir / "async_client.pyi").write_text("\n".join(async_lines) + "\n", encoding="utf-8")
        (package_dir / "sync_client.pyi").write_text("\n".join(sync_lines) + "\n", encoding="utf-8")

    @staticmethod
    def _validate_call(definition: dict[str, Any], args: tuple[Any, ...], kwargs: dict[str, Any]) -> None:
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


def _runtime_type(type_name: str) -> type[Any] | None:
    return {"bool": bool, "bytes": bytes, "float": float, "int": int, "str": str}.get(type_name)


def _call_error(command: str, message: str) -> str:
    if message.startswith("missing a required argument: "):
        name = message.removeprefix("missing a required argument: ")
        return f"{command}() missing 1 required positional argument: {name}"
    if message == "too many positional arguments":
        return f"{command}() takes fewer positional arguments than were given"
    if message.startswith("got an unexpected keyword argument"):
        return f"{command}() {message}"
    if message.startswith("multiple values for argument"):
        return f"{command}() got {message}"
    return f"{command}() {message}"
