"""Built-in 4-byte length-prefixed JSON frame."""

from __future__ import annotations

import asyncio
import builtins
import json
import struct
import itertools
from typing import Any, Dict, List

from .exceptions import RemoteError
from .frame import Frame, Param


class JsonLengthPrefixFrame(Frame):
    """uint32 big-endian JSON length, followed by a UTF-8 JSON object."""

    max_frame_size = 16 * 1024 * 1024
    _session_ids = itertools.count(1)

    def __init__(self) -> None:
        super().__init__()
        self.session_id = next(self._session_ids)

    async def decode_from_reader(self, reader: asyncio.StreamReader) -> None:
        size = struct.unpack("!I", await reader.readexactly(4))[0]
        if size > self.max_frame_size:
            raise ValueError(f"frame exceeds {self.max_frame_size} byte limit")
        try:
            payload = json.loads((await reader.readexactly(size)).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("frame is not valid UTF-8 JSON") from exc
        if not isinstance(payload, dict):
            raise ValueError("request must be a JSON object")
        self.command = payload.get("command")
        self.session_id = payload.get("session_id", 0)
        self._raw_args = payload.get("args", [])
        self._raw_kwargs = payload.get("kwargs", {})

    def parse_args(self, param_list: List[Param]) -> None:
        if not isinstance(self.command, str) or not self.command:
            raise ValueError("request.command must be a non-empty string")
        if not isinstance(self._raw_args, list):
            raise ValueError("request.args must be an array")
        if not isinstance(self._raw_kwargs, dict):
            raise ValueError("request.kwargs must be an object")
        self.args = tuple(self._raw_args)
        self.kwargs = self._raw_kwargs

    def encode(self) -> bytes:
        payload: Dict[str, Any] = {"command": self.command, "args": self.args, "kwargs": self.kwargs,
                                   "session_id": self.session_id}
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        return struct.pack("!I", len(body)) + body

    def decode_from_result(self, result: Any, request: Frame) -> None:
        self.session_id = request.session_id
        self.command = "response.ok"
        self.args = (result,)
        self.kwargs = {}

    def decode_from_exception(self, exception: Exception, request: Frame) -> None:
        self.session_id = request.session_id
        self.command = "response.error"
        self.args = ()
        error: Dict[str, Any] = {
            "code": getattr(exception, "code", "internal_error"),
            "message": str(exception) or type(exception).__name__,
            "solution": getattr(exception, "solution", None),
        }
        exception_type = type(exception)
        if exception_type.__module__ == "builtins" and issubclass(exception_type, Exception):
            try:
                json.dumps(list(exception.args))
            except (TypeError, ValueError):
                pass
            else:
                error["builtin_type"] = exception_type.__qualname__
                error["builtin_args"] = list(exception.args)
        self.kwargs = error

    def result(self) -> Any:
        if self.command == "response.ok":
            if len(self.args) != 1:
                raise ValueError("response.ok must contain exactly one result")
            return self.args[0]
        if self.command == "response.error":
            builtin_type = self.kwargs.get("builtin_type")
            builtin_args = self.kwargs.get("builtin_args", [])
            if isinstance(builtin_type, str) and isinstance(builtin_args, list):
                exception_type = getattr(builtins, builtin_type, None)
                if (
                    isinstance(exception_type, type)
                    and issubclass(exception_type, Exception)
                    and exception_type.__module__ == "builtins"
                ):
                    try:
                        restored = exception_type(*builtin_args)
                    except Exception:
                        restored = None
                    if isinstance(restored, Exception):
                        raise restored
            raise RemoteError(
                self.kwargs.get("code", "internal_error"),
                self.kwargs.get("message", "remote error"),
                self.kwargs.get("solution"),
            )
        raise ValueError(f"frame is not a result response: {self.command!r}")
