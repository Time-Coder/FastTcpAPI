"""Built-in 4-byte length-prefixed JSON frame."""

from __future__ import annotations

import asyncio
import builtins
import json
import struct
import traceback
from typing import Any, Dict, List

from .exceptions import RemoteError
from .frame import Frame, Param


class JsonFrame(Frame):
    """uint32 big-endian JSON length, followed by a UTF-8 JSON object."""

    max_frame_size = 16 * 1024 * 1024

    async def decode(self, reader: asyncio.StreamReader) -> None:
        size = struct.unpack("!I", await reader.readexactly(4))[0]
        if size > self.max_frame_size:
            raise ValueError(f"frame exceeds {self.max_frame_size} byte limit")
        try:
            if size < 1:
                raise ValueError("frame payload is empty")
            first = await reader.readexactly(1)
            if first != b"{":
                raise ValueError("JSON frame payload must start with '{'")
            payload = json.loads((first + await reader.readexactly(size - 1)).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("frame is not valid UTF-8 JSON") from exc
        if not isinstance(payload, dict):
            raise ValueError("request must be a JSON object")
        self.command = payload.get("command")
        self.session_id = payload.get("session_id")
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
        if self.command is None:
            raise ValueError("frame.command must be set before encoding")
        payload: Dict[str, Any] = {"command": self.command, "args": self.args, "kwargs": self.kwargs,
                                   "session_id": self.session_id}
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        return struct.pack("!I", len(body)) + body

    def set_result(self, result: Any, request: Frame) -> None:
        self.session_id = request.session_id
        self.command = "response"
        self.args = ()
        self.kwargs = {"success": True, "data": result}

    def set_exception(self, exception: Exception, request: Frame) -> None:
        self.session_id = request.session_id
        self.command = "response"
        self.args = ()
        error: Dict[str, Any] = {
            "success": False,
            "data": list(exception.args),
            "exception": type(exception).__qualname__,
            "traceback": traceback.format_exc(),
        }
        try:
            json.dumps(error["data"])
        except (TypeError, ValueError):
            error["data"] = [str(value) for value in exception.args]
        self.kwargs = error

    def result(self) -> Any:
        if self.command == "response":
            if self.kwargs.get("success") is True:
                return self.kwargs.get("data")
            if self.kwargs.get("success") is False:
                exception_name = self.kwargs.get("exception")
                exception_args = self.kwargs.get("data", [])
                exception_type = None
                if isinstance(exception_name, str) and isinstance(exception_args, list):
                    exception_type = getattr(builtins, exception_name, None)
                if (
                    isinstance(exception_type, type)
                    and issubclass(exception_type, Exception)
                    and exception_type.__module__ == "builtins"
                ):
                    try:
                        restored = exception_type(*exception_args)
                    except Exception:
                        restored = None
                    if isinstance(restored, Exception):
                        raise restored
                raise RemoteError(
                    exception_name or "RemoteError",
                    self.kwargs.get("traceback", "remote error"),
                )
        raise ValueError(f"frame is not a result response: {self.command!r}")
