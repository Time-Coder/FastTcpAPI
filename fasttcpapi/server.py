"""Application and dispatch lifecycle."""

from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass
from collections.abc import AsyncIterator, Callable, Hashable
from typing import Any, TypeVar, get_type_hints

from .default_frame import JsonLengthPrefixFrame
from .exceptions import CommandError
from .frame import Frame, Param

Handler = Callable[..., Any]
F = TypeVar("F", bound=Handler)
SCHEMA_COMMAND = "__fasttcpapi__.schema"


@dataclass(frozen=True)
class Route:
    handler: Handler
    response_frames: int


class Server:
    def __init__(self, frame_type: type[Frame] = JsonLengthPrefixFrame) -> None:
        if not issubclass(frame_type, Frame):
            raise TypeError("frame_type must be a Frame subclass")
        self.frame_type = frame_type
        self._routes: dict[Hashable, Route] = {}
        self._server: asyncio.AbstractServer | None = None

    def command(self, name: Hashable | None = None, *, response_frames: int = 1) -> Callable[[F], F]:
        """Register a command handler, like ``FastAPI.get`` registers an endpoint."""
        if not isinstance(response_frames, int) or isinstance(response_frames, bool) or response_frames < 1:
            raise ValueError("response_frames must be a positive integer")

        def decorator(handler: F) -> F:
            command_name = handler.__name__ if name is None else name
            if not isinstance(command_name, Hashable):
                raise TypeError("command name must be hashable")
            if command_name == "":
                raise ValueError("command name cannot be empty")
            if command_name == SCHEMA_COMMAND:
                raise ValueError(f"{SCHEMA_COMMAND} is reserved")
            if command_name in self._routes:
                raise ValueError(f"command already registered: {command_name}")
            self._routes[command_name] = Route(handler, response_frames)
            return handler
        return decorator

    async def dispatch(self, frame: Frame) -> AsyncIterator[Any]:
        """Invoke one request and yield each value it produces."""
        route = self._routes.get(frame.command)
        if route is None:
            raise CommandError(f"unknown command: {frame.command}", code="not_found")
        handler = route.handler
        try:
            frame.parse_args(self._params_for(handler))
            inspect.signature(handler).bind(*frame.args, **frame.kwargs)
        except TypeError as exc:
            raise CommandError(str(exc), code="invalid_arguments") from exc

        value = handler(*frame.args, **frame.kwargs)
        if inspect.isawaitable(value):
            value = await value
        if inspect.isasyncgen(value):
            async for item in value:
                yield item
        elif inspect.isgenerator(value):
            for item in value:
                yield item
        else:
            yield value

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                frame = self.frame_type()
                try:
                    await frame.decode_from_reader(reader)
                    if isinstance(frame, JsonLengthPrefixFrame) and frame.command == SCHEMA_COMMAND:
                        await self._send_default_result(frame, writer, self.command_schema())
                    else:
                        route = self._routes.get(frame.command)
                        assert route is not None
                        sent = 0
                        async for result in self.dispatch(frame):
                            if sent == route.response_frames:
                                break
                            frame.decode_from_result(result)
                            writer.write(frame.encode())
                            await writer.drain()
                            sent += 1
                        if sent != route.response_frames:
                            raise RuntimeError(
                                f"command {frame.command!r} produced {sent} response frames; "
                                f"expected {route.response_frames}"
                            )
                except asyncio.IncompleteReadError:
                    break
                except Exception as exc:
                    frame.decode_from_exception(exc)
                    encoded = frame.encode()
                    if encoded:
                        writer.write(encoded)
                        await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    async def serve(self, host: str = "127.0.0.1", port: int = 8000) -> None:
        self._server = await asyncio.start_server(self._handle_client, host, port)
        async with self._server:
            await self._server.serve_forever()

    def exec(self, host: str = "127.0.0.1", port: int = 8000):
        asyncio.run(self.serve(host=host, port=port))

    async def close(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    @staticmethod
    def _params_for(handler: Handler) -> list[Param]:
        signature = inspect.signature(handler)
        try:
            annotations = get_type_hints(handler)
        except (NameError, TypeError):
            annotations = {}
        params: list[Param] = []
        for parameter in signature.parameters.values():
            if parameter.kind not in {
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                inspect.Parameter.KEYWORD_ONLY,
            }:
                raise CommandError(
                    f"unsupported handler parameter kind: {parameter.name}", code="invalid_arguments"
                )
            param_type = annotations.get(parameter.name, parameter.annotation)
            params.append(Param(parameter.name, param_type))
        return params

    async def _send_default_result(
        self, frame: JsonLengthPrefixFrame, writer: asyncio.StreamWriter, result: Any
    ) -> None:
        frame.decode_from_result(result)
        writer.write(frame.encode())
        await writer.drain()

    def command_schema(self) -> list[dict[str, Any]]:
        """Return serializable definitions for the built-in dynamic client."""
        definitions = []
        for command, route in self._routes.items():
            if not isinstance(command, str):
                continue
            handler = route.handler
            signature = inspect.signature(handler)
            params_by_name = {param.name: param for param in self._params_for(handler)}
            parameters = []
            for parameter in signature.parameters.values():
                if parameter.kind not in {
                    inspect.Parameter.POSITIONAL_ONLY,
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                    inspect.Parameter.KEYWORD_ONLY,
                }:
                    continue
                parameter_type = params_by_name[parameter.name].type
                parameters.append({
                    "name": parameter.name,
                    "type": _type_name(parameter_type),
                    "has_default": parameter.default is not inspect.Parameter.empty,
                    "default": _schema_default(parameter.default),
                    "kind": parameter.kind.name,
                })
            definitions.append({
                "command": command,
                "parameters": parameters,
                "response_frames": route.response_frames,
            })
        return definitions


def _type_name(annotation: Any) -> str:
    if annotation is inspect.Parameter.empty:
        return "Any"
    if isinstance(annotation, type):
        return annotation.__name__
    return str(annotation).replace("typing.", "")


def _schema_default(default: Any) -> Any:
    if default is inspect.Parameter.empty:
        return None
    try:
        import json
        json.dumps(default)
    except (TypeError, ValueError):
        return None
    return default
