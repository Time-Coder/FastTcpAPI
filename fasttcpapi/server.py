"""Application and dispatch lifecycle."""

from __future__ import annotations

import asyncio
import functools
import inspect
from dataclasses import dataclass
from collections.abc import AsyncIterator, Hashable
from typing import Any, Callable, Dict, List, Optional, Set, Type, TypeVar, Union, get_type_hints

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
    timeout: Union[float, List[float]]


@dataclass(frozen=True)
class PushRoute:
    command: Hashable
    handler: Handler


class Server:
    def __init__(self, frame_type: Type[Frame] = JsonLengthPrefixFrame) -> None:
        if not issubclass(frame_type, Frame):
            raise TypeError("frame_type must be a Frame subclass")
        self.frame_type = frame_type
        self._routes: Dict[Hashable, Route] = {}
        self._push_routes: List[PushRoute] = []
        self._server: Optional[asyncio.AbstractServer] = None
        self._writers: Set[asyncio.StreamWriter] = set()
        self._client_tasks: Set[asyncio.Task] = set()
        self._on_push: List[Callable[..., Any]] = []
        self._on_request: List[Callable[..., Any]] = []
        self._on_response: List[Callable[..., Any]] = []
        self._on_start: List[Callable[..., Any]] = []
        self._on_close: List[Callable[..., Any]] = []
        self._on_client_connected: List[Callable[..., Any]] = []
        self._on_client_disconnected: List[Callable[..., Any]] = []

    def _decorator(self, callbacks: List[Callable[..., Any]], callback: Optional[Callable[..., Any]] = None):
        def register(function):
            callbacks.append(function)
            return function
        return register if callback is None else register(callback)

    def on_request(self, function=None): return self._decorator(self._on_request, function)
    def on_response(self, function=None): return self._decorator(self._on_response, function)
    def on_push(self, function=None): return self._decorator(self._on_push, function)
    def on_start(self, function=None): return self._decorator(self._on_start, function)
    def on_close(self, function=None): return self._decorator(self._on_close, function)
    def on_client_connected(self, function=None): return self._decorator(self._on_client_connected, function)
    def on_client_disconnected(self, function=None): return self._decorator(self._on_client_disconnected, function)

    def add_push_callback(self, callback: Callable[..., Any]) -> None:
        self._on_push.append(callback)

    def add_request_callback(self, callback: Callable[..., Any]) -> None:
        self._on_request.append(callback)

    def add_response_callback(self, callback: Callable[..., Any]) -> None:
        self._on_response.append(callback)

    def add_start_callback(self, callback): self._on_start.append(callback)
    def add_close_callback(self, callback): self._on_close.append(callback)
    def add_client_connected_callback(self, callback): self._on_client_connected.append(callback)
    def add_client_disconnected_callback(self, callback): self._on_client_disconnected.append(callback)

    async def _callbacks(self, callbacks: List[Callable[..., Any]], *args: Any) -> None:
        for callback in callbacks:
            try:
                value = callback(*args)
                if inspect.isawaitable(value):
                    await value
            except Exception:
                continue

    def command(self, name: Optional[Hashable] = None, *, response_frames: int = 1,
                timeout: Union[float, List[float]] = 30.0) -> Callable[[F], F]:
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
            value = timeout if isinstance(timeout, list) else float(timeout)
            if isinstance(value, list):
                if len(value) != response_frames or any(float(item) <= 0 for item in value):
                    raise ValueError("timeout list must match response_frames and contain positive values")
                value = [float(item) for item in value]
            elif value <= 0:
                raise ValueError("timeout must be positive")
            self._routes[command_name] = Route(handler, response_frames, value)
            return handler
        return decorator

    def push(self, name: Optional[Hashable] = None) -> Callable[[F], F]:
        """Register a handler that starts once for every connected client.

        The handler may return a value, a generator, or an async generator.
        Every produced value is encoded and sent as an unsolicited push frame.
        """
        def decorator(handler: F) -> F:
            command = handler.__name__ if name is None else name
            if not isinstance(command, Hashable) or command == "":
                raise ValueError("push command must be a non-empty hashable value")
            if command == SCHEMA_COMMAND or command in self._routes:
                raise ValueError(f"command already registered: {command}")
            if any(route.command == command for route in self._push_routes):
                raise ValueError(f"push command already registered: {command}")
            self._push_routes.append(PushRoute(command, handler))
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

        loop = asyncio.get_running_loop()
        if inspect.iscoroutinefunction(handler) or inspect.isasyncgenfunction(handler):
            value = handler(*frame.args, **frame.kwargs)
        else:
            value = await loop.run_in_executor(
                None, functools.partial(handler, *frame.args, **frame.kwargs)
            )
        if inspect.isawaitable(value):
            value = await value
        if inspect.isasyncgen(value):
            async for item in value:
                yield item
        elif inspect.isgenerator(value):
            while True:
                item = await loop.run_in_executor(None, _next_or_end, value)
                if item is _GENERATOR_END:
                    break
                yield item
        else:
            yield value

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        if task is not None:
            self._client_tasks.add(task)
        self._writers.add(writer)
        await self._callbacks(self._on_client_connected, reader, writer)
        writer_lock = asyncio.Lock()
        push_tasks = [asyncio.create_task(self._run_push(route, writer, writer_lock)) for route in self._push_routes]
        request_tasks: Set[asyncio.Task] = set()
        try:
            while True:
                frame = self.frame_type()
                try:
                    await frame.decode_from_reader(reader)
                    await self._callbacks(self._on_request, frame)
                    task = asyncio.create_task(self._handle_request(frame, writer, writer_lock))
                    request_tasks.add(task)
                    task.add_done_callback(request_tasks.discard)
                except (asyncio.IncompleteReadError, ConnectionError, OSError):
                    break
        finally:
            await self._callbacks(self._on_client_disconnected, reader, writer)
            if task is not None:
                self._client_tasks.discard(task)
            self._writers.discard(writer)
            for task in request_tasks:
                task.cancel()
            if request_tasks:
                await asyncio.gather(*request_tasks, return_exceptions=True)
            for task in push_tasks:
                task.cancel()
            if push_tasks:
                await asyncio.gather(*push_tasks, return_exceptions=True)
            writer.close()
            # Do not await ProactorStreamWriter.wait_closed() here. When the
            # peer has already reset the socket, Windows reports that reset
            # through the close waiter and asyncio logs it as an unhandled
            # client_connected_cb exception even if the await is caught.

    async def _handle_request(self, frame: Frame, writer: asyncio.StreamWriter,
                              writer_lock: asyncio.Lock) -> None:
        try:
            if isinstance(frame, JsonLengthPrefixFrame) and frame.command == SCHEMA_COMMAND:
                await self._send_default_result(frame, writer, self.command_schema())
                return
            route = self._routes.get(frame.command)
            if route is None:
                raise CommandError(f"unknown command: {frame.command}", code="not_found")
            sent = 0
            async for result in self.dispatch(frame):
                if sent == route.response_frames:
                    break
                frame.decode_from_result(result, frame)
                await self._callbacks(self._on_response, frame)
                async with writer_lock:
                    writer.write(frame.encode())
                    await writer.drain()
                sent += 1
            if sent != route.response_frames:
                raise RuntimeError(
                    f"command {frame.command!r} produced {sent} response frames; "
                    f"expected {route.response_frames}"
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            frame.decode_from_exception(exc, frame)
            encoded = frame.encode()
            await self._callbacks(self._on_response, frame)
            if encoded:
                async with writer_lock:
                    writer.write(encoded)
                    await writer.drain()

    async def _run_push(
        self, route: PushRoute, writer: asyncio.StreamWriter, writer_lock: asyncio.Lock
    ) -> None:
        frame = self.frame_type()
        try:
            loop = asyncio.get_running_loop()
            if inspect.iscoroutinefunction(route.handler) or inspect.isasyncgenfunction(route.handler):
                value = route.handler()
            else:
                value = await loop.run_in_executor(None, route.handler)
            if inspect.isawaitable(value):
                value = await value
            if inspect.isasyncgen(value):
                async for item in value:
                    await self._send_push(route.command, item, frame, writer, writer_lock)
            elif inspect.isgenerator(value):
                while True:
                    item = await loop.run_in_executor(None, _next_or_end, value)
                    if item is _GENERATOR_END:
                        break
                    await self._send_push(route.command, item, frame, writer, writer_lock)
            else:
                await self._send_push(route.command, value, frame, writer, writer_lock)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            frame.decode_from_exception(exc, frame)
            frame.command = route.command
            await self._callbacks(self._on_push, frame)
            async with writer_lock:
                writer.write(frame.encode())
                await writer.drain()

    async def _send_push(
        self,
        command: Hashable,
        result: Any,
        frame: Frame,
        writer: asyncio.StreamWriter,
        writer_lock: asyncio.Lock,
    ) -> None:
        frame.decode_from_result(result, frame)
        frame.command = command
        await self._callbacks(self._on_push, frame)
        async with writer_lock:
            writer.write(frame.encode())
            await writer.drain()

    async def start(self, host: str = "127.0.0.1", port: int = 8000) -> asyncio.AbstractServer:
        """Start listening and return the asyncio server handle.

        This is useful when embedding the server in an existing event loop or
        when tests need to reserve an ephemeral port.
        """
        if self._server is not None:
            return self._server
        self._server = await asyncio.start_server(self._handle_client, host, port)
        await self._callbacks(self._on_start, self)
        return self._server

    async def serve(self, host: str = "127.0.0.1", port: int = 8000) -> None:
        """Start the server and run until it is closed."""
        server = await self.start(host, port)
        async with server:
            await server.serve_forever()

    def exec(self, host: str = "127.0.0.1", port: int = 8000):
        asyncio.run(self.serve(host=host, port=port))

    async def close(self) -> None:
        if self._server is not None:
            self._server.close()
            self._server = None
        current = asyncio.current_task()
        tasks = [task for task in self._client_tasks if task is not current]
        for task in tasks:
            task.cancel()
        writers = list(self._writers)
        self._writers.clear()
        for writer in writers:
            writer.close()
            transport = getattr(writer, "transport", None)
            if transport is not None:
                transport.abort()
        await self._callbacks(self._on_close, self)

    @staticmethod
    def _params_for(handler: Handler) -> List[Param]:
        signature = inspect.signature(handler)
        try:
            annotations = get_type_hints(handler)
        except (NameError, TypeError):
            annotations = {}
        params: List[Param] = []
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
        frame.decode_from_result(result, frame)
        writer.write(frame.encode())
        await writer.drain()

    def command_schema(self) -> List[Dict[str, Any]]:
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
                "timeout": route.timeout,
            })
        for route in self._push_routes:
            if isinstance(route.command, str):
                definitions.append({
                    "command": route.command,
                    "parameters": [],
                    "push": True,
                })
        return definitions

_GENERATOR_END = object()


def _next_or_end(iterator: Any) -> Any:
    try:
        return next(iterator)
    except StopIteration:
        return _GENERATOR_END


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
