"""Application and dispatch lifecycle."""

from __future__ import annotations

import asyncio
import functools
import inspect
from collections.abc import AsyncIterator, Hashable
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Type, TypeVar, Union, get_type_hints

from .json_frame import JsonFrame
from .exceptions import CommandError
from .frame import Frame, Param
from .loggers import DefaultServerLogger, ServerLogger
from .router import Route, PushRoute, Router, SCHEMA_COMMAND
from .client_connection import ClientConnection

Handler = Callable[..., Any]
F = TypeVar("F", bound=Handler)
class Server:
    def __init__(self, frame_type: Type[Frame] = JsonFrame, *, strict_type_check: bool = True,
                 logger: Optional[Type[ServerLogger]] = None) -> None:
        if not issubclass(frame_type, Frame):
            raise TypeError("frame_type must be a Frame subclass")
        self.frame_type = frame_type
        self.strict_type_check = strict_type_check
        if logger is None:
            self.logger = DefaultServerLogger(self)
        elif isinstance(logger, type) and issubclass(logger, ServerLogger):
            self.logger = logger(self)
        else:
            raise TypeError("logger must be a ServerLogger subclass")
        self._routes: Dict[Hashable, Route] = {}
        self._push_routes: List[PushRoute] = []
        self._server: Optional[asyncio.AbstractServer] = None
        self._address: Optional[Tuple[Any, ...]] = None
        self._writers: Set[asyncio.StreamWriter] = set()
        self._clients: Set[ClientConnection] = set()
        self._client_tasks: Set[asyncio.Task] = set()
        self._on_push: List[Callable[..., Any]] = []
        self._on_request: List[Callable[..., Any]] = []
        self._on_response: List[Callable[..., Any]] = []
        self._on_start: List[Callable[..., Any]] = []
        self._on_close: List[Callable[..., Any]] = []
        self._on_client_connected: List[Callable[..., Any]] = []
        self._on_client_disconnected: List[Callable[..., Any]] = []

    @property
    def address(self) -> Optional[Tuple[Any, ...]]:
        """Actual listening socket address, or None before start/after close."""
        return self._address

    @property
    def host(self) -> Optional[str]:
        return None if self._address is None else str(self._address[0])

    @property
    def port(self) -> Optional[int]:
        return None if self._address is None else int(self._address[1])

    def include_router(self, router: Router) -> None:
        """Include routes from a Router, including routers nested within it."""
        if not isinstance(router, Router):
            raise TypeError("router must be a Router instance")
        for command, route in router._routes.items():
            if command in self._routes or any(r.command == command for r in self._push_routes):
                raise ValueError(f"command already registered: {command}")
            self._routes[command] = route
        for route in router._push_routes:
            if route.command in self._routes or any(r.command == route.command for r in self._push_routes):
                raise ValueError(f"command already registered: {route.command}")
            self._push_routes.append(route)

    def _decorator(self, callbacks: List[Callable[..., Any]], callback: Optional[Callable[..., Any]] = None):
        def register(function):
            async def decorated(*args: Any) -> Any:
                value = function(*args[1:])
                if inspect.isawaitable(value):
                    return await value
                return value
            callbacks.append(decorated)
            return function
        return register if callback is None else register(callback)

    def on_request(self, function: Optional[Callable[..., Any]] = None) -> Callable[..., Any]: return self._decorator(self._on_request, function)
    def on_response(self, function: Optional[Callable[..., Any]] = None) -> Callable[..., Any]: return self._decorator(self._on_response, function)
    def on_push(self, function: Optional[Callable[..., Any]] = None) -> Callable[..., Any]: return self._decorator(self._on_push, function)
    def on_start(self, function: Optional[Callable[..., Any]] = None) -> Callable[..., Any]: return self._decorator(self._on_start, function)
    def on_close(self, function: Optional[Callable[..., Any]] = None) -> Callable[..., Any]: return self._decorator(self._on_close, function)
    def on_client_connected(self, function: Optional[Callable[..., Any]] = None) -> Callable[..., Any]: return self._decorator(self._on_client_connected, function)
    def on_client_disconnected(self, function: Optional[Callable[..., Any]] = None) -> Callable[..., Any]: return self._decorator(self._on_client_disconnected, function)

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

    async def _logger_call(self, name: str, *args: Any) -> None:
        try:
            value = getattr(self.logger, name)(*args)
            if inspect.isawaitable(value):
                await value
        except Exception:
            pass

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
            params = self._params_for(handler)
            frame.parse_args(params)
            bound = inspect.signature(handler).bind(*frame.args, **frame.kwargs)
            self._coerce_bound_arguments(bound, params)
            frame.args = tuple(bound.args)
            frame.kwargs = dict(bound.kwargs)
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
        client = ClientConnection(reader, writer)
        self._writers.add(writer)
        self._clients.add(client)
        await self._logger_call("on_client_connected", client)
        await self._callbacks(self._on_client_connected, self, client)
        writer_lock = asyncio.Lock()
        push_tasks = [asyncio.create_task(self._run_push(route, writer, writer_lock, client)) for route in self._push_routes]
        request_tasks: Set[asyncio.Task] = set()
        response_tail = asyncio.get_running_loop().create_future()
        response_tail.set_result(None)
        try:
            while True:
                frame = self.frame_type()
                try:
                    await frame.decode(reader)
                    await self._logger_call("on_request", client, frame)
                    await self._callbacks(self._on_request, self, client, frame)
                    if frame.session_id is None:
                        previous = response_tail
                        response_tail = asyncio.get_running_loop().create_future()
                    else:
                        previous = asyncio.get_running_loop().create_future()
                        previous.set_result(None)
                    task = asyncio.create_task(self._handle_request(
                        frame, writer, writer_lock, previous, response_tail, client
                    ))
                    request_tasks.add(task)
                    task.add_done_callback(request_tasks.discard)
                except (asyncio.IncompleteReadError, ConnectionError, OSError):
                    break
                except Exception:
                    # Frame codecs may reject corrupt data and resynchronise on
                    # their next decode attempt. Keep this client connection alive.
                    if reader.at_eof():
                        break
                    continue
        finally:
            await self._callbacks(self._on_client_disconnected, self, client)
            await self._logger_call("on_client_disconnected", client)
            if task is not None:
                self._client_tasks.discard(task)
            self._writers.discard(writer)
            self._clients.discard(client)
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
                              writer_lock: asyncio.Lock, previous: asyncio.Future,
                              complete: asyncio.Future, client: ClientConnection) -> None:
        try:
            if frame.command is None:
                raise ValueError("frame.command must be set")
            if isinstance(frame, JsonFrame) and frame.command == SCHEMA_COMMAND:
                await self._send_default_result(frame, writer, self.command_schema(), client)
                return
            route = self._routes.get(frame.command)
            if route is None:
                raise CommandError(f"unknown command: {frame.command}", code="not_found")
            values = []
            sent = 0
            async for result in self.dispatch(frame):
                if sent == route.response_frames:
                    break
                values.append(result)
                sent += 1
            await previous
            sent = 0
            for result in values:
                frame.set_result(result, frame)
                frame.validate()
                await self._callbacks(self._on_response, self, client, frame)
                await self._logger_call("on_response", client, frame)
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
            await previous
            frame.set_exception(exc, frame)
            frame.validate()
            encoded = frame.encode()
            await self._callbacks(self._on_response, self, client, frame)
            await self._logger_call("on_response", client, frame)
            if encoded:
                async with writer_lock:
                    writer.write(encoded)
                    await writer.drain()
        finally:
            if not complete.done():
                complete.set_result(None)

    async def _run_push(
        self, route: PushRoute, writer: asyncio.StreamWriter, writer_lock: asyncio.Lock,
        client: ClientConnection,
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
                    await self._send_push(route.command, item, frame, writer, writer_lock, client)
            elif inspect.isgenerator(value):
                while True:
                    item = await loop.run_in_executor(None, _next_or_end, value)
                    if item is _GENERATOR_END:
                        break
                    await self._send_push(route.command, item, frame, writer, writer_lock, client)
            else:
                await self._send_push(route.command, value, frame, writer, writer_lock, client)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            frame.set_exception(exc, frame)
            frame.command = route.command
            frame.validate()
            await self._callbacks(self._on_push, self, client, frame)
            await self._logger_call("on_push", client, frame)
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
        client: ClientConnection,
    ) -> None:
        frame.set_result(result, frame)
        frame.command = command
        if isinstance(frame, JsonFrame):
            frame.args = (result,)
            frame.kwargs = {}
        frame.validate()
        await self._callbacks(self._on_push, self, client, frame)
        await self._logger_call("on_push", client, frame)
        async with writer_lock:
            frame.validate()
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
        if self._server.sockets:
            self._address = self._server.sockets[0].getsockname()
        await self._callbacks(self._on_start, self)
        await self._logger_call("on_start")
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
            self._address = None
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
        await self._logger_call("on_close")

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
            has_default = parameter.default is not inspect.Parameter.empty
            params.append(Param(parameter.name, param_type, has_default, 
                                parameter.default if has_default else None))
        binary_types = (bytes, bytearray)
        for index, param in enumerate(params):
            if param.type in binary_types and index != len(params) - 1:
                raise CommandError(
                    f"bytes/bytearray parameter must be last: {param.name}",
                    code="invalid_arguments",
                )
        return params

    def _coerce_bound_arguments(self, bound: inspect.BoundArguments, params: List[Param]) -> None:
        expected_by_name = {param.name: param.type for param in params}
        for name, value in list(bound.arguments.items()):
            expected = expected_by_name.get(name, inspect.Parameter.empty)
            if expected in (inspect.Parameter.empty, Any, None):
                continue
            if not isinstance(expected, type):
                continue
            if type(value) is expected:
                continue
            if self.strict_type_check:
                raise TypeError(
                    f"argument '{name}' must be {expected.__name__}, not {type(value).__name__}"
                )
            try:
                bound.arguments[name] = expected(value)
            except (TypeError, ValueError, OverflowError) as exc:
                raise TypeError(
                    f"argument '{name}' could not be converted to {expected.__name__}"
                ) from exc

    async def _send_default_result(
        self, frame: JsonFrame, writer: asyncio.StreamWriter, result: Any,
        client: ClientConnection,
    ) -> None:
        frame.set_result(result, frame)
        frame.validate()
        await self._callbacks(self._on_response, self, client, frame)
        await self._logger_call("on_response", client, frame)
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
