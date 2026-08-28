"""Route declarations and per-connection state."""

from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Hashable
from typing import Any, Callable, Dict, List, Optional, TypeVar, Union

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


class Router:
    """Collect command and push routes for inclusion in a server."""

    def __init__(self) -> None:
        self._routes: Dict[Hashable, Route] = {}
        self._push_routes: List[PushRoute] = []

    def command(self, name: Optional[Hashable] = None, *, response_frames: int = 1,
                timeout: Union[float, List[float]] = 30.0) -> Callable[[F], F]:
        if not isinstance(response_frames, int) or isinstance(response_frames, bool) or response_frames < 1:
            raise ValueError("response_frames must be a positive integer")
        def decorator(handler: F) -> F:
            command_name = handler.__name__ if name is None else name
            if not isinstance(command_name, Hashable) or command_name == "":
                raise ValueError("command name must be a non-empty hashable value")
            if command_name == SCHEMA_COMMAND or command_name in self._routes:
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
        def decorator(handler: F) -> F:
            command = handler.__name__ if name is None else name
            if not isinstance(command, Hashable) or command == "":
                raise ValueError("push command must be a non-empty hashable value")
            if command == SCHEMA_COMMAND or command in self._routes or any(r.command == command for r in self._push_routes):
                raise ValueError(f"command already registered: {command}")
            self._push_routes.append(PushRoute(command, handler))
            return handler
        return decorator

    def include_router(self, router: Router) -> None:
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
