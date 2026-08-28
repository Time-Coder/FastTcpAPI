"""FastAPI-inspired request dispatch for framed TCP servers."""

from .server import Router, Server
from .client_connection import ClientConnection
from .binary import decode_typed_arguments
from .client import Client
from .json_frame import JsonFrame
from .exceptions import CommandError, RemoteError
from .frame import Frame, Param
from .loggers import ClientLogger, DefaultClientLogger, DefaultServerLogger, ServerLogger

FastTcpAPI = Server

__all__ = [
    "Client", "ClientConnection", "CommandError", "FastTcpAPI", "Router", "Server", "Frame", "JsonFrame",
    "Param", "RemoteError", "decode_typed_arguments", "ServerLogger", "ClientLogger",
    "DefaultServerLogger", "DefaultClientLogger",
]
