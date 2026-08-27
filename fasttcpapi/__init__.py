"""FastAPI-inspired request dispatch for framed TCP servers."""

from .server import Server
from .binary import decode_typed_arguments
from .client import Client
from .json_frame import JsonFrame
from .exceptions import CommandError, RemoteError
from .frame import Frame, Param

FastTcpAPI = Server

__all__ = [
    "Client", "CommandError", "FastTcpAPI", "Server", "Frame", "JsonFrame",
    "Param", "RemoteError", "decode_typed_arguments",
]
