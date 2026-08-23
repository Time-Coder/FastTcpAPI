"""FastAPI-inspired request dispatch for framed TCP servers."""

from .server import Server
from .binary import decode_typed_arguments
from .client import Client
from .default_frame import JsonLengthPrefixFrame
from .exceptions import CommandError, RemoteError
from .frame import Frame, Param

FastTcpAPI = Server

__all__ = [
    "Client", "CommandError", "FastTcpAPI", "Server", "Frame", "JsonLengthPrefixFrame",
    "Param", "RemoteError", "decode_typed_arguments",
]
