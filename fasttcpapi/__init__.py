"""FastAPI-inspired request dispatch for framed TCP servers."""

from .server import Server
from .binary import decode_typed_arguments
from .async_client import AsyncClient
from .default_frame import JsonLengthPrefixFrame
from .exceptions import CommandError, RemoteError
from .sync_client import SyncClient
from .frame import Frame, Param

FastTcpAPI = Server

__all__ = [
    "AsyncClient", "CommandError", "FastTcpAPI", "Server", "Frame", "JsonLengthPrefixFrame",
    "Param", "RemoteError", "SyncClient", "decode_typed_arguments",
]
