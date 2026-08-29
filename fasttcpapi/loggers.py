"""Logging hooks for server and client lifecycle events."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .client import Client
    from .client_connection import ClientConnection
    from .frame import Frame
    from .server import Server


_DEFAULT_LOG_FORMAT = "%(asctime)s | %(levelname)s | %(message)s"
_DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def _get_default_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter(_DEFAULT_LOG_FORMAT, datefmt=_DEFAULT_DATE_FORMAT)
        )
        logger.addHandler(handler)
    return logger


class ServerLogger:
    def __init__(self, server: Server) -> None:
        self.server = server

    def on_request(self, client: ClientConnection, frame: Frame) -> None: pass
    def on_response(self, client: ClientConnection, frame: Frame) -> None: pass
    def on_push(self, client: ClientConnection, frame: Frame) -> None: pass
    def on_start(self) -> None: pass
    def on_close(self) -> None: pass
    def on_client_connected(self, client: ClientConnection) -> None: pass
    def on_client_disconnected(self, client: ClientConnection) -> None: pass


class ClientLogger:
    def __init__(self, client: Client) -> None:
        self.client = client
        
    def on_request(self, frame: Frame) -> None: pass
    def on_response(self, frame: Frame) -> None: pass
    def on_push(self, frame: Frame) -> None: pass
    def on_connected(self) -> None: pass
    def on_disconnected(self) -> None: pass
    def on_retry_connect(self) -> None: pass


class DefaultServerLogger(ServerLogger):
    def __init__(self, server: Server) -> None:
        super().__init__(server)
        self._logger = _get_default_logger("fasttcpapi.server")

    def on_start(self) -> None:
        self._logger.info("server started at: %s", self.server.address)

    def on_close(self) -> None:
        self._logger.info("server closed")

    def on_client_connected(self, client: ClientConnection) -> None:
        self._logger.info("client connected: %s", client.address)

    def on_client_disconnected(self, client: ClientConnection) -> None:
        self._logger.info("client disconnected: %s", client.address)

    def on_request(self, client: ClientConnection, frame: Frame) -> None:
        self._logger.info("request received from %s:\ncommand=%r\nargs=%r\nkwargs=%r\nraw_data=%r\n",
                          client.address, frame.command, frame.args, frame.kwargs, frame.raw_data)

    def on_response(self, client: ClientConnection, frame: Frame) -> None:
        self._logger.info("response sent to %s:\ncommand=%r\nargs=%r\nkwargs=%r\nraw_data=%r\n",
                          client.address, frame.command, frame.args, frame.kwargs, frame.raw_data)

    def on_push(self, client: ClientConnection, frame: Frame) -> None:
        self._logger.info("push sent to %s:\ncommand=%r\nargs=%r\nkwargs=%r\nraw_data=%r\n",
                          client.address, frame.command, frame.args, frame.kwargs, frame.raw_data)


class DefaultClientLogger(ClientLogger):
    def __init__(self, client: Client) -> None:
        super().__init__(client)
        self._logger = _get_default_logger("fasttcpapi.client")

    def on_connected(self) -> None:
        self._logger.info("connected to %s", self.client.server_address)

    def on_disconnected(self) -> None:
        self._logger.info("disconnected from %s", self.client.server_address)

    def on_retry_connect(self) -> None:
        self._logger.info("retrying connection to %s", self.client.server_address)

    def on_request(self, frame: Frame) -> None:
        self._logger.info("request sent:\ncommand=%r\nargs=%r\nkwargs=%r\nraw_data=%r\n",
                          frame.command, frame.args, frame.kwargs, frame.raw_data)

    def on_response(self, frame: Frame) -> None:
        self._logger.info("response received:\ncommand=%r\nargs=%r\nkwargs=%r\nraw_data=%r\n",
                          frame.command, frame.args, frame.kwargs, frame.raw_data)

    def on_push(self, frame: Frame) -> None:
        self._logger.info("push received:\ncommand=%r\nargs=%r\nkwargs=%r\nraw_data=%r\n",
                          frame.command, frame.args, frame.kwargs, frame.raw_data)
