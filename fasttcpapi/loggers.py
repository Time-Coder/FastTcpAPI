"""Logging hooks for server and client lifecycle events."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .client import Client
    from .client_connection import ClientConnection
    from .frame import Frame
    from .server import Server


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
        self._logger = logging.getLogger("fasttcpapi.server")
        self._logger.setLevel(logging.INFO)
        if not self._logger.handlers:
            self._logger.addHandler(logging.StreamHandler())

    def on_start(self) -> None:
        self._logger.info("server started at: %s", self.server.address)

    def on_close(self) -> None:
        self._logger.info("server closed")

    def on_client_connected(self, client: ClientConnection) -> None:
        self._logger.info("client connected: %s", client.address)

    def on_client_disconnected(self, client: ClientConnection) -> None:
        self._logger.info("client disconnected: %s", client.address)

    def on_request(self, client: ClientConnection, frame: Frame) -> None:
        self._logger.info("request received from %s: command=%r args=%r kwargs=%r raw_data=%r",
                          client.address, frame.command, frame.args, frame.kwargs, frame.raw_data)

    def on_response(self, client: ClientConnection, frame: Frame) -> None:
        self._logger.info("response sent to %s: command=%r args=%r kwargs=%r raw_data=%r",
                          client.address, frame.command, frame.args, frame.kwargs, frame.raw_data)

    def on_push(self, client: ClientConnection, frame: Frame) -> None:
        self._logger.info("push sent to %s: command=%r args=%r kwargs=%r raw_data=%r",
                          client.address, frame.command, frame.args, frame.kwargs, frame.raw_data)


class DefaultClientLogger(ClientLogger):
    def __init__(self, client: Client) -> None:
        super().__init__(client)
        self._logger = logging.getLogger("fasttcpapi.client")
        self._logger.setLevel(logging.INFO)
        if not self._logger.handlers:
            self._logger.addHandler(logging.StreamHandler())

    def on_connected(self) -> None:
        self._logger.info("connected to %s:%s", self.client.server_host, self.client.server_port)

    def on_disconnected(self) -> None:
        self._logger.info("disconnected from %s:%s", self.client.server_host, self.client.server_port)

    def on_retry_connect(self) -> None:
        self._logger.info("retrying connection to %s:%s", self.client.server_host, self.client.server_port)

    def on_request(self, frame: Frame) -> None:
        self._logger.info("request sent: command=%r args=%r kwargs=%r raw_data=%r",
                          frame.command, frame.args, frame.kwargs, frame.raw_data)

    def on_response(self, frame: Frame) -> None:
        self._logger.info("response received: command=%r args=%r kwargs=%r raw_data=%r",
                          frame.command, frame.args, frame.kwargs, frame.raw_data)

    def on_push(self, frame: Frame) -> None:
        self._logger.info("push received: command=%r args=%r kwargs=%r raw_data=%r",
                          frame.command, frame.args, frame.kwargs, frame.raw_data)
