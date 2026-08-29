import asyncio
import time

import pytest

from fasttcpapi import Client, RemoteError, Server
from fasttcpapi.frame import Frame, Param


async def _start(server: Server):
    tcp_server = await server.start(port=0)
    tcp_server._fasttcpapi_owner = server
    return tcp_server, tcp_server.sockets[0].getsockname()[1]


async def _stop(client: Client, tcp_server: asyncio.AbstractServer):
    result = client.disconnect(wait=True)
    if result is not None:
        await result
    owner = getattr(tcp_server, "_fasttcpapi_owner", None)
    if owner is not None:
        await owner.close()
    else:
        tcp_server.close()
        await tcp_server.wait_closed()


@pytest.mark.asyncio
async def test_logger_none_disables_framework_logging():
    server = Server(logger=None)

    @server.command("echo")
    def echo(value: str):
        return value

    tcp_server, port = await _start(server)
    client = Client("127.0.0.1", port, logger=None)
    try:
        assert server.logger is None
        assert client.logger is None
        assert await client.async_call("echo", "value") == "value"
    finally:
        await _stop(client, tcp_server)


class LineFrame(Frame):
    async def decode(self, reader):
        command, value = (await reader.readline()).decode().rstrip("\n").split("|", 1)
        self.command = command
        self._value = value

    def parse_args(self, param_list):
        self.args = (self._value,)
        self.kwargs = {}

    def set_result(self, result, request):
        self.command = "ok"
        self.args = (result,)
        self.kwargs = {}

    def set_exception(self, exception, request):
        self.command = "error"
        self.args = (str(exception),)
        self.kwargs = {}

    def result(self):
        if self.command == "ok":
            return self.args[0]
        raise RuntimeError(self.args[0])

    def encode(self):
        value = self.args[0] if self.args else ""
        return ("%s|%s\n" % (self.command, value)).encode()


@pytest.mark.asyncio
async def test_single_and_multiple_response_frames():
    server = Server()

    @server.command("echo")
    def echo(value: str):
        return value

    @server.command("many", response_frames=2, timeout=[0.2, 0.2])
    async def many():
        yield 1
        yield 2

    tcp_server, port = await _start(server)
    client = Client("127.0.0.1", port)
    try:
        assert await client.async_call("echo", "value") == "value"
        assert await client.async_call("many") == [1, 2]
    finally:
        await _stop(client, tcp_server)


@pytest.mark.asyncio
async def test_timeout_uses_command_definition():
    server = Server()

    @server.command("slow", timeout=0.02)
    async def slow():
        await asyncio.sleep(0.1)
        return "late"

    tcp_server, port = await _start(server)
    client = Client("127.0.0.1", port)
    try:
        with pytest.raises(TimeoutError):
            await client.async_call("slow")
    finally:
        await _stop(client, tcp_server)


@pytest.mark.asyncio
async def test_builtin_and_custom_server_exceptions():
    server = Server()

    @server.command("zero")
    def zero():
        raise ZeroDivisionError("division by zero")

    @server.command("custom")
    def custom():
        raise RuntimeError("custom failure")

    tcp_server, port = await _start(server)
    client = Client("127.0.0.1", port)
    try:
        with pytest.raises(ZeroDivisionError, match="division by zero"):
            await client.async_call("zero")
        with pytest.raises(RuntimeError, match="custom failure"):
            await client.async_call("custom")
    finally:
        await _stop(client, tcp_server)


@pytest.mark.asyncio
async def test_argument_validation_and_unknown_command():
    server = Server()

    @server.command("add")
    def add(left: int, right: int):
        return left + right

    tcp_server, port = await _start(server)
    client = Client("127.0.0.1", port)
    try:
        await client.connect()
        await client.wait_for_connected()
        with pytest.raises(AttributeError):
            client.missing
        with pytest.raises(TypeError):
            await client.async_call("add", "1", 2)
        with pytest.raises(TypeError):
            await client.async_call("add", 1)
    finally:
        await _stop(client, tcp_server)


@pytest.mark.asyncio
async def test_push_callbacks_and_bounded_queue():
    server = Server()

    @server.push("numbers")
    def numbers():
        yield 1
        yield 2
        yield 3

    received = []
    tcp_server, port = await _start(server)
    client = Client("127.0.0.1", port, push_queue_size=2)
    client.add_push_callback(lambda frame: received.append(frame.args[0]))
    try:
        await client.connect()
        await asyncio.sleep(0.02)
        first = await client.next_push()
        second = await client.next_push()
        assert [first.args[0], second.args[0]] == [2, 3]
        assert received[-2:] == [2, 3]
    finally:
        await _stop(client, tcp_server)


@pytest.mark.asyncio
async def test_sync_call_and_submit_propagate_results_and_errors():
    server = Server()

    @server.command("echo")
    def echo(value: str):
        return value

    @server.command("broken")
    def broken():
        raise ValueError("broken")

    tcp_server, port = await _start(server)
    client = Client("127.0.0.1", port, sync=True)
    try:
        assert await asyncio.to_thread(client.sync_call, "echo", "sync") == "sync"
        assert await asyncio.wrap_future(client.submit("echo", "submit")) == "submit"
        with pytest.raises(ValueError, match="broken"):
            await asyncio.wrap_future(client.submit("broken"))
    finally:
        await _stop(client, tcp_server)


@pytest.mark.asyncio
async def test_two_clients_receive_only_their_own_responses():
    server = Server()

    @server.command("identity")
    async def identity(value: str):
        await asyncio.sleep(0.01)
        return value

    tcp_server, port = await _start(server)
    first = Client("127.0.0.1", port)
    second = Client("127.0.0.1", port)
    try:
        assert await asyncio.gather(
            first.async_call("identity", "first"),
            second.async_call("identity", "second"),
        ) == ["first", "second"]
    finally:
        await first.disconnect(wait=True)
        await _stop(second, tcp_server)


@pytest.mark.asyncio
async def test_client_reconnects_after_explicit_close():
    server = Server()

    @server.command("echo")
    def echo(value: str):
        return value

    tcp_server, port = await _start(server)
    client = Client("127.0.0.1", port)
    try:
        assert await client.async_call("echo", "first") == "first"
        await client.disconnect(wait=True)
        client.connect()
        assert await client.async_call("echo", "second") == "second"
    finally:
        await _stop(client, tcp_server)


@pytest.mark.asyncio
async def test_synchronous_handler_does_not_block_an_async_request():
    server = Server()

    @server.command("blocking")
    def blocking():
        time.sleep(0.08)
        return "slow"

    @server.command("fast")
    async def fast():
        return "fast"

    tcp_server, port = await _start(server)
    client = Client("127.0.0.1", port)
    try:
        slow = client.async_call("blocking")
        fast = client.async_call("fast")
        assert await slow == "slow"
        assert await fast == "fast"
    finally:
        await _stop(client, tcp_server)


@pytest.mark.asyncio
async def test_one_client_can_be_called_from_multiple_caller_event_loops():
    server = Server()

    @server.command("echo")
    def echo(value: str):
        return value

    tcp_server, port = await _start(server)
    client = Client("127.0.0.1", port)

    def call_from_new_loop(value: str) -> str:
        async def invoke() -> str:
            return await client.async_call("echo", value)
        return asyncio.run(invoke())

    try:
        assert await asyncio.to_thread(call_from_new_loop, "one") == "one"
        assert await asyncio.to_thread(call_from_new_loop, "two") == "two"
    finally:
        await _stop(client, tcp_server)


@pytest.mark.asyncio
async def test_server_close_terminates_a_pending_client_call():
    server = Server()

    @server.command("slow", timeout=1.0)
    async def slow():
        await asyncio.sleep(0.5)
        return "late"

    tcp_server, port = await _start(server)
    server._server = tcp_server
    client = Client("127.0.0.1", port)
    try:
        await client.connect()
        call = client.async_call("slow")
        await asyncio.sleep(0.05)
        await asyncio.wait_for(server.close(), 0.2)
        with pytest.raises((ConnectionError, asyncio.IncompleteReadError, asyncio.CancelledError)):
            await asyncio.wait_for(call, 0.2)
    finally:
        await client.disconnect(wait=True)
def test_client_accepts_manual_service_definition():
    client = Client("127.0.0.1", 1)
    client.set_service_definition([
        {"command": "echo", "parameters": [], "response_frames": 1, "timeout": 1.0},
        {"command": "updates", "push": True, "parameters": []},
    ])
    assert client._push_commands == {"updates"}
    assert client._schema[0]["command"] == "echo"


@pytest.mark.asyncio
async def test_client_uses_custom_frame_for_request_and_response():
    server = Server(LineFrame)

    @server.command("echo")
    def echo(value):
        return value + "!"

    tcp_server, port = await _start(server)
    client = Client("127.0.0.1", port, frame_type=LineFrame)
    client.set_service_definition([
        {"command": "echo", "parameters": [{"name": "value", "type": "str",
         "kind": "POSITIONAL_OR_KEYWORD", "has_default": False, "default": None}],
         "response_frames": 1, "timeout": 1.0},
    ])
    try:
        assert await client.async_call("echo", "custom") == "custom!"
    finally:
        await _stop(client, tcp_server)


@pytest.mark.asyncio
async def test_client_rejects_missing_command_before_sending():
    client = Client("127.0.0.1", 1)
    client.set_service_definition([
        {"command": "echo", "parameters": [], "response_frames": 1, "timeout": 1.0},
    ])
    with pytest.raises(ValueError, match="frame.command"):
        await client._request(None, response_frames=1, timeout=1.0)
