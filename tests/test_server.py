import asyncio
import ctypes
import json
import struct
import pytest

from fasttcpapi import Server, Frame, Param, JsonFrame as BuiltinJsonFrame, decode_typed_arguments


class JsonFrame(Frame):
    async def decode(self, reader):
        size = struct.unpack("!I", await reader.readexactly(4))[0]
        payload = json.loads(await reader.readexactly(size))
        self.command = payload["command"]
        self.session_id = payload.get("session_id", 0)
        self._args = payload.get("args", [])
        self._kwargs = payload.get("kwargs", {})

    def parse_args(self, param_list):
        self.args = tuple(self._args)
        self.kwargs = self._kwargs

    def set_result(self, result, request):
        self.session_id = request.session_id
        self.command = "ok"
        self.args = (result,)
        self.kwargs = {}

    def set_exception(self, exception, request):
        self.session_id = request.session_id
        self.command = "error"
        self.args = (str(exception),)
        self.kwargs = {}

    def result(self):
        if self.command == "ok":
            return self.args[0]
        if self.command == "error":
            raise RuntimeError(self.args[0])
        raise ValueError("unexpected response")

    def encode(self):
        payload = {"command": self.command, "args": self.args, "kwargs": self.kwargs,
                   "session_id": self.session_id}
        body = json.dumps(payload).encode()
        return struct.pack("!I", len(body)) + body


async def _frame_lifecycle_binds_handler_signature_and_returns_result():
    app = Server(JsonFrame)

    @app.command("sum")
    async def add(left: int, right: int = 1):
        return left + right

    server = await app.start(port=0)
    port = server.sockets[0].getsockname()[1]
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    body = json.dumps({"command": "sum", "args": [4], "kwargs": {"right": 3}}).encode()
    writer.write(struct.pack("!I", len(body)) + body)
    await writer.drain()
    size = struct.unpack("!I", await reader.readexactly(4))[0]
    assert json.loads(await reader.readexactly(size)) == {
        "command": "ok", "args": [7], "kwargs": {}, "session_id": 0,
    }
    writer.close()
    await writer.wait_closed()
    await app.close()


def test_frame_lifecycle_binds_handler_signature_and_returns_result():
    asyncio.run(_frame_lifecycle_binds_handler_signature_and_returns_result())


async def _any_exception_is_given_to_the_frame_encoder():
    app = Server(JsonFrame)

    @app.command("broken")
    def broken():
        raise RuntimeError("unexpected failure")

    server = await app.start(port=0)
    port = server.sockets[0].getsockname()[1]
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    body = json.dumps({"command": "broken"}).encode()
    writer.write(struct.pack("!I", len(body)) + body)
    await writer.drain()
    size = struct.unpack("!I", await reader.readexactly(4))[0]
    assert json.loads(await reader.readexactly(size)) == {
        "command": "error", "args": ["unexpected failure"], "kwargs": {}, "session_id": 0,
    }
    writer.close()
    await writer.wait_closed()
    await app.close()


def test_any_exception_is_given_to_the_frame_encoder():
    asyncio.run(_any_exception_is_given_to_the_frame_encoder())


def test_custom_frame_preserves_request_session_id_in_response():
    async def scenario():
        app = Server(JsonFrame)

        @app.command("echo")
        def echo(value: str):
            return value

        server = await app.start(port=0)
        port = server.sockets[0].getsockname()[1]
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        body = json.dumps({"command": "echo", "args": ["ok"], "session_id": 42}).encode()
        writer.write(struct.pack("!I", len(body)) + body)
        await writer.drain()
        size = struct.unpack("!I", await reader.readexactly(4))[0]
        response = json.loads(await reader.readexactly(size))
        assert response["session_id"] == 42
        assert response["args"] == ["ok"]
        writer.close()
        await app.close()

    asyncio.run(scenario())


def test_typed_binary_arguments_follow_param_list():
    params = [
        Param("number", int), Param("ratio", float), Param("enabled", bool),
        Param("name", str), Param("flags", ctypes.c_uint16),
    ]
    payload = struct.pack("<ifB", -7, 1.25, 1) + b"device\0" + struct.pack("<H", 513)
    values = decode_typed_arguments(payload, params, byteorder="little")
    assert values[:4] == (-7, 1.25, True, "device")
    assert values[4].value == 513


def test_default_frame_rejects_missing_command_before_encoding():
    frame = BuiltinJsonFrame()
    frame.args = (1,)
    try:
        frame.encode()
    except ValueError as exc:
        assert "frame.command" in str(exc)
    else:
        raise AssertionError("encoding a frame without command should fail")


def test_base_frame_rejects_missing_command_after_assembly():
    frame = JsonFrame()
    try:
        frame.validate()
    except ValueError as exc:
        assert "frame.command" in str(exc)
    else:
        raise AssertionError("an assembled frame without command should fail")


async def _each_response_frame_is_sent_before_the_handler_finishes():
    app = Server(JsonFrame)
    started = asyncio.Event()
    release = asyncio.Event()

    @app.command("progress", response_frames=2)
    async def progress():
        yield "ack"
        started.set()
        await release.wait()
        yield "done"

    server = await app.start(port=0)
    port = server.sockets[0].getsockname()[1]
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    body = json.dumps({"command": "progress"}).encode()
    writer.write(struct.pack("!I", len(body)) + body)
    await writer.drain()

    size = struct.unpack("!I", await reader.readexactly(4))[0]
    first = json.loads(await reader.readexactly(size))
    assert first["args"] == ["ack"], "the first frame must arrive before the handler returns"

    release.set()
    size = struct.unpack("!I", await reader.readexactly(4))[0]
    second = json.loads(await reader.readexactly(size))
    assert second["args"] == ["done"]

    writer.close()
    await writer.wait_closed()
    await app.close()


def test_each_response_frame_is_sent_before_the_handler_finishes():
    asyncio.run(_each_response_frame_is_sent_before_the_handler_finishes())


async def _frames_of_one_request_stay_ordered_when_a_connection_is_shared():
    """Sessioned requests interleave freely, but each request keeps its own frame order.

    A frame whose request carries a session id does not wait for the previous request,
    so two in-flight commands may interleave. Within one request the frames are produced
    and written sequentially, so they always arrive in yield order.
    """
    app = Server(JsonFrame)

    @app.command("slow", response_frames=2)
    async def slow(delay: float):
        yield "a"
        await asyncio.sleep(delay)
        yield "b"

    server = await app.start(port=0)
    port = server.sockets[0].getsockname()[1]
    reader, writer = await asyncio.open_connection("127.0.0.1", port)

    def frame(command, session_id, delay):
        body = json.dumps({"command": command, "kwargs": {"delay": delay},
                           "session_id": session_id}).encode()
        return struct.pack("!I", len(body)) + body

    writer.write(frame("slow", "first", 0.2))
    await writer.drain()
    await asyncio.sleep(0.02)
    writer.write(frame("slow", "second", 0.0))
    await writer.drain()

    order = []
    for _ in range(4):
        size = struct.unpack("!I", await reader.readexactly(4))[0]
        payload = json.loads(await reader.readexactly(size))
        order.append((payload["session_id"], payload["args"][0]))

    for session in ("first", "second"):
        assert [mark for sid, mark in order if sid == session] == ["a", "b"], order

    writer.close()
    await writer.wait_closed()
    await app.close()


def test_frames_of_one_request_stay_ordered_when_a_connection_is_shared():
    asyncio.run(_frames_of_one_request_stay_ordered_when_a_connection_is_shared())


async def _frames_already_sent_survive_a_later_handler_error():
    app = Server(JsonFrame)

    @app.command("half")
    async def half():
        yield "ack"
        raise RuntimeError("failed after acknowledging")

    server = await app.start(port=0)
    port = server.sockets[0].getsockname()[1]
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    body = json.dumps({"command": "half"}).encode()
    writer.write(struct.pack("!I", len(body)) + body)
    await writer.drain()

    size = struct.unpack("!I", await reader.readexactly(4))[0]
    assert json.loads(await reader.readexactly(size))["args"] == ["ack"]
    size = struct.unpack("!I", await reader.readexactly(4))[0]
    assert json.loads(await reader.readexactly(size))["command"] == "error"

    writer.close()
    await writer.wait_closed()
    await app.close()


def test_frames_already_sent_survive_a_later_handler_error():
    asyncio.run(_frames_already_sent_survive_a_later_handler_error())


@pytest.mark.asyncio
async def test_server_lifecycle_decorators_are_called():
    app = Server()
    events = []

    @app.on_start
    def started(): events.append("start")

    @app.on_close
    def closed(): events.append("close")

    tcp = await app.start(port=0)
    assert events == ["start"]
    await app.close()
    assert events == ["start", "close"]
