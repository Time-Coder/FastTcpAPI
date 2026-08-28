# FastTcpAPI

FastAPI-style command routing for TCP services with customizable frames.

## Install

```console
pip install fasttcpapi
```

## Server

```python
import asyncio
from fasttcpapi import Server

server = Server()

@server.command("echo")
def echo(message: str, prefix: str = ""):
    return {"message": prefix + message}

@server.command("async_echo")
async def async_echo(message: str):
    return {"message": message}

@server.command("count", response_frames=2)
def count():
    yield 1
    yield 2

@server.command("async_count", response_frames=2)
async def async_count():
    yield {"tick": 1}
    yield {"tick": 2}

if __name__ == "__main__":
    server.exec("127.0.0.1", 9000)
```

Both `def` and `async def` handlers may use `return` for one response. Both
regular generators and async generators may use `yield` for multiple responses;
set `response_frames` to the exact number of yielded values. Use `timeout` to
control the client wait time. Any exception raised by a handler is converted by
the frame implementation and returned to the client.

After `await server.start(...)`, `server.address`, `server.host`, and
`server.port` expose the actual bound listener address. This is especially
useful when starting with `port=0`.

### Pushes

```python
import time

@server.push("clock.push")
async def clock_push():
    while True:
        yield {"unix_time": time.time()}
        await asyncio.sleep(1)
```

The push handler starts once for each connected client.

### Routers

Use `Router` to group commands and include them in a server. Routers can be
nested; there is no automatic prefix, so command names remain unchanged.

```python
from fasttcpapi import Router, Server

common = Router()

@common.command("ping")
def ping():
    return "pong"

admin = Router()
admin.include_router(common)

server = Server()
server.include_router(admin)
```

## Client

```python
import asyncio
from fasttcpapi import Client

async def main():
    client = Client(server_host="127.0.0.1", server_port=9000)
    print(await client.echo("FastTcpAPI", prefix="Hello, "))
    push = await client.next_push()
    print(push.command, push.args)
    await client.close()

asyncio.run(main())
```

`client.server_address` exposes the target server address. `client.self_address`
exposes the connected local socket address, or the configured local bind address
before connecting.

The built-in `JsonFrame` assigns a UUID session ID to each newly created frame.
Custom frame implementations may keep the base default of `None` or provide
their own session ID strategy.

The command proxy uses `__call__` and validates arguments from the server
definition before sending a request. Multi-frame commands return a list. A
missing response raises `TimeoutError`; remote errors are raised by
`Frame.result()`.

With the built-in `JsonFrame`, responses use a four-byte big-endian length
prefix followed by JSON. The response object has `command: "response"`, the
request `session_id`, an empty `args` array, and `kwargs`. Successful responses
contain `{"success": true, "data": value}`. Failed responses contain
`{"success": false, "data": [exception arguments], "exception": "TypeName",
"traceback": "..."}`; built-in exceptions are reconstructed by the client.

Command parameters follow normal Python signatures, including default values.
`Client` and `Server` accept `strict_type_check` (default `True`). In strict
mode values must already have the annotated type; with `False`, annotated
values are converted with that type. `Any` and unannotated parameters skip
checking and conversion. `bytes` and `bytearray` parameters are supported by
custom binary frames and must be the final parameter.

For blocking applications:

```python
client = Client("127.0.0.1", 9000, sync=True)
value = client.echo("FastTcpAPI")
```

For a `concurrent.futures.Future`:

```python
future = client.submit("echo", "FastTcpAPI")
value = future.result()
```

One `Client` instance can be used from multiple threads and event loops. Call
`await client.close()` when it is no longer needed.

Use the same custom frame type on the client when the service does not use the
built-in JSON frame:

```python
client = Client("127.0.0.1", 9000, frame_type=MyFrame)
client.set_service_definition([...])
```

The client normally fetches the service definition automatically. It can also
be supplied manually with `set_service_definition`:

```python
client.set_service_definition([
    {
        "command": "echo",
        "parameters": [
            {"name": "message", "type": "str", "kind": "POSITIONAL_OR_KEYWORD",
             "has_default": False, "default": None},
        ],
        "response_frames": 1,
        "timeout": 30.0,
    },
    {"command": "clock.push", "push": True, "parameters": []},
])
```

Each normal command definition contains `command`, `parameters`,
`response_frames`, and `timeout`. Each parameter contains `name`, `type`,
`kind`, `has_default`, and `default`. A push definition contains `command`,
`push: true`, and optionally `parameters`. Supplying a definition skips the
automatic definition request.

## Custom Frames

Implement `Frame` and pass the class to `Server`. The framework does not impose
a wire layout. Your frame defines how to encode and decode the command,
arguments, keyword arguments, session ID, and protocol-specific metadata.

```python
from fasttcpapi import Frame, Server

class MyFrame(Frame):
    async def decode(self, reader): ...
    def parse_args(self, param_list): ...
    def set_result(self, result, request): ...
    def set_exception(self, exception, request): ...
    def result(self): ...
    def encode(self) -> bytes: ...

server = Server(MyFrame)
```

`set_result` and `set_exception` receive the original request
frame, so responses can reuse its session ID and custom metadata.

For binary payloads, `decode_typed_arguments` supports `int`, `float`, `bool`,
NUL-terminated `str`, and ctypes types:

```python
from fasttcpapi import decode_typed_arguments
values = decode_typed_arguments(payload, param_list, byteorder="little")
```

See `examples/` for complete JSON and binary frame implementations.

## Callbacks

Both `Server` and `Client` support optional synchronous or asynchronous logging
callbacks:

```python
server.add_request_callback(on_request)
server.add_response_callback(on_response)
server.add_push_callback(on_push)

client.add_request_callback(on_request)
client.add_response_callback(on_response)
client.add_push_callback(on_push)
client.add_retry_connect_callback(on_retry_connect)
```

Functions registered with `@server.on_xxx` or `@client.on_xxx` omit the owning
`server`/`client` parameter; the decorator supplies and removes it internally.
Callbacks registered through `add_xxx_callback` keep the owning object as their
first parameter.

The retry callback can also be registered as `@client.on_retry_connect`.

Client `connected`, `disconnected`, and `retry_connect` callbacks receive the
`Client` instance. Client request callbacks receive the assembled request
`frame`; response and push callbacks also receive their `frame`.

Server connection, request, response, and push callbacks receive a
`ClientConnection` object as their first argument. It exposes `reader`,
`writer`, `address`, and a mutable `metadata` dictionary for connection-local
state.

Both constructors accept a `logger` class. Subclass `ServerLogger` or
`ClientLogger`, implement its constructor with the owning `server`/`client`,
and override its `on_xxx` methods; a default logger is enabled when no logger
is supplied.
