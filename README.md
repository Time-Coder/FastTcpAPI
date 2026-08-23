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

## Client

```python
import asyncio
from fasttcpapi import Client

async def main():
    client = Client("127.0.0.1", 9000)
    print(await client.echo("FastTcpAPI", prefix="Hello, "))
    push = await client.next_push()
    print(push.command, push.args)
    await client.close()

asyncio.run(main())
```

The command proxy uses `__call__` and validates arguments from the server
definition before sending a request. Multi-frame commands return a list. A
missing response raises `TimeoutError`; remote errors are raised by
`Frame.result()`.

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

## Custom Frames

Implement `Frame` and pass the class to `Server`. The framework does not impose
a wire layout. Your frame defines how to encode and decode the command,
arguments, keyword arguments, session ID, and protocol-specific metadata.

```python
from fasttcpapi import Frame, Server

class MyFrame(Frame):
    async def decode_from_reader(self, reader): ...
    def parse_args(self, param_list): ...
    def decode_from_result(self, result, request): ...
    def decode_from_exception(self, exception, request): ...
    def result(self): ...
    def encode(self) -> bytes: ...

server = Server(MyFrame)
```

`decode_from_result` and `decode_from_exception` receive the original request
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
server.add_on_request_callback(on_request)
server.add_on_response_callback(on_response)
server.add_on_push_callback(on_push)

client.add_on_request_callback(on_request)
client.add_on_response_callback(on_response)
client.add_on_push_callback(on_push)
```

`FastTcpAPI` remains an alias for `Server`.
