# FastTcpAPI

FastAPI-style TCP command routing with user-defined request/response frames.

## Start a server

```console
python examples/server.py
```

The built-in `JsonLengthPrefixFrame` uses a four-byte unsigned big-endian JSON
payload length, followed by the UTF-8 JSON payload. Requests look like:

```json
{"command":"echo","args":["FastTcpAPI"],"kwargs":{"prefix":"Hello, "}}
```

Default responses are also command frames. A normal return becomes:

```json
{"command":"response.ok","args":[{"message":"Hello, FastTcpAPI"}],"kwargs":{}}
```

An exception becomes `response.error`; its `kwargs` contain `code`, `message`,
and `solution`.

The default protocol reserves `__fasttcpapi__.schema` for the dynamic client.
There is no extra end frame: each command declares its exact response frame
count, and the client receives exactly that many frames.

## Define a command

```python
from fasttcpapi import Server

server = Server()

@server.command("user.get")
async def get_user(user_id: int, verbose: bool = False):
    return {"id": user_id, "verbose": verbose}

@server.command("clock.watch", response_frames=2)
async def watch_clock():
    for tick in range(2):
        yield {"tick": tick}
```

`return` sends one response. A yielding command must declare `response_frames`
equal to its exact number of yielded responses; each `yield` sends one frame.
Any `Exception`
is passed to `frame.decode_from_exception`, allowing the frame to choose its
error response. `CommandError` optionally provides `code` and `solution`
attributes.

```python
@server.command("sensor.read", response_frames=2)
def read_sensor():
    yield {"channel": 1, "value": 12.5}
    yield {"channel": 2, "value": 13.0}
```

The server sends exactly two frames for this command, with no completion frame.
If a handler produces fewer results, the server sends one error frame. Results
after the declared count are discarded to preserve the request/response frame
boundary.

### Active pushes

Use `push` for a handler that starts once per TCP connection. It may return one
value or continuously yield values; every value is sent immediately as an
unsolicited frame:

```python
@server.push("clock.push")
async def push_clock():
    while True:
        yield {"unix_time": time.time()}
        await asyncio.sleep(1)
```

Push tasks run concurrently with normal command handling and are cancelled when
the connection closes. A client for a custom protocol must distinguish push
frames from command responses using the command field or its equivalent. The
complete default-frame example is `python examples/push_server.py`.

## Custom frames

Implement `Frame`, then pass the class to `Server`. A frame instance is
created for each incoming request. Its lifecycle is:

```text
decode_from_reader(reader) -> select @app.command(frame.command) -> parse_args(params)
-> handler -> decode_from_result(value) or decode_from_exception(error) -> encode()
```

`decode_from_reader` must assign `self.command` to the value used by
`@app.command(...)`; it can also retain session IDs, raw parameters, or any
other request metadata.
`parse_args` receives a list of `Param(name, type)` created from the selected
function signature, and must assign `self.args` and/or `self.kwargs`. `encode`
encodes the current `self.command`, `self.args`, and `self.kwargs` as bytes.
`decode_from_result` and `decode_from_exception` convert application outcomes
into those same three fields for the response frame. `result()` is used by a
client after decoding a response frame: it returns the value for a successful
response or raises the represented exception for an error response.

```python
class MyFrame(Frame):
    async def decode_from_reader(self, reader):
        self.command = await reader.readexactly(1)
        self.payload = await reader.readexactly(4)

    def parse_args(self, param_list):
        self.args = (int.from_bytes(self.payload, "little"),)
        self.kwargs = {}

    def decode_from_result(self, result):
        self.command = b"\x81"
        self.args = (result,)
        self.kwargs = {}

    def decode_from_exception(self, exception):
        self.command = b"\x82"
        self.args = (str(exception),)
        self.kwargs = {}

    def result(self):
        if self.command == b"\x81":
            return self.args[0]
        raise RuntimeError(self.args[0])

    def encode(self):
        if self.command == b"\x82":
            return self.command + str(self.args[0]).encode("utf-8")
        return self.command + int(self.args[0]).to_bytes(4, "little")


app = Server(MyFrame)

@app.command(b"\x01")
def command(value: int):
    return value * 2
```

For untagged binary parameter data, `decode_typed_arguments(payload, param_list,
byteorder="little")` handles `int` as signed int32, `float` as IEEE-754
float32, `bool` as 0/1 byte, `str` as NUL-terminated text, and ctypes types as
`ctypes.sizeof(type)` bytes.

Runnable custom examples:

```console
python examples/custom_default_json_frame.py
python examples/custom_device_binary_codec.py
```

The second example implements:

```text
55 AA | length:uint32-little | session:uint8 | device:uint8 |
function:uint8 | parameters
```

Its `length` counts all bytes after the length field. It reuses the decoded
session/device IDs in ACK responses, and selects ACK function codes `0x81` and
`0x82` in `encode`. Change those choices in the frame class to match another
protocol.

## Client

`AsyncClient` supports the built-in `JsonLengthPrefixFrame` protocol. Start the
server, then run the included client example in another terminal:

```console
python examples/server.py
python examples/client.py
```

```python
from fasttcpapi import AsyncClient, RemoteError

client = AsyncClient("127.0.0.1", 9000)
await client.connect()  # Fetches the server's public command definitions.

message = await client.echo("FastTcpAPI", prefix="Hello, ")
ticks = await client.call("clock.watch")  # response_frames=2: returns a two-item list.
```

Before sending a command, the client validates positional argument count,
keyword names, required arguments, and built-in `bool`, `bytes`, `float`,
`int`, and `str` annotations from the fetched definition. A mismatch raises
`TypeError` locally; it does not open a command request to the server.

The first connection writes `fasttcpapi/async_client.pyi` and
`fasttcpapi/sync_client.pyi`. They contain the server's valid Python command
names and parameter types, so an IDE can use them as local type-stub artifacts.
Commands containing dots or
other non-identifier characters remain callable with:

```python
result = await client.call("user.get", 42)
```

The default frame's `result()` raises `RemoteError` for `response.error`,
exposing `code` and `solution`. For built-in Python exceptions whose original
`args` are JSON-serializable, it instead reconstructs and raises the same
built-in exception type with the same arguments. Custom exceptions, including
`CommandError`, and built-in exceptions with non-serializable arguments fall
back to `RemoteError`. Custom frame protocols require their own client because
their wire format and response completion rules are application-specific.

For synchronous applications use `SyncClient`:

```python
from fasttcpapi import SyncClient

client = SyncClient("127.0.0.1", 9000)
client.connect()
value = client.echo("FastTcpAPI")
ticks = client.call("clock.watch")
```

Run the complete blocking example with `python examples/sync_client.py`.
`FastTcpAPI` remains an alias for `Server` for backwards compatibility.
