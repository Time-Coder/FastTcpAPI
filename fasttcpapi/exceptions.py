from typing import Any


class CommandError(Exception):
    """An expected command failure that can be serialized by a frame codec."""

    def __init__(
        self,
        message: str,
        *,
        code: Any = "command_error",
        solution: Any = None,
        data: Any = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.solution = solution
        self.data = data


class RemoteError(Exception):
    """An exception represented by a response frame received from a server."""

    def __init__(self, code: Any, message: str, solution: Any = None) -> None:
        super().__init__(message)
        self.code = code
        self.solution = solution
