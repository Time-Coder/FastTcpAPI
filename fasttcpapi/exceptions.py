class CommandError(Exception):
    """An expected command failure that can be serialized by a frame codec."""

    def __init__(
        self,
        message: str,
        *,
        code: object = "command_error",
        solution: object = None,
        data: object = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.solution = solution
        self.data = data


class RemoteError(Exception):
    """An exception represented by a response frame received from a server."""

    def __init__(self, code: object, message: str, solution: object = None) -> None:
        super().__init__(message)
        self.code = code
        self.solution = solution
