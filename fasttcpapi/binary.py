"""Helpers for codecs whose command parameters are an untagged byte sequence."""

from __future__ import annotations

import ctypes
import struct
from typing import Any

from .frame import Param


def decode_typed_arguments(
    payload: bytes, param_list: list[Param], *, byteorder: str = "little", encoding: str = "utf-8"
) -> tuple[Any, ...]:
    """Decode positional arguments according to a handler's parameter list.

    Supported annotations are ``int`` (signed int32), ``float`` (IEEE-754
    float32), ``bool`` (a byte containing 0 or 1), ``str`` (NUL-terminated),
    and ctypes types (``ctypes.sizeof(annotation)`` bytes). Trailing parameters
    with Python defaults may be omitted from *payload*.
    """
    if byteorder not in {"little", "big"}:
        raise ValueError("byteorder must be 'little' or 'big'")

    offset = 0
    values: list[Any] = []
    prefix = "<" if byteorder == "little" else ">"

    for parameter in param_list:
        value, offset = _decode_one(payload, offset, parameter.type, prefix, encoding)
        values.append(value)

    if offset != len(payload):
        raise ValueError(f"{len(payload) - offset} unread parameter bytes")
    return tuple(values)


def _decode_one(payload: bytes, offset: int, annotation: Any, prefix: str, encoding: str) -> tuple[Any, int]:
    if annotation is int:
        return _unpack(payload, offset, prefix + "i", "int")
    if annotation is float:
        return _unpack(payload, offset, prefix + "f", "float")
    if annotation is bool:
        raw, next_offset = _take(payload, offset, 1, "bool")
        if raw[0] not in {0, 1}:
            raise ValueError("bool parameter must be 0 or 1")
        return bool(raw[0]), next_offset
    if annotation is str:
        terminator = payload.find(b"\0", offset)
        if terminator < 0:
            raise ValueError("string parameter is missing its NUL terminator")
        try:
            return payload[offset:terminator].decode(encoding), terminator + 1
        except UnicodeDecodeError as exc:
            raise ValueError("string parameter is not valid text") from exc
    try:
        size = ctypes.sizeof(annotation)
        raw, next_offset = _take(payload, offset, size, getattr(annotation, "__name__", "ctypes"))
        return annotation.from_buffer_copy(raw), next_offset
    except (TypeError, AttributeError) as exc:
        raise ValueError(f"unsupported binary annotation: {annotation!r}") from exc


def _unpack(payload: bytes, offset: int, format_string: str, type_name: str) -> tuple[Any, int]:
    size = struct.calcsize(format_string)
    raw, next_offset = _take(payload, offset, size, type_name)
    return struct.unpack(format_string, raw)[0], next_offset


def _take(payload: bytes, offset: int, size: int, type_name: str) -> tuple[bytes, int]:
    if len(payload) - offset < size:
        raise ValueError(f"{type_name} parameter needs {size} bytes")
    return payload[offset:offset + size], offset + size
