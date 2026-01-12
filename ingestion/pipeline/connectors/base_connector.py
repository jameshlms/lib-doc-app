from pathlib import Path
from abc import ABC, abstractmethod
from typing import Any, Generator, Union, Tuple, TypeAlias, Literal
from collections.abc import Iterator

from utils.converters import tuple_to_str


path_or_bytes: TypeAlias = Union[Path, str, bytes, bytearray, Iterator[bytes]]


class BaseConnector(ABC):
    """Abstract base class for documentation connectors.

    Args:
        target_ver (Tuple[int, ...]): The target version of the documentation for the connector to retrieve.
    """

    def __init__(
        self,
        /,
        lib_name: str,
        target_ver: Union[Tuple[int, ...], Literal["latest", "stable"]],
    ) -> None:
        self._lib_name = lib_name
        self._target_ver = target_ver

    @property
    def target_ver(self) -> str:
        if isinstance(self._target_ver, str):
            return self._target_ver
        return tuple_to_str(self._target_ver, sep=".")

    def load_params(self) -> dict[str, Any]:
        return {
            "lib_name": self._lib_name,
            "target_ver": self._target_ver,
        }

    @abstractmethod
    def stream(self) -> Generator[Tuple[str, path_or_bytes], None, None]: ...
