from collections.abc import Iterator
from pathlib import Path
from typing import TypeAlias, Union
from abc import ABC, abstractmethod
from typing import Tuple

from utils.converters import tuple_to_str


path_or_bytes: TypeAlias = Union[Path, str, bytes, bytearray, Iterator[bytes]]


class BaseStorage(ABC):
    @abstractmethod
    def __init__(
        self,
        /,
        target_ver: Tuple[int, ...],
    ) -> None:
        self._target_ver = target_ver

    @property
    def target_ver(self) -> str:
        return tuple_to_str(self._target_ver, sep=".")

    @abstractmethod
    def upload_file(
        self,
        name: str,
        contents: path_or_bytes,
    ) -> None: ...

    def upload_files(
        self,
        files: Iterator[tuple[str, path_or_bytes]],
    ) -> None:
        for name, contents in files:
            self.upload_file(name, contents)
