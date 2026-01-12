from typing import TypeAlias, Union, Iterator
from pathlib import Path


path_or_bytes: TypeAlias = Union[Path, str, bytes, bytearray, Iterator[bytes]]
