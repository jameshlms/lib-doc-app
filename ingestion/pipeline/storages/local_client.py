from pipeline.storages.base_storage import BaseStorage
from typing import Union, Literal
from pathlib import Path
from typing import Tuple
from string import Template


class LocalClient(BaseStorage):
    def __init__(
        self,
        target_ver: Tuple[int, ...],
        out_path: str | Path,
    ) -> None:
        super().__init__(target_ver=target_ver)
        if "$version" not in str(out_path):
            raise ValueError("out_path must include $version placeholder")
        t = Template(str(out_path))
        v = self.target_ver
        self._out_path = Path(t.substitute(version=v))

    def upload_file(self, name: str, contents: Union[Path, str, bytes]) -> None:
        out_file = self._out_path / name
        out_file.parent.mkdir(parents=True, exist_ok=True)
        match contents:
            case bytes():
                bytes_contents = contents
            case str() | Path():
                bytes_contents = Path(contents).read_bytes()
            case _:
                raise TypeError("contents must be bytes, str, or Path")

        with open(out_file, "wb") as f:
            f.write(bytes_contents)

    def ping(self) -> Literal["pong"]:
        return "pong"
