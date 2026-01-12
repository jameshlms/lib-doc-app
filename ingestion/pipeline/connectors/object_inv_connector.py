from collections.abc import Generator
from typing import Literal, Tuple, Union

import requests

from pipeline.connectors.base_connector import BaseConnector, path_or_bytes


class ObjectInvConnector(BaseConnector):
    def __init__(
        self,
        /,
        lib_name: str,
        target_ver: Union[Tuple[int, ...], Literal["latest", "stable"]],
        index_path: str,
        base_url: str,
    ) -> None:
        super().__init__(
            lib_name=lib_name,
            target_ver=target_ver,
        )

        if "|VERSION|" not in index_path:
            raise RuntimeError("index_path does not contain |VERSION|")

        if not index_path.startswith("http") or not index_path.endswith("object.inv"):
            raise RuntimeError(
                "index_path must be an http(s) link to a Sphinx object.inv file"
            )

        self._index_path = index_path.replace("|VERSION|", self.target_ver)
        self._base_url = base_url.rstrip("/") if base_url.endswith("/") else base_url

    def _get_reference_page(self, name: str) -> str:
        return f"{self._base_url}/{name}"

    def stream(self) -> Generator[Tuple[str, path_or_bytes], None, None]:
        with requests.get(self._index_path, timeout=10) as response:
            response.raise_for_status()
            with open(self._index_path, "wb") as f:
                f.write(response.content)
        with open(self._index_path, "r", encoding="utf-8") as f:
            for line in f:
                yield line.strip(), line.strip().encode("utf-8")
