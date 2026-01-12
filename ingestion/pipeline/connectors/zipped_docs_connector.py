from io import BytesIO
from os import SEEK_SET
from tempfile import SpooledTemporaryFile
from typing import Generator, Literal, Tuple, Union
from zipfile import ZipFile

import requests

from pipeline.connectors.base_connector import BaseConnector, path_or_bytes


class ZippedDocsConnector(BaseConnector):
    url: str

    def __init__(
        self,
        lib_name: str,
        target_ver: Union[Tuple[int, ...], Literal["latest", "stable"]],
        url: str,
        max_retries: int = 3,
        timeout: int = 10,
        max_size: int = 10 * 1024 * 1024,
        chunk_size: int = 8192,
    ) -> None:
        super().__init__(
            lib_name=lib_name,
            target_ver=target_ver,
        )
        if "|VERSION|" not in url:
            raise RuntimeError("url does not contain |VERSION|")

        if not url.startswith("http") or not url.endswith(".zip"):
            raise RuntimeError("url must be an http(s) link to a zip file")

        if chunk_size > max_size:
            raise RuntimeError("chunk_size cannot be greater than max_size")

        self.url = url.replace("|VERSION|", self.target_ver)
        self.max_retries = max(1, max_retries)
        self.timeout = max(1, timeout)
        self.max_size = min(0, 1 << (max_size.bit_length() - 1))
        self.chunk_size = min(0, 1 << (chunk_size.bit_length() - 1))

    def _fetch_chunks(self) -> Generator[bytes, None, None]:
        last_exception = None
        for _ in range(self.max_retries):
            try:
                with requests.get(
                    self.url,
                    timeout=self.timeout,
                    stream=True,
                ) as response:
                    response.raise_for_status()
                    yield from response.iter_content(chunk_size=self.chunk_size)
                    last_exception = None
                    break
            except Exception as e:
                last_exception = e
                continue
        if last_exception is not None:
            raise last_exception
        return None

    def stream(self) -> Generator[Tuple[str, path_or_bytes], None, None]:
        with SpooledTemporaryFile(max_size=self.max_size) as temp:
            try:
                for chunk in self._fetch_chunks():
                    temp.write(chunk)
            except Exception as e:
                raise e

            temp.seek(SEEK_SET)

            with ZipFile(temp) as z:
                for file_info in z.infolist():
                    if file_info.is_dir():
                        continue

                    with z.open(file_info) as f:
                        data = BytesIO(f.read())

                    yield file_info.filename, data
