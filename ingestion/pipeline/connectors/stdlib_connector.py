from pathlib import Path
from typing import Generator, Tuple, Union

from pipeline.connectors.base_connector import BaseConnector


class StdlibConnector(BaseConnector):
    def fetch_documents(self):
        pass

    def stream_documents(self) -> Generator[Tuple[str, Union[Path, str, bytes]], None, None]:
        pass

    def fetch_stream_documents(self) -> Generator[Tuple[Path, Path], None, None]:
        pass