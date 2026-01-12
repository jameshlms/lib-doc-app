from collections.abc import Generator, Tuple
from io import BytesIO
from time import sleep
from typing import Literal, Union
from urllib.parse import urldefrag, urljoin

import requests
from sphobjinv import Inventory
from sphobjinv.data import DataObjBytes

from pipeline.connectors.base_connector import path_or_bytes
from pipeline.connectors.object_inv_connector import ObjectInvConnector

_sphinx_placeholder: Literal["$"] = "$"


class SphinxDocsConnector(ObjectInvConnector):
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
            index_path=index_path,
            base_url=base_url,
        )

    def _get_inv(self) -> tuple[str, list[DataObjBytes]]:
        inv_url = urljoin(self._base_url + "/objects.inv")
        with requests.get(inv_url, timeout=10) as response:
            response.raise_for_status()
            inv = Inventory(BytesIO(response.content), self._base_url)
        return inv_url, inv.objects

    def _get_object_url(
        self, uri: str, name: str | None = None
    ) -> tuple[str, str | None]:
        if _sphinx_placeholder in uri and name is not None:
            uri = uri.replace(_sphinx_placeholder, name)

        full_path = urljoin(self._base_url + "/", uri)

        page_url, fragment = urldefrag(full_path)

        return page_url, fragment or None

    def stream(self) -> Generator[tuple[str, path_or_bytes], None, None]:
        headers = {"User-Agent": f"{self._lib_name}-rag-fetch/1.0"}
        seen_page_urls: set[str] = set()
        _, inv_objects = self._get_inv()

        for obj in inv_objects:
            if not isinstance(obj, DataObjBytes):
                continue

            uri = str(obj.uri)
            page_url, _ = self._get_object_url(
                uri=uri,
                name=obj.name,
            )

            if page_url in seen_page_urls:
                continue
            seen_page_urls.add(page_url)

            with requests.get(page_url, timeout=10) as response:
                response.raise_for_status()
                yield uri, response.content

            sleep(0.2)

        return None
