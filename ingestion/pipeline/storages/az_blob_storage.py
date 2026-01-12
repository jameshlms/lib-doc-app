from pipeline.storages.base_storage import BaseStorage, path_or_bytes
from typing import AnyStr, Union, Tuple
from pathlib import Path
from azure.storage.blob import BlobServiceClient


class AzBlobStorage(BaseStorage):
    _service_client_inst = None
    _container_client_inst = None

    def __new__(cls, *args, **kwargs):
        if not cls._container_client_inst:
            connect_str = kwargs.get("connect_str")
            container_name = kwargs.get("container_name")
            if not connect_str:
                raise ValueError("connect_str is required")
            if not container_name:
                raise ValueError("container_name is required")

            cls._service_client_inst = BlobServiceClient.from_connection_string(
                connect_str
            )
            cls._container_client_inst = cls._service_client_inst.get_container_client(
                container_name
            )

        return super().__new__(cls)

    def __init__(
        self,
        target_ver: Tuple[int, ...],
        out_path: Union[Path, str],
        /,
        connect_str: AnyStr,
        container_name: AnyStr,
    ):
        super().__init__(target_ver)
        self._out_path = out_path
        self._connect_str = connect_str
        self._container_name = container_name

    def upload_file(
        self,
        name: str,
        contents: path_or_bytes,
    ) -> None:
        out = Path(self._out_path) / name

        client = self._container_client_inst.get_blob_client(str(out))

        if isinstance(contents, (bytes, bytearray)):
            client.upload_blob(contents)

        elif isinstance(contents, (str, Path)):
            with open(contents, "rb") as b:
                client.upload_blob(b)

        else:
            raise TypeError("contents must be bytes, bytearray, str, or Path")
