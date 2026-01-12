import logging
from pipeline.connectors.base_connector import BaseConnector
from pipeline.storages.base_storage import BaseStorage
from typing import Tuple, Union


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def read_into_storage(
    connector: BaseConnector,
    storage: Union[BaseStorage, Tuple[BaseStorage]],
):
    if connector is None:
        raise ValueError("A connector must be provided")
    if isinstance(storage, BaseStorage) and not isinstance(storage, tuple):
        s = (storage,)
    else:
        s = storage

    for storage in s:
        storage.upload_files(connector.stream())
