import pipeline.connectors as connectors
import pipeline.storages as storages
from pipeline.ingestion import read_into_storage

import logging


def main():
    target_ver = (2, 2)
    pandas_url = "https://pandas.pydata.org/pandas-docs/version/$version/pandas.zip"
    pandas_path = "./data/docs/pandas/v$version"

    pandas_connector = connectors.ZippedDocsConnector(target_ver, pandas_url)
    local_storage = storages.LocalClient(target_ver, pandas_path)
    read_into_storage(pandas_connector, local_storage)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s:%(name)s: %(message)s"
    )
    main()
