import json
import os
import tempfile
from enum import Enum, unique
from pathlib import Path
from typing import List, Union
from uuid import uuid4

import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa


@unique
class DataType(Enum):
    YOUTUBE_SEARCH = "ytsearch"
    YOUTUBE_VIDEO = "ytvideo"
    PREPROCESSED = "preprocessed"


def _dump_as_json(data: List, data_type: DataType) -> str:
    DATA_DIR = tempfile.mkdtemp(dir="/tmp/", prefix=f"aviyel__{data_type.value}__")
    filename = uuid4().hex
    path = os.path.join(DATA_DIR, f"{filename}.json")
    with open(path, "w") as f:
        f.write(json.dumps(data))
    return path


def _dump_as_parquet(data: pd.DataFrame, data_type: DataType) -> str:
    path = os.path.join("/tmp", f"aviyel__{data_type.value}")
    table = pa.Table.from_pandas(data)
    pq.write_to_dataset(table, root_path=path)
    return path


def dump(data: Union[List, pd.DataFrame], data_type: DataType) -> str:
    """Write data in specified directory in /tmp/"""
    store_as_json = data_type in [DataType.YOUTUBE_SEARCH, DataType.YOUTUBE_VIDEO]
    return (
        _dump_as_json(data, data_type)
        if store_as_json
        else _dump_as_parquet(data, data_type)
    )


def loads(data_type: DataType, as_dataframe: bool = False) -> str:
    """Loads all files one by one for specified data type"""
    dirs = Path("/tmp/").glob(f"aviyel__{data_type.value}*")
    for dir in dirs:
        for path in dir.glob("**/*"):
            if as_dataframe:
                yield pd.read_json(path)
            else:
                with open(path, "r") as f:
                    yield json.loads(f.read())
