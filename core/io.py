import json
import os
import shutil
import tempfile
from enum import Enum, unique
from pathlib import Path, PosixPath
from typing import Generator, List, Optional, Union
from uuid import uuid4

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq


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


def load_preprocessed_data(columns: List[str]) -> pd.DataFrame:
    dataset = ds.dataset("/tmp/aviyel__preprocessed/")
    table = dataset.scanner(columns=columns).to_table()
    return table.to_pandas()


def loads(data_type: DataType, as_dataframe: bool = False) -> Generator:
    """Loads all files one by one for specified data type"""
    dirs = Path("/tmp/").glob(f"aviyel__{data_type.value}*")
    for dir in dirs:
        for path in dir.glob("**/*"):
            if as_dataframe:
                loaded_data = pd.read_json(path)
            else:
                with open(path, "r") as f:
                    loaded_data = json.loads(f.read())

            yield loaded_data


def _add_delete_marker_for_file(file_path: Union[str, PosixPath]):
    if isinstance(file_path, str):
        file_path = Path(file_path)
    shutil.rmtree(file_path)


def add_delete_marker(
    file_path: Optional[Union[str, PosixPath]] = None,
    data_type: Optional[DataType] = None,
):
    # TODO: Remove support to delete file path
    if file_path:
        _add_delete_marker_for_file(file_path)
    else:
        dirs = Path("/tmp/").glob(f"aviyel__{data_type.value}*")
        for dir in dirs:
            _add_delete_marker_for_file(dir)
