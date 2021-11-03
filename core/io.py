import json
import os
import tempfile
from enum import Enum
from pathlib import Path
from typing import List
from uuid import uuid4

import pandas as pd


class DataType(Enum):
    YOUTUBE_SEARCH = "ytsearch"
    YOUTUBE_VIDEO = "ytvideo"


def dump(data: List, data_type: DataType) -> str:
    """Write data in specified directory in /tmp/"""

    DATA_DIR = tempfile.mkdtemp(dir="/tmp/", prefix=f"aviyel__{data_type.value}__")

    filename = uuid4().hex
    path = os.path.join(DATA_DIR, f"{filename}.json")
    with open(path, "w") as f:
        f.write(json.dumps(data))

    return path


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
