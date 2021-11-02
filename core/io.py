import json
import os
import tempfile
from enum import Enum
from typing import Dict
from uuid import uuid4


class DataType(Enum):
    YOUTUBE_SEARCH = "ytsearch"


def dump(data: Dict, data_type: DataType) -> str:
    """Write data in specified directory in /tmp/"""

    DATA_DIR = tempfile.mkdtemp(dir="/tmp/", prefix=f"aviyel__{data_type.value}__")

    filename = uuid4().hex
    path = os.path.join(DATA_DIR, f"{filename}.json")
    with open(path, "w") as f:
        f.write(json.dumps(data))

    return path
