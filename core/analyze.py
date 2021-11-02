import re
from typing import Dict
import numpy as np

import pandas as pd

df = pd.read_json(
    "/tmp/aviyel__ytvideo__z3tonovj/038d3ee19e97463fb219849c0eb8f214.json"
)
df = df.explode("items")

df = pd.json_normalize(df.items)

tagsdf = df[["id", "snippet.tags"]].rename(columns={"snippet.tags": "tags"})
tagsdf = tagsdf.explode("tags")

# Tag Vs number videos
tagsdf.groupby(by=["tags"]).agg({"id": pd.Series.nunique})


# Tag with most videos
tagsdf.groupby(by=["tags"], as_index=False).agg({"id": pd.Series.nunique}).sort_values(
    by=["id"], ascending=[False]
)

# Tag with least videos
tagsdf.groupby(by=["tags"], as_index=False).agg({"id": pd.Series.nunique}).sort_values(
    by=["id"], ascending=[True]
)

# Tag vs Avg duration of videos
cdf = df[["id", "snippet.tags", "contentDetails.duration"]].rename(
    columns={"snippet.tags": "tags", "contentDetails.duration": "videoDuration"}
)
cdf = cdf.explode("tags")
ISO_8601 = re.compile(
    "P"
    "(?:T"
    "(?:(?P<hours>\d+)H)?"
    "(?:(?P<minutes>\d+)M)?"
    "(?:(?P<seconds>\d+)S)?"
    ")?"
)
cdf["parsedDuration"] = cdf.apply(
    lambda x: ISO_8601.match(x["videoDuration"]).groupdict(), axis=1
)
_cdf = cdf[["id", "tags"]].join(pd.json_normalize(cdf.parsedDuration))
_cdf.fillna(0, inplace=True)
_cdf = _cdf.astype(dtype={"seconds": np.int16, "minutes": np.int16, "hours": np.int16})
_cdf["duration"] = _cdf["seconds"] + (_cdf["minutes"] * 60) + (_cdf["hours"] * 60 * 60)
_cdf.groupby(by=["tags"], as_index=False).agg({"duration": np.average}).sort_values(
    by=["duration"], ascending=[False]
)
