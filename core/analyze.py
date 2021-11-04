import re

import numpy as np
import pandas as pd
import pyarrow.dataset as ds

ISO_8601 = re.compile(
    "P"
    "(?:T"
    "(?:(?P<hours>\d+)H)?"
    "(?:(?P<minutes>\d+)M)?"
    "(?:(?P<seconds>\d+)S)?"
    ")?"
)


def cleanup_video_data(df: pd.DataFrame) -> pd.DataFrame:
    # Unpack items to have one row for each video
    df = df.explode("items")
    df = pd.json_normalize(df["items"])

    # Unpack tags to have one tag for one video in each row
    df = df.explode("snippet.tags")

    # Unpack topic categories
    df = df.explode("topicDetails.topicCategories")

    # Handle missing tags
    df = df.fillna(value={"snippet.tags": "unknown-marker"})

    # Preprocess duration column
    df["parsedDuration"] = df.apply(
        lambda x: ISO_8601.match(x["contentDetails.duration"]).groupdict(), axis=1
    )
    df = df.join(pd.json_normalize(df.parsedDuration))
    df.fillna(value={col: 0 for col in ["hours", "minutes", "seconds"]}, inplace=True)
    df = df.astype(
        dtype={"seconds": np.uint64, "minutes": np.uint64, "hours": np.uint64}
    )
    df["duration"] = df["seconds"] + (df["minutes"] * 60) + (df["hours"] * 60 * 60)

    return df


def compute_videos_per_tag():
    # TODO: Add common interface to load parquet data files
    dataset = ds.dataset("/tmp/aviyel__preprocessed/")
    table = dataset.scanner(columns=["id", "snippet.tags"]).to_table()
    df = (
        table.to_pandas()
        .rename(columns={"snippet.tags": "tags"})
        .drop_duplicates(subset=["id", "tags"])
    )
    df = (
        df.groupby(by=["tags"], as_index=False)
        .agg({"id": pd.Series.nunique})
        .rename(columns={"id": "n_videos"})
    )
    return df


def compute_popular_videos_by_tag():
    return compute_videos_per_tag().sort_values(by=["n_videos"], ascending=[False])


def compute_unpopular_videos_by_tag():
    return compute_videos_per_tag().sort_values(by=["n_videos"], ascending=[True])


def _compute_video_duration_by_tag():
    dataset = ds.dataset("/tmp/aviyel__preprocessed/")
    table = dataset.scanner(columns=["id", "snippet.tags", "duration"]).to_table()
    df = (
        table.to_pandas()
        .rename(columns={"snippet.tags": "tags"})
        .drop_duplicates(subset=["id", "tags", "duration"])
    )
    return df


def compute_avg_video_duration_by_tag():
    df = _compute_video_duration_by_tag()
    df = (
        df.groupby(by=["tags"], as_index=False)
        .agg({"duration": np.mean})
        .sort_values(by=["duration"], ascending=[False])
    )
    return df


def compute_most_video_time_tag():
    df = _compute_video_duration_by_tag()
    df = (
        df.groupby(by=["tags"], as_index=False)
        .agg({"duration": np.sum})
        .sort_values(by=["duration"], ascending=[False])
    ).head(1)
    return df


def compute_least_video_time_tag():
    df = _compute_video_duration_by_tag()
    df = (
        df.groupby(by=["tags"], as_index=False)
        .agg({"duration": np.sum})
        .sort_values(by=["duration"], ascending=[True])
    ).head(1)
    return df
