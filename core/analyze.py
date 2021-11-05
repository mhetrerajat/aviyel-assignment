import re
import string
from typing import Callable

import numpy as np
import pandas as pd
from gensim.parsing.porter import PorterStemmer
from gensim.parsing.preprocessing import remove_stopwords
from sklearn.cluster import KMeans
from sklearn.feature_extraction.text import TfidfVectorizer

from core.io import load_preprocessed_data

pd.options.mode.chained_assignment = None

ISO_8601 = re.compile(
    "P"
    "(?:T"
    "(?:(?P<hours>\d+)H)?"
    "(?:(?P<minutes>\d+)M)?"
    "(?:(?P<seconds>\d+)S)?"
    ")?"
)

__all__ = [
    "cleanup_video_data",
    "compute_videos_per_tag",
    "compute_videos_per_category",
    "compute_tag_with_most_videos",
    "compute_category_with_most_videos",
    "compute_tag_with_least_videos",
    "compute_category_with_least_videos",
    "compute_avg_video_duration_by_tag",
    "compute_avg_video_duration_by_category",
    "compute_most_video_time_tag",
    "compute_most_video_time_category",
    "compute_least_video_time_tag",
    "compute_least_video_time_category",
    "compute_engagement_per_tag",
]


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
    df.loc[:, "parsedDuration"] = df.apply(
        lambda x: ISO_8601.match(x["contentDetails.duration"]).groupdict(), axis=1
    )
    df = df.join(pd.json_normalize(df.parsedDuration))
    df.fillna(value={col: 0 for col in ["hours", "minutes", "seconds"]}, inplace=True)
    df = df.astype(
        dtype={"seconds": np.uint64, "minutes": np.uint64, "hours": np.uint64}
    )
    df.loc[:, "duration"] = (
        df["seconds"] + (df["minutes"] * 60) + (df["hours"] * 60 * 60)
    )

    # Compute category for the videos
    cdf = _categorize_videos(df)
    df = pd.merge(df, cdf, left_on=["id"], right_on=["vid"], how="left")

    return df


def _categorize_videos(df: pd.DataFrame) -> pd.DataFrame:
    def _preprocess_text(text: str) -> str:
        return (
            text.translate(str.maketrans("", "", string.punctuation))
            .translate(str.maketrans("", "", string.digits))
            .lower()
        )

    required_cols = ["id", "snippet.tags"]
    cdf = df[required_cols]

    cdf.loc[:, "processedTag"] = cdf["snippet.tags"].map(lambda x: x.split(" "))
    cdf = cdf.explode("processedTag")
    cdf.loc[:, "processedTag"] = (
        cdf["processedTag"]
        .map(
            _preprocess_text,
        )
        .map(remove_stopwords)
    )

    # Remove rows with missing tags
    cdf = cdf[cdf["processedTag"] != ""]

    def _stemmer(text: str) -> str:
        stemmer = PorterStemmer()
        return stemmer.stem_sentence(text)

    cdf.loc[:, "stemmedTag"] = cdf.apply(lambda x: _stemmer(x["processedTag"]), axis=1)

    cdf = cdf.drop_duplicates(subset=["id", "stemmedTag"])

    vectorizer = TfidfVectorizer(analyzer="word", ngram_range=(1, 2))
    word_matrix = vectorizer.fit_transform(cdf["stemmedTag"])

    word_df = pd.DataFrame(
        word_matrix.toarray(), columns=vectorizer.get_feature_names_out()
    )
    video_df = cdf[["id"]].rename(columns={"id": "vid"})
    result = video_df.join(word_df)

    kmeans = KMeans(n_clusters=8, max_iter=1000)
    kmeans.fit(word_df)
    result.loc[:, "categoryCode"] = kmeans.predict(word_df)
    result = result[["vid", "categoryCode"]]

    # Add label for categories
    # Labels are basically most frequently used tag for that category
    rdf = pd.merge(cdf, result, left_on=["id"], right_on=["vid"], how="left")
    ldf = rdf.groupby(by=["categoryCode"], as_index=False)["processedTag"].agg(
        lambda x: pd.Series.mode(x).values[-1]
    )
    ldf = ldf.rename(columns={"processedTag": "category"})

    result = pd.merge(result, ldf)

    return result


def _compute_nvideos_metric(against_col_name: str) -> pd.DataFrame:
    """Computes number of videos against specified column"""

    col_name = "snippet.tags" if against_col_name == "tag" else "category"

    # Fetch required columns from the preprocessed storage
    base_df = load_preprocessed_data(columns=["id", col_name])

    # Rename columns as per requested column name
    if col_name != against_col_name:
        base_df = base_df.rename(columns={col_name: against_col_name})

    df = base_df.drop_duplicates(subset=["id", against_col_name])

    # Count number of videos for each `against column`
    df = (
        df.groupby(by=[against_col_name], as_index=False)
        .agg({"id": pd.Series.nunique})
        .rename(columns={"id": "n_videos"})
    )

    return df


def compute_videos_per_tag() -> pd.DataFrame:
    """Count number of videos per tag"""
    return _compute_nvideos_metric(against_col_name="tag")


def compute_videos_per_category() -> pd.DataFrame:
    """Count number of videos per category"""
    return _compute_nvideos_metric(against_col_name="category")


def compute_tag_with_most_videos() -> pd.DataFrame:
    return (
        _compute_nvideos_metric(against_col_name="tag")
        .sort_values(by=["n_videos"], ascending=[False])
        .head(1)
    )


def compute_category_with_most_videos() -> pd.DataFrame:
    return (
        _compute_nvideos_metric(against_col_name="category")
        .sort_values(by=["n_videos"], ascending=[False])
        .head(1)
    )


def compute_tag_with_least_videos() -> pd.DataFrame:
    return (
        _compute_nvideos_metric(against_col_name="tag")
        .sort_values(by=["n_videos"], ascending=[True])
        .head(1)
    )


def compute_category_with_least_videos() -> pd.DataFrame:
    return (
        _compute_nvideos_metric(against_col_name="category")
        .sort_values(by=["n_videos"], ascending=[True])
        .head(1)
    )


def _compute_video_duration_metric(
    against_col_name: str, agg_func: Callable
) -> pd.DataFrame:
    col_name = "snippet.tags" if against_col_name == "tag" else "category"

    # Fetch required columns from the preprocessed storage
    base_df = load_preprocessed_data(columns=["id", col_name, "duration"])

    # Rename columns as per requested column name
    if col_name != against_col_name:
        base_df = base_df.rename(columns={col_name: against_col_name})

    df = base_df.drop_duplicates(subset=["id", against_col_name, "duration"])

    df = df.groupby(by=[against_col_name], as_index=False).agg({"duration": agg_func})

    return df


def compute_avg_video_duration_per_tag() -> pd.DataFrame:
    return _compute_video_duration_metric(
        against_col_name="tag", agg_func=np.mean
    ).sort_values(by=["duration"], ascending=[False])


def compute_avg_video_duration_per_category() -> pd.DataFrame:
    return _compute_video_duration_metric(
        against_col_name="category", agg_func=np.mean
    ).sort_values(by=["duration"], ascending=[False])


def compute_most_video_time_tag() -> pd.DataFrame:
    return (
        _compute_video_duration_metric(against_col_name="tag", agg_func=np.sum)
        .sort_values(by=["duration"], ascending=[False])
        .head(1)
    )


def compute_most_video_time_category() -> pd.DataFrame:
    return (
        _compute_video_duration_metric(against_col_name="category", agg_func=np.sum)
        .sort_values(by=["duration"], ascending=[False])
        .head(1)
    )


def compute_least_video_time_tag() -> pd.DataFrame:
    return (
        _compute_video_duration_metric(against_col_name="tag", agg_func=np.sum)
        .sort_values(by=["duration"], ascending=[True])
        .head(1)
    )


def compute_least_video_time_category() -> pd.DataFrame:
    return (
        _compute_video_duration_metric(against_col_name="category", agg_func=np.sum)
        .sort_values(by=["duration"], ascending=[True])
        .head(1)
    )


def compute_engagement_per_tag() -> pd.DataFrame:
    # Fetch required columns from the preprocessed storage
    base_df = load_preprocessed_data(
        columns=[
            "id",
            "snippet.tags",
            "statistics.viewCount",
            "statistics.likeCount",
            "statistics.dislikeCount",
            "statistics.favoriteCount",
            "statistics.commentCount",
        ]
    )
    df = base_df.drop_duplicates(subset=["id", "snippet.tags"])
    rename_cols = {x: x.split(".")[-1] for x in df.columns}
    df = df.rename(columns=rename_cols)
    df = df.drop(["id"], axis=1)
    df = df.groupby(by=["tags"], as_index=False).agg(np.sum)
    return df
