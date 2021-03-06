import re
import string
from typing import Callable

import numpy as np
import pandas as pd
from gensim.parsing.porter import PorterStemmer
from gensim.parsing.preprocessing import remove_stopwords
from sklearn.cluster import KMeans
from sklearn.feature_extraction.text import TfidfVectorizer

from core.io import DataType, load_processed_data

pd.options.mode.chained_assignment = None

ISO_8601 = re.compile(
    "P"
    "(?:T"
    "(?:(?P<hours>\d+)H)?"
    "(?:(?P<minutes>\d+)M)?"
    "(?:(?P<seconds>\d+)S)?"
    ")?"
)

ENGLISH_LETTERS = re.compile("[^a-zA-Z0-9]+")

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

    return df


def categorize_videos() -> pd.DataFrame:
    df = load_processed_data(data_type=DataType.PREPROCESSED)

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

    # Remove non-english tags
    isEng = lambda x: re.sub(ENGLISH_LETTERS, "", x)
    cdf["processedTag"] = np.vectorize(isEng)(cdf["processedTag"])

    cdf.loc[:, "processedTag"] = (
        cdf["processedTag"]
        .map(
            _preprocess_text,
        )
        .map(remove_stopwords)
    )

    # Remove rows with missing tags
    cdf = cdf[cdf["processedTag"] != ""]

    # Remove python tag as its most common one
    cdf = cdf[cdf["processedTag"] != "python"]

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

    # Number of clusters decided by elbow method
    n_cluster = 4

    kmeans = KMeans(n_clusters=n_cluster, max_iter=1000)
    kmeans.fit(word_df)
    result.loc[:, "categoryCode"] = kmeans.predict(word_df)
    result = result[["vid", "categoryCode"]]

    # Add label for categories
    # Labels are basically most frequently used tag for that category
    rdf = pd.merge(cdf, result, left_on=["id"], right_on=["vid"], how="left")

    # # Size of each cluster
    # rdf.drop_duplicates(subset=["vid", "categoryCode"]).groupby(
    #     by=["categoryCode"]
    # ).count()

    grp = rdf.groupby(by=["categoryCode"], as_index=False)
    used_label = set()
    for category_code in rdf["categoryCode"].unique():
        top_n_tags = (
            grp.get_group(category_code)
            .groupby(by=["processedTag"], as_index=False)
            .agg({"vid": "count"})
            .sort_values(by=["vid"], ascending=[False])
            .head(4)["processedTag"]
            .tolist()
        )

        label = None
        for popular_tag in top_n_tags:
            if popular_tag not in used_label:
                label = popular_tag
                used_label.add(label)
                break
        result.loc[result["categoryCode"] == category_code, "category"] = label

    return result


def _compute_nvideos_metric(for_categories: bool = False) -> pd.DataFrame:
    """Computes number of videos against specified column"""

    # Fetch required columns from the preprocessed storage
    required_cols = ["id", "snippet.tags"]
    if for_categories:
        required_cols.append("category")
    base_df = load_processed_data(columns=required_cols)

    df = base_df.drop_duplicates(subset=required_cols)

    # Rename columns
    df = df.rename(columns={"snippet.tags": "tags"})

    # Count number of videos for each `against column`
    grp_cols = ["category", "tags"] if for_categories else ["tags"]
    df = (
        df.groupby(by=grp_cols, as_index=False)
        .agg({"id": pd.Series.nunique})
        .rename(columns={"id": "n_videos"})
    )

    return df


def compute_videos_per_tag() -> pd.DataFrame:
    """Count number of videos per tag"""
    return _compute_nvideos_metric()


def compute_videos_per_category() -> pd.DataFrame:
    """Count number of videos per category"""
    return _compute_nvideos_metric(for_categories=True)


def compute_tag_with_most_videos() -> pd.DataFrame:
    return (
        _compute_nvideos_metric()
        .sort_values(by=["n_videos"], ascending=[False])
        .head(1)
    )


def compute_category_with_most_videos() -> pd.DataFrame:
    return (
        _compute_nvideos_metric(for_categories=True)
        .sort_values(by=["n_videos"], ascending=[False])
        .groupby(by=["category"], as_index=False)
        .first()
    )


def compute_tag_with_least_videos() -> pd.DataFrame:
    return (
        _compute_nvideos_metric().sort_values(by=["n_videos"], ascending=[True]).head(1)
    )


def compute_category_with_least_videos() -> pd.DataFrame:
    return (
        _compute_nvideos_metric(for_categories=True)
        .sort_values(by=["n_videos"], ascending=[True])
        .groupby(by=["category"], as_index=False)
        .first()
    )


def _compute_video_duration_metric(
    agg_func: Callable, for_categories: bool = False
) -> pd.DataFrame:

    # Fetch required columns from the preprocessed storage
    required_cols = ["id", "snippet.tags", "duration"]
    if for_categories:
        required_cols.append("category")
    base_df = load_processed_data(columns=required_cols)
    df = base_df.drop_duplicates(subset=required_cols)

    # Rename columns as per requested column name
    df = df.rename(columns={"snippet.tags": "tag"})

    grp_cols = ["category", "tag"] if for_categories else ["tag"]
    df = df.groupby(by=grp_cols, as_index=False).agg({"duration": agg_func})

    return df


def compute_avg_video_duration_per_tag() -> pd.DataFrame:
    return _compute_video_duration_metric(agg_func=np.mean).sort_values(
        by=["duration"], ascending=[False]
    )


def compute_avg_video_duration_per_category() -> pd.DataFrame:
    return _compute_video_duration_metric(
        agg_func=np.mean, for_categories=True
    ).sort_values(by=["duration"], ascending=[False])


def compute_most_video_time_tag() -> pd.DataFrame:
    return (
        _compute_video_duration_metric(agg_func=np.sum)
        .sort_values(by=["duration"], ascending=[False])
        .head(1)
    )


def compute_most_video_time_category() -> pd.DataFrame:
    return (
        _compute_video_duration_metric(agg_func=np.sum, for_categories=True)
        .sort_values(by=["duration"], ascending=[False])
        .groupby(by=["category"], as_index=False)
        .first()
    )


def compute_least_video_time_tag() -> pd.DataFrame:
    return (
        _compute_video_duration_metric(agg_func=np.sum)
        .sort_values(by=["duration"], ascending=[True])
        .head(1)
    )


def compute_least_video_time_category() -> pd.DataFrame:
    return (
        _compute_video_duration_metric(agg_func=np.sum, for_categories=True)
        .sort_values(by=["duration"], ascending=[True])
        .groupby(by=["category"], as_index=False)
        .first()
    )


def compute_engagement_per_tag() -> pd.DataFrame:
    # Fetch required columns from the preprocessed storage
    base_df = load_processed_data(
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
