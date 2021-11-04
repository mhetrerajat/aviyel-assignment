import re
import string

import numpy as np
import pandas as pd
from gensim.parsing.porter import PorterStemmer
from gensim.parsing.preprocessing import remove_stopwords
from sklearn.cluster import KMeans
from sklearn.feature_extraction.text import TfidfVectorizer

from core.io import load_preprocessed_data

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

    # Compute category for the videos
    cdf = _categorize_videos(df)
    df = pd.merge(df, cdf, left_on=["id"], right_on=["vid"], how="left")

    return df


def _categorize_videos(df: pd.DataFrame) -> pd.DataFrame:
    def _preprocess_text(text: str) -> str:
        return (
            remove_stopwords(text)
            .translate(str.maketrans("", "", string.punctuation))
            .translate(str.maketrans("", "", string.digits))
            .lower()
        )

    required_cols = ["id", "snippet.tags"]
    cdf = df[required_cols]

    cdf["processedTag"] = cdf.apply(lambda x: x["snippet.tags"].split(" "), axis=1)
    cdf = cdf.explode("processedTag")
    cdf["processedTag"] = cdf.apply(
        lambda x: _preprocess_text(x["processedTag"]),
        axis=1,
    )

    def _stemmer(text: str) -> str:
        stemmer = PorterStemmer()
        return stemmer.stem_sentence(text)

    cdf["stemmedTag"] = cdf.apply(lambda x: _stemmer(x["processedTag"]), axis=1)

    cdf = cdf.drop_duplicates(subset=["id", "stemmedTag"])

    vectorizer = TfidfVectorizer(analyzer="word", ngram_range=(1, 2))
    word_matrix = vectorizer.fit_transform(cdf["stemmedTag"])

    word_df = pd.DataFrame(
        word_matrix.toarray(), columns=vectorizer.get_feature_names()
    )
    video_df = cdf[["id"]].rename(columns={"id": "vid"})
    result = video_df.join(word_df)

    kmeans = KMeans(n_clusters=8, max_iter=1000)
    kmeans.fit(word_df)
    result["category"] = kmeans.predict(word_df)

    result = result[["vid", "category"]]

    return result


def compute_videos_per_tag():
    df = load_preprocessed_data(columns=["id", "snippet.tags"])
    df = df.rename(columns={"snippet.tags": "tags"}).drop_duplicates(
        subset=["id", "tags"]
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
    df = load_preprocessed_data(columns=["id", "snippet.tags", "duration"])
    df = df.rename(columns={"snippet.tags": "tags"}).drop_duplicates(
        subset=["id", "tags", "duration"]
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
