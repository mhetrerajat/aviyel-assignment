"""Interface to fetch data from YouTube using Google APIs"""

import os
from typing import Generator, Dict

from apiclient.discovery import build


def _get_google_api_key():
    return os.environ["GOOGLE_API_KEY"]


def _get_youtube_client():
    return build("youtube", "v3", developerKey=_get_google_api_key())


def search(keyword: str, max_results: int = 100) -> Generator:
    """Return search results for specified keywords using YT Data API"""

    youtube = _get_youtube_client()

    terminate_pagination = False
    next_page_token = None
    total_fetched = 0

    while not terminate_pagination:
        params = {
            "q": keyword,
            "part": "snippet",
            "maxResults": 25,
        }
        if next_page_token:
            params["pageToken"] = next_page_token

        request = youtube.search().list(**params)
        response = request.execute()

        next_page_token = response.get("nextPageToken")

        total_fetched += 25

        terminate_pagination = not next_page_token or total_fetched >= max_results
        yield response


def video(video_id: str) -> Dict:
    """Return video details using its youtube video id"""
    youtube = _get_youtube_client()
    request = youtube.videos().list(
        part="snippet,contentDetails,statistics,topicDetails", id=video_id
    )
    return request.execute()
