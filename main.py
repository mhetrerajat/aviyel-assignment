from rich.console import Console

from core.io import DataType, dump, loads
from core.youtube_api import search, fetch_video_details
from core.analyze import cleanup_video_data

console = Console()

with console.status("[bold green]Fetching search results...") as status:
    for data in search(keyword="python"):
        num_fetched = len(data["items"])

        path = dump(data=data, data_type=DataType.YOUTUBE_SEARCH)

        console.log(f"Fetched {num_fetched} results and saved to {path}")


with console.status("[bold green]Fetching video details...") as status:
    ref_search_data = loads(data_type=DataType.YOUTUBE_SEARCH)
    for search_data in ref_search_data:
        items = search_data.get("items", [])
        video_data = []
        for video in items:
            video_id = video.get("id", {}).get("videoId")
            video_details = fetch_video_details(video_id=video_id)
            video_data.append(video_details)

        path = dump(data=video_data, data_type=DataType.YOUTUBE_VIDEO)
        console.log(f"Fetched video details and stored at {path}")

with console.status("[bold green] Preprocessing and cleaning up data..") as status:
    ref_video_data = loads(data_type=DataType.YOUTUBE_VIDEO, as_dataframe=True)
    for video_df in ref_video_data:
        df = cleanup_video_data(video_df)
        path = dump(data=df, data_type=DataType.PREPROCESSED)
    console.log(f"Stored preprocessed data at {path}")
