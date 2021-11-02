from rich.console import Console

from core.io import DataType, dump, loads
from core.youtube_api import search, fetch_video_details

console = Console()

# with console.status("[bold green]Fetching search results...") as status:
#     for data in search(keyword="python"):
#         num_fetched = len(data["items"])

#         path = dump(data=data, data_type=DataType.YOUTUBE_SEARCH)

#         console.log(f"Fetched {num_fetched} results and saved to {path}")


with console.status("[bold green]Fetching video details...") as status:
    ref_search_data = loads(data_type=DataType.YOUTUBE_SEARCH)
    for search_data in ref_search_data:
        items = search_data.get("items", [])
        for video in items:
            video_id = video.get("id", {}).get("videoId")
            if video_id:
                video_details = fetch_video_details(video_id=video_id)

                path = dump(data=video_details, data_type=DataType.YOUTUBE_VIDEO)

                console.log(f"Fetched video details and stored at {path}")
            else:
                # TODO: Remove this condition by fetching only video searches
                console.log("Skipped search result as its not a video")
