import click
from rich.console import Console
from rich.markdown import Markdown

from core.analyze import (
    cleanup_video_data,
    compute_popular_videos_by_tag,
    compute_videos_per_tag,
    compute_unpopular_videos_by_tag,
    compute_avg_video_duration_by_tag,
    compute_most_video_time_tag,
    compute_least_video_time_tag,
)
from core.io import DataType, add_delete_marker, dump, loads
from core.youtube_api import fetch_video_details, search

console = Console()


@click.group()
def cli():
    pass


@cli.command()
def raw():
    """Fetches raw data using YouTube Data API"""

    with console.status("[bold red] Truncate old data...") as _:
        add_delete_marker(data_type=DataType.YOUTUBE_VIDEO)
        add_delete_marker(data_type=DataType.YOUTUBE_SEARCH)
        add_delete_marker(data_type=DataType.PREPROCESSED)

    with console.status("[bold green]Fetching search results...") as _:
        for data in search(keyword="python"):
            num_fetched = len(data["items"])

            path = dump(data=data, data_type=DataType.YOUTUBE_SEARCH)

            console.log(f"Fetched {num_fetched} results and saved to {path}")

    with console.status("[bold green]Fetching video details...") as _:
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


@cli.command()
def preprocess():
    """Preprocess raw data and make them consumable for analysis"""
    with console.status("[bold green] Preprocessing and cleaning up data..") as status:
        ref_video_data = loads(data_type=DataType.YOUTUBE_VIDEO, as_dataframe=True)
        for video_df in ref_video_data:
            df = cleanup_video_data(video_df)
            path = dump(data=df, data_type=DataType.PREPROCESSED)

        console.log(f"Stored preprocessed data at {path}")

    with console.status("[bold red] Truncate old data...") as _:
        add_delete_marker(data_type=DataType.YOUTUBE_VIDEO)
        add_delete_marker(data_type=DataType.YOUTUBE_SEARCH)


@cli.group()
def metrics():
    """Compute specified metrics using preprocessed data"""
    pass


@metrics.command()
def videos_per_tag():
    """Compute Tags Vs number of videos"""
    df = compute_videos_per_tag()
    path = "/tmp/videos_per_tag.csv"
    df.to_csv(path)
    console.log(f"Exported to {path}")


@metrics.command()
def popular_videos_per_tag():
    """Compute Tag with most videos i.e most popular tags"""
    df = compute_popular_videos_by_tag()
    path = "/tmp/popular_videos_per_tag.csv"
    df.to_csv(path)
    console.log(f"Exported to {path}")


@metrics.command()
def unpopular_videos_per_tag():
    """Compute Tag with least videos i.e most unpopular tags"""
    df = compute_unpopular_videos_by_tag()
    path = "/tmp/unpopular_videos_per_tag.csv"
    df.to_csv(path)
    console.log(f"Exported to {path}")


@metrics.command()
def avg_video_duration_per_tag():
    """Compute Tag vs Avg duration of videos"""
    df = compute_avg_video_duration_by_tag()
    path = "/tmp/avg_video_duration_per_tag.csv"
    df.to_csv(path)
    console.log(f"Exported to {path}")


@metrics.command()
def most_video_time_tag():
    """Compute Tag with most video time"""
    df = compute_most_video_time_tag()
    console.log(df.head(1))


@metrics.command()
def least_video_time_tag():
    """Compute Tag with least video time"""
    df = compute_least_video_time_tag()
    console.log(df.head(1))


if __name__ == "__main__":
    title = """
        Aviyel Data Assignment
    """
    console.print(Markdown(title))
    cli()
