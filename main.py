from rich.console import Console

from core.io import DataType, dump
from core.youtube_api import search

console = Console()

with console.status("[bold green]Working on tasks...") as status:
    for data in search(keyword="python"):
        num_fetched = len(data["items"])

        path = dump(data=data, data_type=DataType.YOUTUBE_SEARCH)

        console.log(f"Fetched {num_fetched} results and saved to {path}")
