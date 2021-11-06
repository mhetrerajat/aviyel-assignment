# Performance Analysis

This document talks about storage requirements for both `raw` and `preprocess` stage.

## Raw Stage

The `raw` stage contains two types of data

- YT Search Result Data
- YT Video Details Data

YT search result data is kept in folders prefix with `aviyel__ytsearch` and each folder contains single json file with 50 search results. On average each folder takes around `48KBs` , we can consider it has `50KBs`

We can say, each search result data is of around `1KB`

```bash
ls -lh /tmp/aviyel__ytsearch__*/*.json | awk '{print $5}' | tr -d "K" | awk '{ total += $1; count++ } END { print total/count }'
```

YT video details data is kept in folders prefix with `aviyel__ytvideo` and each folder contains video details for all search results from single search result file i.e video details of 50 videos. On average each folder takes around `300KBs`. It is also 6 times as that of above.

We can say, each video detail data is of around `6KB`

```bash
ls -lh /tmp/aviyel__ytvideo__*/*.json | awk '{print $5}' | tr -d "K" | awk '{ total += $1; count++ } END { print total/count }'
```

## Preprocess Stage

The `preprocess` stage also contains two types of data i.e `preprocessed` and `datalake`. The `preprocessed` is intermediate stage which reads youtube video details data from the previous stage i.e `raw` and transforms it, cleans it and store it in parquet format.
The data is stored in small parquet chunks instead of a single parquet file. The reason behind storing them as chunks is the data from the `raw` stage is read in chunks i.e 50 videos in each task. This chunking can easily be scaled and these task can be ran in parallel without causing any data corruption.

Average size of the chunk is `180KBs`, for simplification we will consider it as `200KBs`

The number of chunks is equal to number of YT Search API requests i.e for 500 search results with 50 results in each request we need to make 10 requests. In this case, number of chunks is `10`.

```bash
# Check size of all parquet chunks
ls -lh /tmp/aviyel__preprocessed/*.parquet | awk '{print $5}'

# Avg size of parquet chunk
ls -lh /tmp/aviyel__preprocessed/*.parquet | awk '{print $5}' | tr -d "K" | awk '{ total += $1; count++ } END { print total/count }'
```

The `datalake` is the final stage and it is used by the `metrics` stage to compute everything. Current implementation of `datalake` stage, read entire preprocessed stage data in one go and apply clustering algorithm to compute categories for videos using their tags. As this algorithm requires entire data to train, the entire `preprocessed` stage data is loaded into memory for computation. This makes it most resource intensive task of the entire ETL pipeline.
In this stage, all parquet chunks from the previous stage are combined into one single parquet file.

Size of the datalake is around `2MBs` (2048 KBs) i.e approx `2000KBs`. This contains data for around 500 videos (i.e number of chunks is 10)

```bash
# Size of the datalake
ls -lh /tmp/aviyel__datalake/*.parquet | awk '{print $5}'
```

### Conclusion

To store data for `1000` YouTube Videos and assuming we are fetching `50` results per search request, we need

- Size requirement for YT Search Data : 1000 \* 1 KB = 1000 KB i.e 1 MB
- Size requirement for YT Video Data : 1000 \* 6 KB = 6 MB
- Size requirement for preprocessed data : ( 1000 / 50 ) \* 200 KBs = 4000 KB = 4 MB
- Size requirement for datalake data : 4 MB <br/>

The first three are the intermediate data points and they can be deleted after running their subsequent process i.e YT Search Data can be deleted once we generate YT Video Data and so on.

If we want to store 1 million YT Video Data we need the disk of atleast 4-5GBs
