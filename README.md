# Aviyel Data Assignment

## Overview

- Fetch details about atleast 300 videos using YouTube Data API
- Compute following metrics
  - Tags vs number videos i.e Number of videos per tag
  - Tag with most videos
  - Tag with least videos
  - Tags vs Avg duration of videos
  - Tag with most video time
  - Tag with least video time
- Classify videos into categories using the tags. Categories tags into groups. Compute above metrics for each category
- Bonus
  - Compute engagement metrics per tag. It includes view, like, dislike, favourite, comment counts

## Usage

Requires `Python 3.7`

- Install requirements

```bash
pip install -r requirements.txt
```

- Set `GOOGLE_API_KEY` in the environment using following command

```bash
export GOOGLE_API_KEY=<YOUR_API_KEY>
```

- Run the app

```bash
python main.py
```
