import os
import requests
import logging
import json
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# Load keys
load_dotenv()
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"
VIDEO_URL = "https://www.googleapis.com/youtube/v3/videos"


def fetch_youtube_data(query="football highlights", max_results=10):
    """
    Fetch videos from YouTube API and enrich with statistics (likes, comments)
    """
    params = {
        "q": query,
        "part": "snippet",
        "type": "video",
        "maxResults": max_results,
        "key": YOUTUBE_API_KEY
    }

    search_resp = requests.get(SEARCH_URL, params=params)
    if search_resp.status_code != 200:
        logging.error(f"YouTube search API error {search_resp.status_code}: {search_resp.text}")
        return []

    items = search_resp.json().get("items", [])
    if not items:
        return []

    video_ids = [item["id"]["videoId"] for item in items]

    # Fetch statistics
    stats_params = {
        "id": ",".join(video_ids),
        "part": "statistics,snippet",
        "key": YOUTUBE_API_KEY
    }
    stats_resp = requests.get(VIDEO_URL, params=stats_params)
    if stats_resp.status_code != 200:
        logging.error(f"YouTube videos API error {stats_resp.status_code}: {stats_resp.text}")
        return []

    stats_items = stats_resp.json().get("items", [])

    results = []
    for item in stats_items:
        video_id = item["id"]
        snippet = item.get("snippet", {})
        stats = item.get("statistics", {})

        results.append({
            "post_id": video_id,
            "platform": "youtube",
            "content": snippet.get("title"),
            "author_id": snippet.get("channelId"),
            "posted_at": snippet.get("publishedAt"),
            "likes": int(stats.get("likeCount", 0)),
            "comments": int(stats.get("commentCount", 0)),
            "shares": 0,  
        })

    return results


def save_raw_youtube(data, run_date=None):
    """
    Save raw YouTube data to data/raw/youtube/run_date/
    """
    if run_date is None:
        run_date = datetime.utcnow().strftime("%Y-%m-%d")

    folder = Path(f"data/raw/youtube/{run_date}")
    folder.mkdir(parents=True, exist_ok=True)

    file_path = folder / f"youtube_{datetime.utcnow().strftime('%H%M%S')}.json"

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    logging.info(f"Saved YouTube raw data to {file_path}")


#if __name__ == "__main__":
#    logging.info("Fetching YouTube data...")
#    videos = fetch_youtube_data(query="football highlights", max_results=10)
#    if videos:
#        save_raw_youtube(videos)
