import json
import logging
from datetime import datetime
from pathlib import Path
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def load_raw_json(folder_path):
    """
    Reads all JSON files in a folder and returns a list of records
    """
    records = []
    folder = Path(folder_path)
    if not folder.exists():
        logging.warning(f"No folder found: {folder_path}")
        return records

    for file in folder.glob("*.json"):
        with open(file, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
                # Handle both list and dict
                if isinstance(data, list):
                    records.extend(data)
                elif isinstance(data, dict):
                    records.append(data)
            except Exception as e:
                logging.error(f"Error reading {file}: {e}")
    return records


def normalize_twitter(records):
    """
    Normalize raw Twitter JSON to unified schema
    """
    normalized = []
    for rec in records:
        metrics = rec.get("public_metrics", {})
        normalized.append({
            "post_id": rec.get("id"),
            "platform": "twitter",
            "content": rec.get("text"),
            "author_id": rec.get("author_id"),
            "posted_at": rec.get("created_at"),
            "likes": int(metrics.get("like_count", 0)),
            "comments": int(metrics.get("reply_count", 0)),
            "shares": int(metrics.get("retweet_count", 0)),
        })
    return normalized


def normalize_youtube(records):
    """
    Normalize raw YouTube JSON to unified schema
    (already structured in youtube_client.py)
    """
    normalized = []
    for rec in records:
        normalized.append({
            "post_id": rec.get("post_id"),
            "platform": "youtube",
            "content": rec.get("content"),
            "author_id": rec.get("author_id"),
            "posted_at": rec.get("posted_at"),
            "likes": int(rec.get("likes", 0)),
            "comments": int(rec.get("comments", 0)),
            "shares": int(rec.get("shares", 0)),
        })
    return normalized


def run_normalization(run_date=None):
    """
    Run normalization for Twitter + YouTube and save unified parquet
    """
    if run_date is None:
        run_date = datetime.utcnow().strftime("%Y-%m-%d")

    # Paths
    twitter_folder = Path(f"data/raw/twitter/{run_date}")
    youtube_folder = Path(f"data/raw/youtube/{run_date}")
    unified_folder = Path("data/unified")
    unified_folder.mkdir(parents=True, exist_ok=True)

    # Load raw
    twitter_raw = load_raw_json(twitter_folder)
    youtube_raw = load_raw_json(youtube_folder)

    # Normalize
    twitter_norm = normalize_twitter(twitter_raw)
    youtube_norm = normalize_youtube(youtube_raw)

    all_data = twitter_norm + youtube_norm

    if not all_data:
        logging.warning("No data to normalize!")
        return None

    # Convert to DataFrame
    df = pd.DataFrame(all_data)

    # Save as parquet
    file_path = unified_folder / f"{run_date}.parquet"
    df.to_parquet(file_path, engine="pyarrow", index=False)

    logging.info(f"Saved unified data to {unified_folder}")
    return file_path


#if __name__ == "__main__":
#    run_normalization()
