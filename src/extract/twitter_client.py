import os
import requests
import logging
import json
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# Load API keys from .env
load_dotenv()
TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

BASE_URL = "https://api.twitter.com/2/tweets/search/recent"


def get_twitter_headers():
    """Return the auth header for Twitter API"""
    return {"Authorization": f"Bearer {TWITTER_BEARER_TOKEN}"}


def fetch_twitter_data(query="#football", max_results=10):
    """
    Fetch tweets from Twitter API (recent search)
    """
    params = {
        "query": query,
        "tweet.fields": "author_id,created_at,public_metrics",
        "max_results": max_results
    }

    response = requests.get(BASE_URL, headers=get_twitter_headers(), params=params)

    if response.status_code != 200:
        logging.error(f"Twitter API error {response.status_code}: {response.text}")
        return []

    return response.json().get("data", [])


def save_raw_twitter(data, run_date=None):
    """
    Save raw Twitter data to data/raw/twitter/run_date/
    """
    if run_date is None:
        run_date = datetime.utcnow().strftime("%Y-%m-%d")

    folder = Path(f"data/raw/twitter/{run_date}")
    folder.mkdir(parents=True, exist_ok=True)

    file_path = folder / f"tweets_{datetime.utcnow().strftime('%H%M%S')}.json"

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    logging.info(f"Saved Twitter raw data to {file_path}")


#if __name__ == "__main__":
#    logging.info("Fetching Twitter data...")
#    tweets = fetch_twitter_data(query="#football", max_results=20)
#    if tweets:
#        save_raw_twitter(tweets)
