import logging
from datetime import datetime

# Import clients
from src.extract.twitter_client import fetch_twitter_data, save_raw_twitter
from src.extract.youtube_client import fetch_youtube_data, save_raw_youtube
from transform.normalized import run_normalization
from transform.analytics.daily_metrics import compute_daily_metrics
from transform.analytics.top_posts import compute_top_posts

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def run_extraction():
    """Run data extraction for Twitter and YouTube"""

    run_date = datetime.utcnow().strftime("%Y-%m-%d")

    # --- Twitter ---
    logging.info("Starting Twitter extraction...")
    tweets = fetch_twitter_data(query="#football", max_results=10)
    if tweets:
        save_raw_twitter(tweets, run_date=run_date)
    else:
        logging.warning("No Twitter data fetched!")

    # --- YouTube ---
    logging.info("Starting YouTube extraction...")
    videos = fetch_youtube_data(query="football highlights", max_results=20)
    if videos:
        save_raw_youtube(videos, run_date=run_date)
    else:
        logging.warning("No YouTube data fetched!")


def main():
    logging.info("=== Starting Social Media Analytics Pipeline ===")
    run_extraction()
    run_normalization()
    compute_daily_metrics()
    compute_top_posts()
    logging.info("=== Pipeline finished successfully ===")


if __name__ == "__main__":
    main()
