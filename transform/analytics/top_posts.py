import logging
from datetime import datetime
from pathlib import Path
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def compute_top_posts(run_date=None):
    """
    Compute Top 5 overall + Top 3 per platform
    """
    if run_date is None:
        run_date = datetime.utcnow().strftime("%Y-%m-%d")

    unified_file = Path(f"data/unified/{run_date}.parquet")
    outputs_folder = Path("data/outputs")
    outputs_folder.mkdir(parents=True, exist_ok=True)

    if not unified_file.exists():
        logging.warning(f"No unified file found for {run_date}")
        return None

    df = pd.read_parquet(unified_file)
    df["engagement_score"] = df["likes"] + df["comments"] + df["shares"]

    # Top 5 overall
    top_overall = df.sort_values(
        by=["engagement_score", "likes", "posted_at"], ascending=[False, False, False]
    ).head(5)

    overall_path = outputs_folder / f"top5_posts_overall_{run_date}.csv"
    top_overall.to_csv(overall_path, index=False)
    logging.info(f"Saved Top 5 overall to {overall_path}")

    # Top 3 per platform
    top_by_platform = (
        df.sort_values(
            by=["engagement_score", "likes", "posted_at"], ascending=[False, False, False]
        )
        .groupby("platform")
        .head(3)
    )

    platform_path = outputs_folder / f"top3_posts_by_platform_{run_date}.csv"
    top_by_platform.to_csv(platform_path, index=False)
    logging.info(f"Saved Top 3 per platform to {platform_path}")

    return overall_path, platform_path


#if __name__ == "__main__":
#    compute_top_posts()
