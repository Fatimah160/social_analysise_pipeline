import logging
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def compute_daily_metrics(run_date=None, window=7):
    """
    Compute daily engagement per platform + moving average (MA7)
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

    # Compute engagement score
    df["engagement_score"] = df["likes"] + df["comments"] + df["shares"]

    # Group by platform
    metrics = (
        df.groupby("platform")[["likes", "comments", "shares", "engagement_score"]]
        .sum()
        .reset_index()
    )
    metrics["date"] = run_date

    # Save daily metrics
    daily_path = outputs_folder / f"daily_metrics_{run_date}.csv"
    metrics.to_csv(daily_path, index=False)
    logging.info(f"Saved daily metrics to {daily_path}")

   
    # 7-day Moving Average (MA7)
    all_files = sorted(outputs_folder.glob("daily_metrics_*.csv"))
    if len(all_files) < window:
        logging.warning(f"Not enough history for {window}-day moving average")
        return daily_path

    # merge last 7 days
    dfs = [pd.read_csv(f) for f in all_files[-window:]]
    hist = pd.concat(dfs)

    # transform to date
    hist["date"] = pd.to_datetime(hist["date"])

    # Sort 
    hist = hist.sort_values(by=["platform", "date"])

    # rolling average
    ma = (
        hist.groupby("platform")[["likes", "comments", "shares", "engagement_score"]]
        .rolling(window, min_periods=1)
        .mean()
        .reset_index()
    )

    
    ma["date"] = hist.reset_index()["date"]

    ma_path = outputs_folder / f"ma{window}_metrics_{run_date}.csv"
    ma.to_csv(ma_path, index=False)
    logging.info(f"Saved {window}-day moving average to {ma_path}")

    return daily_path, ma_path


#if __name__ == "__main__":
#    compute_daily_metrics()
