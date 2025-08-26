from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# نضيف البروجيكت في الـ PYTHONPATH
sys.path.append("/mnt/c/Windows/system32/social_analysis_pipeline")

# extract functions
from src.extract.twitter_client import fetch_twitter_data, save_raw_twitter
from src.extract.youtube_client import fetch_youtube_data, save_raw_youtube
from transform.normalized import run_normalization
from transform.analytics.daily_metrics import compute_daily_metrics
from transform.analytics.top_posts import compute_top_posts


# Default arguments
default_args = {
    "owner": "fatma",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG
with DAG(
    dag_id="social_media_pipeline",
    default_args=default_args,
    description="ETL pipeline for Twitter & YouTube analytics",
    schedule_interval="@daily",
    start_date=datetime(2025, 8, 26),
    catchup=False,
    dagrun_timeout= timedelta(minutes=10),
    tags=["social", "etl"],
) as dag: 
    
    # Task 1 extract twitter data
    def extract_twitter(**kwargs):
        run_date = kwargs["ds"]  
        tweets = fetch_twitter_data(query="#football", max_results=20)
        if tweets:
            save_raw_twitter(tweets, run_date=run_date)

    t1 = PythonOperator(
        task_id="extract_twitter",
        python_callable=extract_twitter,
    )

    # Task 2: extract youtube data
    def extract_youtube(**kwargs):
        run_date = kwargs["ds"]
        videos = fetch_youtube_data(query="football highlights", max_results=20)
        if videos:
            save_raw_youtube(videos, run_date=run_date)

    t2 = PythonOperator(
        task_id="extract_youtube",
        python_callable=extract_youtube,
        
    )

    # Task 3: Normalization
    t3 = PythonOperator(
        task_id="normalize",
        python_callable=run_normalization,
        op_kwargs={"run_date": "{{ ds }}"},
    )

    # Task 4: Daily Metrics
    t4 = PythonOperator(
        task_id="daily_metrics",
        python_callable=compute_daily_metrics,
        op_kwargs={"run_date": "{{ ds }}", "window": 7},
    )

    # Task 5: Top Posts
    t5 = PythonOperator(
        task_id="top_posts",
        python_callable=compute_top_posts,
        op_kwargs={"run_date": "{{ ds }}"},
    )

    # orders
    [t1, t2] >> t3 >> t4 >> t5 