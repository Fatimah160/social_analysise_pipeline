# 📊 Social Media Analytics Pipeline

## 🚀 Overview
This project is an ETL pipeline for social media analytics using Python and Apache Airflow.
It:
- Fetches new posts daily from **Twitter** and **YouTube** (can be extended to Facebook, etc.)
- Normalizes raw JSON into a unified schema
- Computes **daily engagement metrics** and **7-day moving average**
- Extracts **Top posts** (overall + per platform)
- Stores results in **CSV** and **SQLite database**

---

## ⚙️ Setup

### 1. Clone repo
```bash
git clone https://github.com/Fatimah160/social_analysise_pipeline.git
cd social_analysise_pipeline
```
### 2. create conda environment 
```
conda create -n social_pipeline python=3.10 -y
conda activate social_pipeline
```
### 3. install dependences
```
pip install -r requirements.txt
```
### 4. API keys
```
TWITTER_BEARER_TOKEN=your_twitter_api_key
YOUTUBE_API_KEY=your_youtube_api_key
```
### 5. Run pipeline 
##Option A: Run manually (debugging)
```
python3 main.py
```
##Option B: Run with Airflow (recommended ✅)
- Initialize Airflow (first time only):
```
airflow db init
```

- Start Airflow services:
```
airflow webserver --port 8080
airflow scheduler
```

- Place DAG file inside:
```
airflow/dags/social_media_pipeline.py
```
##Airflow Graph
<img width="1332" height="471" alt="brave_RufBT1EPbH" src="https://github.com/user-attachments/assets/cbb6ce6d-ed0b-4b6c-9ef5-9d9de0136fdb" />

## Steps executed:

-  Fetch new posts (Twitter + YouTube)

-  Save raw JSON → data/raw/

-  Normalize to unified schema → Parquet in data/unified/

-  Daily metrics + 7-day MA → CSV in data/outputs/

-  Top posts (overall + per platform) → CSV in data/outputs/

## Next Steps

- Add dashboard using Streamlit or Superset

- Support more platforms (Facebook, Instagram, TikTok)
