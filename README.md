# 📊 Social Media Analytics Pipeline

## 🚀 Overview
This project is a small **ETL pipeline** for social media analytics using Python.  
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
```
python3 main.py
```
## Steps executed:

### 1. Fetch new posts (Twitter + YouTube)

### 2. Save raw JSON → data/raw/

### 3. Normalize to unified schema → Parquet in data/unified/

### 4. Daily metrics + 7-day MA → CSV in data/outputs/

### 5. Top posts (overall + per platform) → CSV in data/outputs/

## Automating with Cron (WSL/Linux)
### 1. Edit crontab:
```
crontab -e
```
### 2. Add job to run every day at 01:00 AM:
```
0 1 * * * cd /home/username/social_analytics_pipeline && /usr/bin/python3 main.py >> cron_log.txt 2>&1
```
### 3. Verify:
```
crontab -l
```
## Next Steps

- Extend pipeline with Airflow DAG instead of cron

- Add dashboard using Streamlit or Superset

- Support more platforms (Facebook, Instagram, TikTok)
