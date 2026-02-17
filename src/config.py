"""
Configuration file for the data ingestion system

Author: Lile Zhang
Date: January 2026
"""

# Default configuration
DEFAULT_CONFIG = {
    # Scraper configuration
    "app_id": "com.openai.chatgpt",
    "lang": "en",
    "country": "us",
    "batch_size": 200,
    "sort_order": "NEWEST",  # Options: NEWEST, RATING, MOST_RELEVANT
    # Database configuration
    "db_path": "data/database/reviews.db",
    # Logging configuration
    "log_file": "logs/pipeline.log",
    "monitoring_history_file": "data/monitoring/run_history.jsonl",
    "monitoring_reports_dir": "data/monitoring/reports",
    # Scheduler configuration
    "scheduler_lock_file": "data/runtime/pipeline_scheduler.lock",
    # Pipeline configuration
    "default_reviews_count": 1000,
    "fetch_app_info": True,
}

# App IDs for testing
APP_IDS = {
    "chatgpt": "com.openai.chatgpt",
}
