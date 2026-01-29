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
    # Pipeline configuration
    "default_reviews_count": 1000,
    "fetch_app_info": True,
}

# App IDs for testing
APP_IDS = {
    "chatgpt": "com.openai.chatgpt",
}
