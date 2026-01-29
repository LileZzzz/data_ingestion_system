"""
Quick test script for DatabaseManager

Fast testing of database operations.

Usage:
    python scripts/test_db_manager.py
"""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

import pandas as pd

from src.db_manager import DatabaseManager


def main():
    """Quick test of database operations"""
    print("=" * 80)
    print("DatabaseManager Quick Test")
    print("=" * 80)

    # Use test database
    test_db_path = "data/database/test_reviews.db"
    test_db_dir = os.path.dirname(test_db_path)
    if test_db_dir and not os.path.exists(test_db_dir):
        os.makedirs(test_db_dir, exist_ok=True)

    # Remove test database if exists
    if os.path.exists(test_db_path):
        os.remove(test_db_path)
        print(f"Removed existing test database: {test_db_path}")

    # Test with context manager
    print("\n[Test 1] Context Manager & Schema Creation")
    print("-" * 80)
    with DatabaseManager(test_db_path) as db:
        success = db.create_schema()
        print(f"Schema created: {success}")

        # Test app info insertion
        print("\n[Test 2] App Info Insertion")
        print("-" * 80)
        sample_app_info = {
            "appId": "com.test.app",
            "title": "Test App",
            "developer": "Test Developer",
            "genre": "Productivity",
            "score": 4.5,
            "ratings": 1000,
            "reviews": 500,
        }
        success = db.insert_app_info(sample_app_info)
        print(f"App info inserted: {success}")

        # Test reviews insertion
        print("\n[Test 3] Reviews Insertion")
        print("-" * 80)
        sample_reviews_data = {
            "reviewId": ["review_001", "review_002", "review_003"],
            "userName": ["User1", "User2", "User3"],
            "score": [5, 4, 3],
            "content": ["Great!", "Good", "Average"],
            "reviewed_at": [
                datetime.now() - timedelta(days=1),
                datetime.now() - timedelta(days=2),
                datetime.now() - timedelta(days=3),
            ],
        }
        reviews_df = pd.DataFrame(sample_reviews_data)
        success = db.insert_reviews(reviews_df, "com.test.app")
        print(f"Reviews inserted: {success}")

        # Test retrieval
        print("\n[Test 4] Data Retrieval")
        print("-" * 80)
        all_reviews = db.get_reviews("com.test.app")
        print(f"Retrieved {len(all_reviews)} reviews")

        # Test statistics
        print("\n[Test 5] Statistics")
        print("-" * 80)
        stats = db.get_statistics()
        print(f"Total apps: {stats.get('app_count', 0)}")
        print(f"Total reviews: {stats.get('review_count', 0)}")

    print("\n" + "=" * 80)
    print("Test completed!")
    print(f"Test database: {test_db_path}")
    print("=" * 80)


if __name__ == "__main__":
    main()
