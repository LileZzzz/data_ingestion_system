"""
Quick test script for DataCleaner

Fast testing of data cleaning functionality.

Usage:
    python scripts/test_data_cleaner.py
"""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

import pandas as pd

from src.data_cleaner import DataCleaner


def main():
    """Quick test of data cleaning functionality"""
    print("=" * 80)
    print("DataCleaner Quick Test")
    print("=" * 80)

    cleaner = DataCleaner()

    # Test 1: App Info Cleaning
    print("\n[Test 1] App Information Cleaning")
    print("-" * 80)
    sample_app_info = {
        "appId": "com.example.app",
        "title": "Example App",
        "score": "4.5",  # String (should be converted)
        "ratings": "10000",  # String (should be converted)
        "released": "2024-01-15",  # String date
        "lastUpdatedOn": "2026-01-20 10:30:00",  # String datetime
    }
    cleaned_app_info = cleaner.clean_app_info(sample_app_info)
    if cleaned_app_info:
        print("Cleaned app info:")
        print(
            f"  Score: {cleaned_app_info.get('score')} (type: {type(cleaned_app_info.get('score')).__name__})"
        )
        print(
            f"  Ratings: {cleaned_app_info.get('ratings')} (type: {type(cleaned_app_info.get('ratings')).__name__})"
        )
    else:
        print("Failed to clean app info")

    # Test 2: Reviews Cleaning
    print("\n[Test 2] Reviews Cleaning")
    print("-" * 80)
    sample_reviews = [
        {
            "reviewId": "review_001",
            "userName": "John Doe",
            "score": 5,
            "content": "Great app!",
            "at": datetime.now() - timedelta(days=1),
        },
        {
            "reviewId": "review_002",
            "userName": "Jane Smith",
            "score": 4,
            "content": "  Good app  ",  # Extra whitespace
            "at": datetime.now() - timedelta(days=2),
        },
        {
            "reviewId": "review_001",  # Duplicate
            "userName": "John Doe",
            "score": 5,
            "content": "Great app!",
            "at": datetime.now() - timedelta(days=1),
        },
        {
            "reviewId": "review_003",
            "score": "3",  # String score
            "content": "Average",
            "at": "2026-01-20 10:30:00",  # String timestamp
        },
    ]

    print(f"Before cleaning: {len(sample_reviews)} reviews")
    cleaned_df = cleaner.clean_reviews(sample_reviews)
    print(f"After cleaning: {len(cleaned_df)} reviews")
    print(f"Removed: {len(sample_reviews) - len(cleaned_df)} duplicates/invalid")

    if not cleaned_df.empty:
        print("\nSample cleaned review:")
        first_review = cleaned_df.iloc[0]
        print(f"  Review ID: {first_review.get('reviewId', 'N/A')}")
        print(f"  User: {first_review.get('userName', 'N/A')}")
        print(f"  Score: {first_review.get('score', 'N/A')}")
        print(f"  Content: {first_review.get('content', 'N/A')}")

    print("\n" + "=" * 80)
    print("Test completed!")
    print("=" * 80)


if __name__ == "__main__":
    main()
