"""
Quick test script for GooglePlayScraper

Fast testing of scraper functionality.

Usage:
    python scripts/test_scraper.py
"""

import os
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

from src.play_store_scraper import GooglePlayScraper


def main():
    """Quick test of scraper functionality"""
    print("=" * 80)
    print("GooglePlayScraper Quick Test")
    print("=" * 80)

    scraper = GooglePlayScraper()

    # Fetch app info
    print("\n[Test 1] Fetching App Information")
    print("-" * 80)
    app_info = scraper.fetch_app_info()
    if app_info:
        print(f"App Title: {app_info.get('title', 'N/A')}")
        print(f"Developer: {app_info.get('developer', 'N/A')}")
        print(f"Rating: {app_info.get('score', 'N/A')}")
        print(f"Reviews Count: {app_info.get('reviews', 'N/A')}")
    else:
        print("Failed to fetch app info")

    # Fetch reviews
    print("\n[Test 2] Fetching Reviews")
    print("-" * 80)
    reviews_data, token = scraper.fetch_reviews(10)
    print(f"Fetched {len(reviews_data)} reviews")

    # Display continuation token info
    if token:
        print(
            f"\nContinuation token: {type(token).__name__} (length: {len(str(token))})"
        )
    else:
        print("\nNo continuation token (reached end of reviews)")

    # Display sample reviews
    if reviews_data:
        print("\n[Test 3] Sample Reviews")
        print("-" * 80)
        for i, review in enumerate(reviews_data[:3], 1):
            print(f"\nReview {i}:")
            print(f"  User: {review.get('userName', 'N/A')}")
            print(f"  Score: {review.get('score', 'N/A')}")
            print(f"  Content: {review.get('content', 'N/A')[:50]}...")

    print("\n" + "=" * 80)
    print("Test completed!")
    print("=" * 80)


if __name__ == "__main__":
    main()
