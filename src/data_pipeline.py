"""
Data Pipeline Module

Main pipeline orchestrating data ingestion workflow:
scrape -> clean -> load to database

Author: Lile Zhang
Date: January 2026
"""

from typing import Dict, Optional, Tuple

import config
from data_cleaner import DataCleaner
from db_manager import DatabaseManager
from play_store_scraper import GooglePlayScraper


class DataPipeline:
    """Main pipeline for data ingestion"""

    def __init__(
        self,
        app_id: Optional[str] = None,
        db_path: Optional[str] = None,
        lang: Optional[str] = None,
        country: Optional[str] = None,
        batch_size: Optional[int] = None,
    ) -> None:
        """
        Initialize the data pipeline

        Args:
            app_id: Google Play app ID (defaults to config.DEFAULT_CONFIG["app_id"])
            db_path: Path to SQLite database (defaults to config.DEFAULT_CONFIG["db_path"])
            lang: Language code (defaults to config.DEFAULT_CONFIG["lang"])
            country: Country code (defaults to config.DEFAULT_CONFIG["country"])
            batch_size: Batch size for scraping (defaults to config.DEFAULT_CONFIG["batch_size"])
        """
        # Use config defaults if not provided
        self.app_id = app_id or config.DEFAULT_CONFIG["app_id"]
        self.db_path = db_path or config.DEFAULT_CONFIG["db_path"]

        # Initialize components with config defaults
        self.scraper = GooglePlayScraper(
            app_id=self.app_id,
            lang=lang or config.DEFAULT_CONFIG["lang"],
            country=country or config.DEFAULT_CONFIG["country"],
            batch_size=batch_size or config.DEFAULT_CONFIG["batch_size"],
        )
        self.cleaner = DataCleaner()
        self.db_manager = DatabaseManager(db_path=self.db_path)

    def run(
        self,
        total_reviews: Optional[int] = None,
        fetch_app_info: Optional[bool] = None,
    ) -> bool:
        """
        Run the complete data ingestion pipeline

        Args:
            total_reviews: Total number of reviews to fetch
                (defaults to config.DEFAULT_CONFIG["default_reviews_count"])
            fetch_app_info: Whether to fetch app information
                (defaults to config.DEFAULT_CONFIG["fetch_app_info"])

        Returns:
            Boolean indicating success
        """
        # Use config defaults if not provided
        if total_reviews is None:
            total_reviews = config.DEFAULT_CONFIG["default_reviews_count"]
        if fetch_app_info is None:
            fetch_app_info = config.DEFAULT_CONFIG["fetch_app_info"]

        print("=" * 80)
        print("DATA INGESTION PIPELINE")
        print("=" * 80)
        print(f"App ID: {self.app_id}")
        print(f"Target reviews: {total_reviews}")
        print(f"Database: {self.db_path}")
        print("=" * 80)

        try:
            # Use context manager for database operations
            with self.db_manager:
                # Step 1: Create schema if needed
                if not self.db_manager.create_schema():
                    print("Failed to create database schema")
                    return False

                # Step 2: Fetch app information
                if fetch_app_info:
                    print("\n[Step 1/4] Fetching app information...")
                    app_info = self.scraper.fetch_app_info()
                    if app_info:
                        cleaned_app_info = self.cleaner.clean_app_info(app_info)
                        if cleaned_app_info:
                            self.db_manager.insert_app_info(cleaned_app_info)
                        else:
                            print("Warning: Failed to clean app info, continuing...")
                    else:
                        print("Warning: Failed to fetch app info, continuing...")
                else:
                    print("\n[Step 1/4] Skipping app info fetch...")

                # Step 3: Fetch reviews
                print("\n[Step 2/4] Fetching reviews...")
                reviews, continuation_token = self.scraper.fetch_reviews(
                    total_count=total_reviews
                )

                if not reviews:
                    print("No reviews fetched")
                    return False

                # Step 4: Clean reviews
                print("\n[Step 3/4] Cleaning reviews...")
                cleaned_reviews_df = self.cleaner.clean_reviews(reviews)

                if cleaned_reviews_df.empty:
                    print("No valid reviews after cleaning")
                    return False

                # Step 5: Load to database
                print("\n[Step 4/4] Loading data to database...")
                success = self.db_manager.insert_reviews(
                    cleaned_reviews_df, self.app_id
                )

                if not success:
                    print("Failed to insert reviews into database")
                    return False

                # Step 6: Display statistics
                self._display_statistics()

                return True

        except Exception as e:
            print(f"\nPipeline error: {e}")
            return False

    def incrementally_fetch(
        self, batch_size: int = 1000, max_batches: Optional[int] = None
    ) -> bool:
        """
        Incrementally fetch reviews in batches

        Args:
            batch_size: Number of reviews per batch
            max_batches: Maximum number of batches (None for unlimited)

        Returns:
            Boolean indicating success
        """
        print("=" * 80)
        print("INCREMENTAL DATA INGESTION")
        print("=" * 80)
        print(f"App ID: {self.app_id}")
        print(f"Batch size: {batch_size}")
        print(f"Max batches: {max_batches or 'Unlimited'}")
        print("=" * 80)

        try:
            # Use context manager for database operations
            with self.db_manager:
                # Create schema
                if not self.db_manager.create_schema():
                    print("Failed to create database schema")
                    return False

                # Fetch app info first
                app_info = self.scraper.fetch_app_info()
                if app_info:
                    cleaned_app_info = self.cleaner.clean_app_info(app_info)
                    if cleaned_app_info:
                        self.db_manager.insert_app_info(cleaned_app_info)

                # Incremental fetching
                continuation_token: Optional[str] = None
                batch_num = 0
                total_fetched = 0

                while True:
                    if max_batches and batch_num >= max_batches:
                        print(f"\nReached max batches limit ({max_batches})")
                        break

                    batch_num += 1
                    print(f"\n[Batch {batch_num}] Fetching {batch_size} reviews...")

                    reviews, continuation_token = self.scraper.fetch_reviews(
                        total_count=batch_size, continuation_token=continuation_token
                    )

                    if not reviews:
                        print("No more reviews available")
                        break

                    # Clean and load
                    cleaned_reviews_df = self.cleaner.clean_reviews(reviews)
                    if not cleaned_reviews_df.empty:
                        success = self.db_manager.insert_reviews(
                            cleaned_reviews_df, self.app_id
                        )
                        if success:
                            total_fetched += len(cleaned_reviews_df)
                        else:
                            print(f"Warning: Failed to insert batch {batch_num}")

                    if not continuation_token:
                        print("Reached end of available reviews")
                        break

                print(f"\nIncremental fetch completed: {total_fetched:,} reviews")
                self._display_statistics()

                return True

        except Exception as e:
            print(f"\nIncremental fetch error: {e}")
            return False

    def _display_statistics(self) -> None:
        """
        Display database statistics

        Internal helper method to display statistics after pipeline operations
        """
        print("\n" + "=" * 80)
        print("PIPELINE COMPLETED")
        print("=" * 80)
        stats = self.db_manager.get_statistics()
        print(f"Total apps in database:     {stats.get('app_count', 0):,}")
        print(f"Total reviews in database:  {stats.get('review_count', 0):,}")
        if stats.get("reviews_by_app"):
            print("\nReviews by app:")
            for app_id, count in stats["reviews_by_app"].items():
                print(f"  {app_id}: {count:,} reviews")
        if stats.get("reviews_by_score"):
            print("\nReviews by score:")
            for score, count in sorted(stats["reviews_by_score"].items()):
                print(f"  {score} stars: {count:,} reviews")
        print("=" * 80)
