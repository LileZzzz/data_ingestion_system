"""
Database Manager Module

Handles database schema creation, connection management, and data loading
for the data ingestion system.

Author: Lile Zhang
Date: January 2026
"""

import json
import os
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Union

import pandas as pd


def _convert_timestamp(
    value: Union[pd.Timestamp, datetime, str, None],
) -> Optional[datetime]:
    """
    Convert pandas Timestamp to Python datetime or None

    Args:
        value: pandas Timestamp, datetime, or None

    Returns:
        Python datetime object or None
    """
    if value is None or pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if isinstance(value, datetime):
        return value
    # Try to convert string to datetime
    try:
        return pd.to_datetime(value).to_pydatetime()
    except (ValueError, TypeError):
        return None


class DatabaseManager:
    """Manages database operations for the data ingestion system"""

    def __init__(self, db_path: str = "data/database/reviews.db") -> None:
        """
        Initialize the database manager

        Args:
            db_path: Path to the SQLite database file
        """
        self.db_path = db_path
        self.conn = None

        # Create directory if it doesn't exist
        db_dir = os.path.dirname(db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)

    def connect(self) -> bool:
        """Create a connection to the database"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.execute("PRAGMA foreign_keys = ON")
            print(f"Connected to database: {self.db_path}")
            return True
        except Exception as e:
            print(f"Error connecting to database: {e}")
            return False

    def close(self) -> None:
        """Close the database connection"""
        if self.conn:
            self.conn.close()
            self.conn = None
            print(" Database connection closed")

    def __enter__(self) -> "DatabaseManager":
        """
        Context manager entry point

        Returns:
            DatabaseManager instance with connection established
        """
        self.connect()
        return self

    def __exit__(
        self,
        exc_type: Optional[type],
        exc_val: Optional[Exception],
        exc_tb: Optional[Any],
    ) -> None:
        """
        Context manager exit point

        Args:
            exc_type: Exception type if an exception occurred
            exc_val: Exception value if an exception occurred
            exc_tb: Exception traceback if an exception occurred
        """
        self.close()
        return False

    def create_schema(self) -> bool:
        """Create database schema with all necessary tables"""
        if not self.conn:
            print(" No database connection")
            return False

        try:
            cursor = self.conn.cursor()

            # Create apps table
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS apps (
                    app_id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    developer TEXT,
                    genre TEXT,
                    category TEXT,
                    score REAL,
                    ratings INTEGER,
                    reviews_count INTEGER,
                    installs TEXT,
                    min_installs INTEGER,
                    real_installs INTEGER,
                    price REAL,
                    currency TEXT,
                    free BOOLEAN,
                    released DATE,
                    last_updated DATE,
                    version TEXT,
                    content_rating TEXT,
                    description TEXT,
                    raw_data TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            # Create reviews table
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS reviews (
                    review_id TEXT PRIMARY KEY,
                    app_id TEXT NOT NULL,
                    user_name TEXT,
                    score INTEGER NOT NULL CHECK (score >= 1 AND score <= 5),
                    content TEXT NOT NULL,
                    reviewed_at TIMESTAMP,
                    review_created_version TEXT,
                    thumbs_up_count INTEGER,
                    reply_content TEXT,
                    replied_at TIMESTAMP,
                    raw_data TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (app_id) REFERENCES apps(app_id) ON DELETE CASCADE
                )
            """
            )

            # Create indexes for better query performance
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_reviews_app_id 
                ON reviews(app_id)
            """
            )

            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_reviews_reviewed_at 
                ON reviews(reviewed_at)
            """
            )

            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_reviews_score 
                ON reviews(score)
            """
            )

            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_reviews_app_score 
                ON reviews(app_id, score)
            """
            )

            self.conn.commit()
            print(" Database schema created successfully")
            return True

        except Exception as e:
            print(f" Error creating schema: {e}")
            self.conn.rollback()
            return False

    def insert_app_info(self, app_info: Dict[str, Any]) -> bool:
        """
        Insert or update app information

        Args:
            app_info: Dictionary containing app information
        """
        if not self.conn:
            print(" No database connection")
            return False

        if not app_info:
            print(" No app info to insert")
            return False

        try:
            cursor = self.conn.cursor()

            # Extract app_id from appId
            app_id = app_info.get("appId", "")

            # Store raw data as JSON for complete data preservation
            raw_data_json = json.dumps(app_info, default=str)

            # Handle category field: if categories is a list/dict, serialize it
            category_value = app_info.get("category")
            if category_value is None:
                # Try to get from categories field (plural)
                categories = app_info.get("categories")
                if categories:
                    # If categories is a list or dict, serialize to JSON string
                    if isinstance(categories, (list, dict)):
                        category_value = json.dumps(categories, default=str)
                    else:
                        category_value = str(categories)
            elif isinstance(category_value, (list, dict)):
                # If category itself is a list/dict, serialize it
                category_value = json.dumps(category_value, default=str)
            else:
                # Convert to string if not already
                category_value = (
                    str(category_value) if category_value is not None else None
                )

            # Check if app already exists to preserve created_at
            cursor.execute("SELECT created_at FROM apps WHERE app_id = ?", (app_id,))
            existing_row = cursor.fetchone()
            existing_created_at = existing_row[0] if existing_row else None

            # Map fields from scraper output to database schema
            # Use INSERT OR REPLACE but preserve created_at if it exists
            if existing_created_at:
                # Update existing record, preserve created_at
                cursor.execute(
                    """
                    UPDATE apps SET
                        title = ?, developer = ?, genre = ?, category = ?, score = ?,
                        ratings = ?, reviews_count = ?, installs = ?, min_installs = ?,
                        real_installs = ?, price = ?, currency = ?, free = ?,
                        released = ?, last_updated = ?, version = ?,
                        content_rating = ?, description = ?, raw_data = ?, updated_at = ?
                    WHERE app_id = ?
                    """,
                    (
                        app_info.get("title"),
                        app_info.get("developer"),
                        app_info.get("genre"),
                        category_value,
                        app_info.get("score"),
                        app_info.get("ratings"),
                        app_info.get("reviews"),
                        app_info.get("installs"),
                        app_info.get("minInstalls"),
                        app_info.get("realInstalls"),
                        app_info.get("price"),
                        app_info.get("currency"),
                        app_info.get("free"),
                        _convert_timestamp(app_info.get("released")),
                        _convert_timestamp(app_info.get("lastUpdatedOn")),
                        app_info.get("version"),
                        app_info.get("contentRating"),
                        app_info.get("description"),
                        raw_data_json,
                        datetime.now(),
                        app_id,
                    ),
                )
            else:
                # Insert new record
                cursor.execute(
                    """
                    INSERT INTO apps (
                        app_id, title, developer, genre, category, score, ratings,
                        reviews_count, installs, min_installs, real_installs,
                        price, currency, free, released, last_updated, version,
                        content_rating, description, raw_data, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        app_id,
                        app_info.get("title"),
                        app_info.get("developer"),
                        app_info.get("genre"),
                        category_value,
                        app_info.get("score"),
                        app_info.get("ratings"),
                        app_info.get("reviews"),
                        app_info.get("installs"),
                        app_info.get("minInstalls"),
                        app_info.get("realInstalls"),
                        app_info.get("price"),
                        app_info.get("currency"),
                        app_info.get("free"),
                        _convert_timestamp(app_info.get("released")),
                        _convert_timestamp(app_info.get("lastUpdatedOn")),
                        app_info.get("version"),
                        app_info.get("contentRating"),
                        app_info.get("description"),
                        raw_data_json,
                        datetime.now(),
                    ),
                )

            self.conn.commit()
            print(f"App info inserted/updated: {app_id}")
            return True

        except Exception as e:
            print(f"Error inserting app info: {e}")
            self.conn.rollback()
            return False

    def insert_reviews(
        self,
        reviews_df: pd.DataFrame,
        app_id: str,
        batch_size: int = 1000,
        auto_commit: bool = True,
    ) -> bool:
        """
        Insert reviews into the database with optimized batch processing

        Args:
            reviews_df: DataFrame containing cleaned reviews
            app_id: App ID to associate with reviews
            batch_size: Number of records to insert per batch (default: 1000)
            auto_commit: Whether to commit after each batch (default: True)

        Returns:
            Boolean indicating success
        """
        if not self.conn:
            print("No database connection")
            return False

        if reviews_df.empty:
            print("No reviews to insert")
            return True

        try:
            # Add app_id column if not present
            if "app_id" not in reviews_df.columns:
                reviews_df = reviews_df.copy()
                reviews_df["app_id"] = app_id

            # Prepare all records for insertion
            records = []
            for _, row in reviews_df.iterrows():
                # Store raw review data as JSON (get all original fields)
                review_dict = row.to_dict()
                raw_data_json = json.dumps(review_dict, default=str)

                record = (
                    row.get("reviewId"),
                    app_id,
                    row.get("userName"),
                    row.get("score"),
                    row.get("content"),
                    _convert_timestamp(row.get("reviewed_at")),
                    row.get("reviewCreatedVersion"),
                    row.get("thumbsUpCount"),
                    row.get("replyContent"),
                    _convert_timestamp(row.get("repliedAt")),
                    raw_data_json,
                    datetime.now(),
                    datetime.now(),
                )
                records.append(record)

            total_records = len(records)
            cursor = self.conn.cursor()

            # Process in batches for better performance and memory management
            total_inserted = 0
            total_failed = 0
            num_batches = (total_records + batch_size - 1) // batch_size

            # Show progress for large datasets
            if total_records > batch_size:
                print(
                    f"Inserting {total_records:,} reviews in {num_batches} batch(es) of {batch_size:,}..."
                )

            # Insert records in batches
            for batch_idx in range(num_batches):
                start_idx = batch_idx * batch_size
                end_idx = min(start_idx + batch_size, total_records)
                batch_records = records[start_idx:end_idx]

                try:
                    # Use INSERT OR IGNORE to avoid overwriting existing reviews
                    # This preserves existing reviews and only adds new ones
                    cursor.executemany(
                        """
                        INSERT OR IGNORE INTO reviews (
                            review_id, app_id, user_name, score, content, reviewed_at,
                            review_created_version, thumbs_up_count, reply_content,
                            replied_at, raw_data, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                        batch_records,
                    )

                    batch_inserted = cursor.rowcount
                    total_inserted += batch_inserted

                    # Commit after each batch if auto_commit is enabled
                    if auto_commit:
                        self.conn.commit()

                    # Print progress for large datasets
                    if total_records > batch_size:
                        progress_pct = (end_idx / total_records) * 100
                        print(
                            f"  Batch {batch_idx + 1}/{num_batches}: Inserted {batch_inserted:,} reviews "
                            f"({progress_pct:.1f}% complete)"
                        )

                except Exception as batch_error:
                    # Handle partial failures gracefully
                    total_failed += len(batch_records)
                    print(
                        f"  Error inserting batch {batch_idx + 1}/{num_batches}: {batch_error}"
                    )
                    if auto_commit:
                        self.conn.rollback()
                    # Continue with next batch instead of failing completely

            # Final commit if auto_commit is disabled (manual transaction control)
            if not auto_commit:
                self.conn.commit()

            # Print summary
            if total_failed > 0:
                print(f"Inserted {total_inserted:,} reviews, {total_failed:,} failed")
            else:
                print(f"Inserted {total_inserted:,} reviews into database")

            return total_inserted > 0

        except Exception as e:
            print(f"Error inserting reviews: {e}")
            self.conn.rollback()
            return False

    def get_app_info(self, app_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve app information from database

        Args:
            app_id: App ID to query

        Returns:
            Dictionary containing app information, or None if not found
        """
        if not self.conn:
            return None

        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT * FROM apps WHERE app_id = ?", (app_id,))
            row = cursor.fetchone()

            if row:
                columns = [
                    "app_id",
                    "title",
                    "developer",
                    "genre",
                    "category",
                    "score",
                    "ratings",
                    "reviews_count",
                    "installs",
                    "min_installs",
                    "real_installs",
                    "price",
                    "currency",
                    "free",
                    "released",
                    "last_updated",
                    "version",
                    "content_rating",
                    "description",
                    "created_at",
                    "updated_at",
                ]
                return dict(zip(columns, row))
            return None

        except Exception as e:
            print(f"Error retrieving app info: {e}")
            return None

    def get_reviews(
        self, app_id: Optional[str] = None, limit: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Retrieve reviews from database

        Args:
            app_id: Filter by app ID (optional)
            limit: Maximum number of reviews to return (optional)

        Returns:
            DataFrame containing reviews
        """
        if not self.conn:
            return pd.DataFrame()

        try:
            query = "SELECT * FROM reviews"
            params = []

            if app_id:
                query += " WHERE app_id = ?"
                params.append(app_id)

            query += " ORDER BY reviewed_at DESC"

            if limit:
                query += " LIMIT ?"
                params.append(limit)

            df = pd.read_sql_query(query, self.conn, params=params)
            return df

        except Exception as e:
            print(f"Error retrieving reviews: {e}")
            return pd.DataFrame()

    def get_reviews_by_date_range(
        self,
        app_id: str,
        start_date: Union[datetime, pd.Timestamp, str],
        end_date: Union[datetime, pd.Timestamp, str],
    ) -> pd.DataFrame:
        """
        Filter reviews by date range for a specific app

        Args:
            app_id: App ID to filter by
            start_date: Start date (inclusive)
            end_date: End date (inclusive)

        Returns:
            DataFrame containing reviews within the date range
        """
        if not self.conn:
            return pd.DataFrame()

        try:
            start_dt = _convert_timestamp(start_date)
            end_dt = _convert_timestamp(end_date)

            if start_dt is None or end_dt is None:
                print("Invalid date format provided")
                return pd.DataFrame()

            query = """
                SELECT * FROM reviews 
                WHERE app_id = ? 
                AND reviewed_at >= ? 
                AND reviewed_at <= ?
                ORDER BY reviewed_at DESC
            """
            params = [app_id, start_dt, end_dt]

            df = pd.read_sql_query(query, self.conn, params=params)
            return df

        except Exception as e:
            print(f"Error retrieving reviews by date range: {e}")
            return pd.DataFrame()

    def get_reviews_by_score(
        self,
        app_id: str,
        min_score: int = 1,
        max_score: int = 5,
    ) -> pd.DataFrame:
        """
        Filter reviews by score range for a specific app

        Args:
            app_id: App ID to filter by
            min_score: Minimum score (inclusive, default: 1)
            max_score: Maximum score (inclusive, default: 5)

        Returns:
            DataFrame containing reviews within the score range
        """
        if not self.conn:
            return pd.DataFrame()

        try:
            # Validate score range
            if min_score < 1 or max_score > 5 or min_score > max_score:
                print(
                    "Invalid score range. Scores must be between 1 and 5, and min_score <= max_score"
                )
                return pd.DataFrame()

            query = """
                SELECT * FROM reviews 
                WHERE app_id = ? 
                AND score >= ? 
                AND score <= ?
                ORDER BY reviewed_at DESC
            """
            params = [app_id, min_score, max_score]

            df = pd.read_sql_query(query, self.conn, params=params)
            return df

        except Exception as e:
            print(f"Error retrieving reviews by score: {e}")
            return pd.DataFrame()

    def get_recent_reviews(
        self,
        app_id: str,
        days: int = 7,
    ) -> pd.DataFrame:
        """
        Get reviews from the last N days for a specific app

        Args:
            app_id: App ID to filter by
            days: Number of days to look back (default: 7)

        Returns:
            DataFrame containing recent reviews
        """
        if not self.conn:
            return pd.DataFrame()

        try:
            if days < 1:
                print("Days must be a positive integer")
                return pd.DataFrame()

            # Calculate the start date (N days ago)
            start_date = datetime.now() - timedelta(days=days)

            query = """
                SELECT * FROM reviews 
                WHERE app_id = ? 
                AND reviewed_at >= ?
                ORDER BY reviewed_at DESC
            """
            params = [app_id, start_date]

            df = pd.read_sql_query(query, self.conn, params=params)
            return df

        except Exception as e:
            print(f"Error retrieving recent reviews: {e}")
            return pd.DataFrame()

    def search_reviews(
        self,
        app_id: str,
        keyword: str,
    ) -> pd.DataFrame:
        """
        Search reviews by content keyword for a specific app

        Args:
            app_id: App ID to filter by
            keyword: Keyword to search for in review content

        Returns:
            DataFrame containing matching reviews
        """
        if not self.conn:
            return pd.DataFrame()

        try:
            if not keyword or not keyword.strip():
                print("Keyword cannot be empty")
                return pd.DataFrame()

            # Use LIKE for case-insensitive search
            query = """
                SELECT * FROM reviews 
                WHERE app_id = ? 
                AND LOWER(content) LIKE LOWER(?)
                ORDER BY reviewed_at DESC
            """
            params = [app_id, f"%{keyword}%"]

            df = pd.read_sql_query(query, self.conn, params=params)
            return df

        except Exception as e:
            print(f"Error searching reviews: {e}")
            return pd.DataFrame()

    def get_review_count_by_app(self, app_id: str) -> int:
        """
        Get count of reviews for a specific app

        Args:
            app_id: App ID to query

        Returns:
            Integer count of reviews for the app
        """
        if not self.conn:
            return 0

        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT COUNT(*) FROM reviews WHERE app_id = ?",
                (app_id,),
            )
            count = cursor.fetchone()[0]
            return count if count is not None else 0

        except Exception as e:
            print(f"Error getting review count: {e}")
            return 0

    def get_statistics(self) -> Dict[str, Any]:
        """Get database statistics"""
        if not self.conn:
            return {}

        try:
            cursor = self.conn.cursor()

            # Count apps
            cursor.execute("SELECT COUNT(*) FROM apps")
            app_count = cursor.fetchone()[0]

            # Count reviews
            cursor.execute("SELECT COUNT(*) FROM reviews")
            review_count = cursor.fetchone()[0]

            # Count reviews by app
            cursor.execute(
                """
                SELECT app_id, COUNT(*) as count 
                FROM reviews 
                GROUP BY app_id
            """
            )
            reviews_by_app = dict(cursor.fetchall())

            # Count reviews by score
            cursor.execute(
                """
                SELECT score, COUNT(*) as count 
                FROM reviews 
                GROUP BY score 
                ORDER BY score
            """
            )
            reviews_by_score = dict(cursor.fetchall())

            return {
                "app_count": app_count,
                "review_count": review_count,
                "reviews_by_app": reviews_by_app,
                "reviews_by_score": reviews_by_score,
            }

        except Exception as e:
            print(f"Error getting statistics: {e}")
            return {}
