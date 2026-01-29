"""
Data Cleaning Module

Handles validation, cleaning, and normalization of scraped review data.

Author: Lile Zhang
Date: January 2026
"""

from datetime import datetime, timedelta

import pandas as pd


class DataCleaner:
    """Module for cleaning and normalizing scraped data"""

    def __init__(self):
        """Initialize the data cleaner"""
        self.cleaning_stats = {
            "total_records": 0,
            "valid_records": 0,
            "invalid_records": 0,
            "duplicates_removed": 0,
            "missing_fields_filled": 0,
        }

    def clean_reviews(self, reviews):
        """
        Clean and normalize review data.

        Preserves all fields from input (reviewId, userName, userImage, content,
        score, thumbsUpCount, reviewCreatedVersion, at -> reviewed_at, appVersion,
        replyContent and repliedAt). Only invalid rows are dropped;
        no columns are removed.

        Args:
            reviews: List of review dictionaries

        Returns:
            DataFrame with cleaned review data
        """
        if not reviews:
            print("No reviews to clean")
            return pd.DataFrame()

        try:
            print(f"Cleaning {len(reviews)} reviews...")

            df = pd.DataFrame(reviews)
            self.cleaning_stats["total_records"] = len(df)
            _all_columns = set(df.columns)

            if df.empty:
                print("Empty DataFrame after conversion")
                return df

            # Step 1: Validate required fields
            df = self._validate_required_fields(df)

            # Step 2: Remove duplicates
            df = self._remove_duplicates(df)

            # Step 3: Normalize timestamps
            df = self._normalize_timestamps(df)

            # Step 4: Validate and normalize scores
            df = self._normalize_scores(df)

            # Step 5: Clean text content
            df = self._clean_text_content(df)

            # Step 6: Normalize optional numeric fields (thumbsUpCount)
            df = self._normalize_numeric_optionals(df)

            # Step 7: Handle missing values
            df = self._handle_missing_values(df)

            # Step 8: Standardize column names (keep all columns, reorder only)
            df = self._standardize_columns(df)

            # Ensure we preserved all fields (at -> reviewed_at rename is expected)
            _final_cols = set(df.columns)
            _expected = _all_columns - {"at"} | (
                {"reviewed_at"} if "at" in _all_columns else set()
            )
            if not _expected.issubset(_final_cols):
                _missing = _expected - _final_cols
                print(f"Warning: Columns no longer present after cleaning: {_missing}")

            self.cleaning_stats["valid_records"] = len(df)
            self.cleaning_stats["invalid_records"] = (
                self.cleaning_stats["total_records"]
                - self.cleaning_stats["valid_records"]
            )

            print(f"Cleaned {len(df)} valid reviews")
            self._print_cleaning_summary()

            return df

        except Exception as e:
            print(f"Error cleaning reviews: {e}")
            return pd.DataFrame()

    def clean_app_info(self, app_info):
        """
        Clean and normalize app information.

        Preserves all keys from input. Only normalizes released, lastUpdatedOn, and
        numeric fields (score, ratings, reviews, realInstalls, minInstalls, price);
        all other product fields are kept as-is.

        Args:
            app_info: Dictionary containing app information

        Returns:
            Dictionary with cleaned app information (all keys preserved)
        """
        if not app_info:
            print("No app info to clean")
            return None

        try:
            print("Cleaning app information...")

            # Normalize timestamps
            cleaned_info = app_info.copy()

            # Convert release date
            if "released" in cleaned_info and cleaned_info["released"]:
                try:
                    if isinstance(cleaned_info["released"], str):
                        cleaned_info["released"] = pd.to_datetime(
                            cleaned_info["released"], errors="coerce"
                        )
                except Exception as e:
                    print(f"Error converting released date: {e}")

            # Convert last updated date
            if "lastUpdatedOn" in cleaned_info and cleaned_info["lastUpdatedOn"]:
                try:
                    if isinstance(cleaned_info["lastUpdatedOn"], str):
                        cleaned_info["lastUpdatedOn"] = pd.to_datetime(
                            cleaned_info["lastUpdatedOn"], errors="coerce"
                        )
                except Exception as e:
                    print(f"Error converting lastUpdatedOn date: {e}")

            # Ensure numeric fields are numeric
            numeric_fields = [
                "score",
                "ratings",
                "reviews",
                "realInstalls",
                "minInstalls",
                "price",
            ]
            for field in numeric_fields:
                if field in cleaned_info:
                    try:
                        cleaned_info[field] = pd.to_numeric(
                            cleaned_info[field], errors="coerce"
                        )
                    except Exception as e:
                        print(f"Error converting {field} to numeric: {e}")

            print("App information cleaned")
            return cleaned_info

        except Exception as e:
            print(f"Error cleaning app info: {e}")
            return None

    def _validate_required_fields(self, df):
        """Validate that required fields exist and are not empty"""
        required_fields = ["reviewId", "content", "score"]

        initial_count = len(df)

        # Check for missing required fields
        missing_fields = [field for field in required_fields if field not in df.columns]
        if missing_fields:
            print(f"Warning: Missing required fields: {missing_fields}")

        # Remove rows where required fields are missing or empty
        if "reviewId" in df.columns:
            df = df[df["reviewId"].notna()].copy()
        if "content" in df.columns:
            # Keep reviews even if content is empty, but mark them
            df["content"] = df["content"].fillna("")
        if "score" in df.columns:
            df = df[df["score"].notna()].copy()

        removed = initial_count - len(df)
        if removed > 0:
            print(f"  Removed {removed} records with missing required fields")

        return df

    def _remove_duplicates(self, df):
        """Remove duplicate reviews based on reviewId"""
        initial_count = len(df)

        if "reviewId" in df.columns:
            df = df.drop_duplicates(subset=["reviewId"], keep="first").copy()

        duplicates_removed = initial_count - len(df)
        if duplicates_removed > 0:
            self.cleaning_stats["duplicates_removed"] = duplicates_removed
            print(f"  Removed {duplicates_removed} duplicate reviews")

        return df

    def _normalize_timestamps(self, df):
        """Normalize timestamp fields to datetime objects"""
        # Handle 'at' column
        if "at" in df.columns:
            df["at"] = pd.to_datetime(df["at"], errors="coerce")
            # Rename 'at' to 'reviewed_at' for consistency with database schema
            df = df.rename(columns={"at": "reviewed_at"})

        # Ensure reviewed_at exists and is valid (required field for database)
        if "reviewed_at" not in df.columns or df["reviewed_at"].isna().all():
            print(
                "Warning: No valid reviewed_at timestamp found, using current time as placeholder"
            )
            df["reviewed_at"] = datetime.now()
        else:
            # Fill any NaT values with current time
            df["reviewed_at"] = df["reviewed_at"].fillna(datetime.now())

        return df

    def _normalize_scores(self, df):
        """Validate and normalize score values"""
        if "score" not in df.columns:
            print("Warning: No score column found")
            return df

        initial_count = len(df)

        # Convert to numeric
        df["score"] = pd.to_numeric(df["score"], errors="coerce")

        # Remove rows with invalid scores
        df = df[df["score"].notna()].copy()
        df = df[(df["score"] >= 1) & (df["score"] <= 5)].copy()

        # Round to nearest integer
        df["score"] = df["score"].round().astype(int)

        removed = initial_count - len(df)
        if removed > 0:
            print(f"  Removed {removed} records with invalid scores")

        return df

    def _clean_text_content(self, df):
        """Clean and normalize text content"""
        text_columns = ["content", "replyContent", "userName"]

        for col in text_columns:
            if col in df.columns:
                # Convert to string
                df[col] = df[col].astype(str)

                # Replace 'nan' string (from conversion) with empty string
                df[col] = df[col].replace("nan", "")

                # Strip whitespace
                df[col] = df[col].str.strip()

                # Replace empty strings with None for optional fields
                if col in ["replyContent", "userName"]:
                    df[col] = df[col].replace("", None)

        return df

    def _normalize_numeric_optionals(self, df):
        """Convert optional numeric fields (thumbsUpCount) to numeric, NaN -> None"""
        if "thumbsUpCount" in df.columns:
            df["thumbsUpCount"] = pd.to_numeric(df["thumbsUpCount"], errors="coerce")
            df["thumbsUpCount"] = df["thumbsUpCount"].where(
                pd.notna(df["thumbsUpCount"]), None
            )
        return df

    def _handle_missing_values(self, df):
        """Handle missing values in optional fields"""
        # Optional fields that can be None
        optional_fields = [
            "userName",
            "userImage",
            "reviewCreatedVersion",
            "appVersion",
            "replyContent",
            "repliedAt",
            "thumbsUpCount",
        ]

        for field in optional_fields:
            if field not in df.columns:
                continue
            # For object/string columns, replace NaN with None
            if df[field].dtype == "object":
                df[field] = df[field].where(pd.notna(df[field]), None)
            # For numeric columns, convert NaN to None
            elif pd.api.types.is_numeric_dtype(df[field]):
                df[field] = df[field].where(pd.notna(df[field]), None)

        return df

    def _standardize_columns(self, df):
        """Reorder columns only; keep all columns so no product/review info is dropped."""
        preferred_order = [
            "reviewId",
            "userName",
            "userImage",
            "score",
            "content",
            "reviewed_at",
            "reviewCreatedVersion",
            "appVersion",
            "thumbsUpCount",
            "replyContent",
            "repliedAt",
        ]
        ordered_columns = [c for c in preferred_order if c in df.columns]
        remaining_columns = [c for c in df.columns if c not in ordered_columns]
        final_columns = ordered_columns + remaining_columns
        return df[final_columns].copy()

    def _print_cleaning_summary(self):
        """Print summary of cleaning operations"""
        stats = self.cleaning_stats
        print("\n" + "=" * 50)
        print("CLEANING SUMMARY")
        print("=" * 50)
        print(f"Total records:       {stats['total_records']:,}")
        print(f"Valid records:       {stats['valid_records']:,}")
        print(f"Invalid records:     {stats['invalid_records']:,}")
        print(f"Duplicates removed:  {stats['duplicates_removed']:,}")
        print("=" * 50)
