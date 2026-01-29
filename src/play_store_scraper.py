"""
Google Play Store Scraper Module

A modular, reusable scraper for extracting app information and reviews
from the Google Play Store.

Author: Lile Zhang
Date: January 2026
"""

from typing import Dict, List, Optional, Tuple

from google_play_scraper import Sort, app, reviews

import config


class GooglePlayScraper:
    """
    A modular scraper for Google Play Store data.

    This class provides functionality to fetch app information and reviews
    from the Google Play Store with configurable parameters.

    Attributes:
        app_id: Google Play app ID
        lang: Language code for scraping
        country: Country code for scraping
        batch_size: Number of reviews to fetch per batch
        sort: Sort order for reviews (NEWEST, RATING, MOST_RELEVANT)
    """

    def __init__(
        self,
        app_id: Optional[str] = None,
        lang: Optional[str] = None,
        country: Optional[str] = None,
        batch_size: Optional[int] = None,
        sort_order: Optional[str] = None,
    ):
        """
        Initialize the scraper with configuration.

        Args:
            app_id: Google Play app ID. Defaults to config value.
            lang: Language code (e.g., 'en'). Defaults to config value.
            country: Country code (e.g., 'us'). Defaults to config value.
            batch_size: Number of reviews to fetch per batch. Defaults to config value.
            sort_order: Sort order for reviews ('NEWEST', 'RATING', 'MOST_RELEVANT').
                Defaults to config value.
        """
        # Use config defaults if not provided
        self.app_id = app_id or config.DEFAULT_CONFIG["app_id"]
        self.lang = lang or config.DEFAULT_CONFIG["lang"]
        self.country = country or config.DEFAULT_CONFIG["country"]
        self.batch_size = batch_size or config.DEFAULT_CONFIG["batch_size"]
        sort_config = sort_order or config.DEFAULT_CONFIG.get("sort_order", "NEWEST")

        # Validate and set sort order
        self.sort = self._parse_sort_order(sort_config)

    def _parse_sort_order(self, sort_order: str) -> Sort:
        """
        Parse sort order string into Sort enum.

        Args:
            sort_order: Sort order string ('NEWEST', 'RATING', 'MOST_RELEVANT').

        Returns:
            Sort enum value.
        """
        sort_mapping = {
            "NEWEST": Sort.NEWEST,
            "RATING": Sort.RATING,
            "MOST_RELEVANT": Sort.MOST_RELEVANT,
        }

        sort_upper = sort_order.upper()
        if sort_upper not in sort_mapping:
            raise ValueError(
                f"Invalid sort_order: {sort_order}. "
                f"Must be one of: {list(sort_mapping.keys())}"
            )

        return sort_mapping[sort_upper]

    def fetch_app_info(self) -> Optional[Dict]:
        """
        Fetch app information from Google Play Store.

        This method retrieves detailed information about the app specified
        by app_id, including title, description, ratings, and other metadata.

        Returns:
            Dictionary containing app information, or None if error occurs.
        """
        try:
            return app(self.app_id, lang=self.lang, country=self.country)
        except Exception as e:
            print(f"Error fetching app info: {e}")
            return None

    def fetch_reviews(
        self, total_count: int, continuation_token: Optional[str] = None
    ) -> Tuple[List[Dict], Optional[str]]:
        """
        Fetch reviews from Google Play Store with pagination support.

        This method retrieves reviews in batches, handling pagination
        automatically.

        Args:
            total_count: Maximum number of reviews to fetch.
            continuation_token: Token to continue from previous fetch (optional).

        Returns:
            Tuple of (list of reviews, continuation_token for next batch).
            If an error occurs, returns ([], None).
        """
        if total_count <= 0:
            return [], None

        collected_reviews: List[Dict] = []
        current_token = continuation_token

        try:
            while len(collected_reviews) < total_count:
                remaining = total_count - len(collected_reviews)
                batch_count = min(self.batch_size, remaining)

                try:
                    if current_token:
                        batch_reviews, current_token = reviews(
                            self.app_id,
                            lang=self.lang,
                            country=self.country,
                            sort=self.sort,
                            count=batch_count,
                            continuation_token=current_token,
                        )
                    else:
                        batch_reviews, current_token = reviews(
                            self.app_id,
                            lang=self.lang,
                            country=self.country,
                            sort=self.sort,
                            count=batch_count,
                        )

                    if not batch_reviews:
                        break

                    collected_reviews.extend(batch_reviews)

                    if current_token is None:
                        break

                except Exception as e:
                    print(f"Error fetching batch: {e}")
                    break

            return collected_reviews, current_token

        except Exception as e:
            print(f"Error in fetch_reviews: {e}")
            return [], None
