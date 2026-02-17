"""
Data Pipeline Module

Main pipeline orchestrating data ingestion workflow:
scrape -> clean -> load to database

Author: Lile Zhang
Date: January 2026
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from uuid import uuid4

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
        log_file: Optional[str] = None,
        log_to_console: bool = True,
        monitoring_history_file: Optional[str] = None,
        monitoring_reports_dir: Optional[str] = None,
    ) -> None:
        """
        Initialize the data pipeline

        Args:
            app_id: Google Play app ID (defaults to config.DEFAULT_CONFIG["app_id"])
            db_path: Path to SQLite database (defaults to config.DEFAULT_CONFIG["db_path"])
            lang: Language code (defaults to config.DEFAULT_CONFIG["lang"])
            country: Country code (defaults to config.DEFAULT_CONFIG["country"])
            batch_size: Batch size for scraping (defaults to config.DEFAULT_CONFIG["batch_size"])
            log_file: Path to log file (defaults to config.DEFAULT_CONFIG["log_file"])
            log_to_console: Whether to print logs to console
            monitoring_history_file: JSONL file for monitoring history
                (defaults to config.DEFAULT_CONFIG["monitoring_history_file"])
            monitoring_reports_dir: Directory for per-run reports
                (defaults to config.DEFAULT_CONFIG["monitoring_reports_dir"])
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
        self.log_file = log_file or config.DEFAULT_CONFIG.get("log_file")
        self.monitoring_history_file = (
            monitoring_history_file
            or config.DEFAULT_CONFIG.get("monitoring_history_file")
        )
        self.monitoring_reports_dir = (
            monitoring_reports_dir
            or config.DEFAULT_CONFIG.get("monitoring_reports_dir")
        )
        self.logger = self._setup_logger(
            log_file_path=self.log_file,
            log_to_console=log_to_console,
        )
        self.last_run_summary: Dict[str, Any] = {}

    def _setup_logger(
        self, log_file_path: Optional[str], log_to_console: bool
    ) -> logging.Logger:
        """Configure logger for structured pipeline events."""
        logger = logging.getLogger(f"data_pipeline.{id(self)}")
        logger.handlers.clear()
        if log_to_console:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                "%(asctime)s %(levelname)s %(name)s %(message)s"
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        if log_file_path:
            Path(log_file_path).parent.mkdir(parents=True, exist_ok=True)
            file_handler = logging.FileHandler(log_file_path, encoding="utf-8")
            formatter = logging.Formatter(
                "%(asctime)s %(levelname)s %(name)s %(message)s"
            )
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
        return logger

    def _log_event(
        self,
        level: int,
        event: str,
        run_summary: Dict[str, Any],
        **extra: Any,
    ) -> None:
        """Emit a structured log line with run context."""
        payload = {
            "event": event,
            "run_id": run_summary.get("run_id"),
            "mode": run_summary.get("mode"),
            "app_id": self.app_id,
            **extra,
        }
        self.logger.log(level, json.dumps(payload, default=str))

    def _initialize_run_summary(
        self, mode: str, target_reviews: Optional[int] = None
    ) -> Dict[str, Any]:
        """Create a run summary object with default metrics."""
        return {
            "run_id": str(uuid4()),
            "mode": mode,
            "app_id": self.app_id,
            "db_path": self.db_path,
            "start_time_utc": datetime.now(timezone.utc).isoformat(),
            "end_time_utc": None,
            "duration_seconds": None,
            "status": "running",
            "target_reviews": target_reviews,
            "fetch_app_info": False,
            "batches_processed": 0,
            "total_reviews_fetched": 0,
            "total_reviews_cleaned": 0,
            "total_reviews_inserted": 0,
            "total_duplicates_or_existing": 0,
            "error_stage": None,
            "error_message": None,
        }

    def _finalize_run_summary(
        self,
        run_summary: Dict[str, Any],
        status: str,
        error_stage: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> None:
        """Finalize run summary with timing and terminal status."""
        end_time = datetime.now(timezone.utc)
        start_time = datetime.fromisoformat(run_summary["start_time_utc"])
        run_summary["end_time_utc"] = end_time.isoformat()
        run_summary["duration_seconds"] = round(
            (end_time - start_time).total_seconds(), 3
        )
        run_summary["status"] = status
        run_summary["error_stage"] = error_stage
        run_summary["error_message"] = error_message
        self._enrich_monitoring_fields(run_summary)
        self._persist_monitoring_artifacts(run_summary)
        self.last_run_summary = dict(run_summary)

    def _record_batch_counts(
        self,
        run_summary: Dict[str, Any],
        fetched_count: int,
        cleaned_count: int,
        inserted_count: int,
    ) -> None:
        """Update summary counts for a fetched/cleaned/inserted batch."""
        run_summary["total_reviews_fetched"] += fetched_count
        run_summary["total_reviews_cleaned"] += cleaned_count
        run_summary["total_reviews_inserted"] += inserted_count
        run_summary["total_duplicates_or_existing"] += max(
            cleaned_count - inserted_count, 0
        )

    def get_last_run_summary(self) -> Dict[str, Any]:
        """Return the most recent pipeline run summary."""
        return dict(self.last_run_summary)

    def get_log_file_path(self) -> Optional[str]:
        """Return configured log file path."""
        return self.log_file

    def _load_previous_run_summary(
        self, mode: str, app_id: str
    ) -> Optional[Dict[str, Any]]:
        """Load most recent run summary for same mode/app from history."""
        if not self.monitoring_history_file:
            return None
        history_path = Path(self.monitoring_history_file)
        if not history_path.exists():
            return None

        try:
            lines = history_path.read_text(encoding="utf-8").splitlines()
        except Exception as exc:
            self.logger.warning(
                "Failed reading monitoring history: %s",
                str(exc),
            )
            return None

        for raw_line in reversed(lines):
            raw_line = raw_line.strip()
            if not raw_line:
                continue
            try:
                item = json.loads(raw_line)
            except json.JSONDecodeError:
                continue
            if item.get("mode") == mode and item.get("app_id") == app_id:
                return item
        return None

    def _calculate_change_pct(
        self, current_value: int, previous_value: int
    ) -> Optional[float]:
        """Calculate percentage change with zero-safe behavior."""
        if previous_value <= 0:
            return None
        return round(((current_value - previous_value) / previous_value) * 100, 2)

    def _enrich_monitoring_fields(self, run_summary: Dict[str, Any]) -> None:
        """Attach simple baseline comparison and anomaly flags."""
        previous = self._load_previous_run_summary(
            mode=run_summary.get("mode", ""),
            app_id=run_summary.get("app_id", ""),
        )
        anomalies = []
        monitoring = {
            "previous_run_id": None,
            "previous_status": None,
            "fetched_change_pct": None,
            "inserted_change_pct": None,
            "material_change_detected": False,
            "anomalies": anomalies,
        }

        if run_summary.get("status") == "failed":
            anomalies.append("run_failed")

        fetched_now = int(run_summary.get("total_reviews_fetched", 0) or 0)
        inserted_now = int(run_summary.get("total_reviews_inserted", 0) or 0)

        if previous:
            monitoring["previous_run_id"] = previous.get("run_id")
            monitoring["previous_status"] = previous.get("status")

            fetched_prev = int(previous.get("total_reviews_fetched", 0) or 0)
            inserted_prev = int(previous.get("total_reviews_inserted", 0) or 0)

            monitoring["fetched_change_pct"] = self._calculate_change_pct(
                fetched_now, fetched_prev
            )
            monitoring["inserted_change_pct"] = self._calculate_change_pct(
                inserted_now, inserted_prev
            )

            fetched_drop = monitoring["fetched_change_pct"]
            inserted_drop = monitoring["inserted_change_pct"]

            if fetched_drop is not None and fetched_drop <= -80:
                anomalies.append("material_fetched_drop")
            if inserted_drop is not None and inserted_drop <= -80:
                anomalies.append("material_inserted_drop")
            if inserted_now == 0 and inserted_prev > 0:
                anomalies.append("zero_inserted_after_nonzero_baseline")
            if fetched_now == 0 and fetched_prev > 0:
                anomalies.append("zero_fetched_after_nonzero_baseline")

        if anomalies:
            monitoring["material_change_detected"] = True

        run_summary["monitoring"] = monitoring

    def _persist_monitoring_artifacts(self, run_summary: Dict[str, Any]) -> None:
        """Persist monitoring artifacts to JSONL history and per-run JSON report."""
        try:
            if self.monitoring_history_file:
                history_path = Path(self.monitoring_history_file)
                history_path.parent.mkdir(parents=True, exist_ok=True)
                with history_path.open("a", encoding="utf-8") as history_file:
                    history_file.write(json.dumps(run_summary, default=str) + "\n")

            if self.monitoring_reports_dir:
                reports_dir = Path(self.monitoring_reports_dir)
                reports_dir.mkdir(parents=True, exist_ok=True)
                run_id = run_summary.get("run_id", "unknown_run")
                report_path = reports_dir / f"{run_id}.json"
                report_path.write_text(
                    json.dumps(run_summary, indent=2, default=str),
                    encoding="utf-8",
                )
                latest_path = reports_dir / "latest.json"
                latest_path.write_text(
                    json.dumps(run_summary, indent=2, default=str),
                    encoding="utf-8",
                )
        except Exception as exc:
            self.logger.warning(
                "Failed to persist monitoring artifacts: %s",
                str(exc),
            )

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
        run_summary = self._initialize_run_summary(
            mode="standard", target_reviews=total_reviews
        )
        run_summary["fetch_app_info"] = fetch_app_info

        print("=" * 80)
        print("DATA INGESTION PIPELINE")
        print("=" * 80)
        print(f"App ID: {self.app_id}")
        print(f"Target reviews: {total_reviews}")
        print(f"Database: {self.db_path}")
        print(f"Log file: {self.log_file or 'Disabled'}")
        print("=" * 80)
        self._log_event(
            logging.INFO,
            "run_started",
            run_summary,
            target_reviews=total_reviews,
            fetch_app_info=fetch_app_info,
        )

        try:
            # Use context manager for database operations
            with self.db_manager:
                # Step 1: Create schema if needed
                if not self.db_manager.create_schema():
                    print("Failed to create database schema")
                    self._finalize_run_summary(
                        run_summary,
                        status="failed",
                        error_stage="create_schema",
                        error_message="Failed to create database schema",
                    )
                    self._log_event(
                        logging.ERROR,
                        "run_failed",
                        run_summary,
                        error_stage="create_schema",
                        error_message="Failed to create database schema",
                    )
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
                            self._log_event(
                                logging.WARNING,
                                "app_info_clean_failed",
                                run_summary,
                            )
                    else:
                        print("Warning: Failed to fetch app info, continuing...")
                        self._log_event(
                            logging.WARNING,
                            "app_info_fetch_failed",
                            run_summary,
                        )
                else:
                    print("\n[Step 1/4] Skipping app info fetch...")
                    self._log_event(logging.INFO, "app_info_skipped", run_summary)

                # Step 3: Fetch reviews
                print("\n[Step 2/4] Fetching reviews...")
                reviews, continuation_token = self.scraper.fetch_reviews(
                    total_count=total_reviews
                )

                if not reviews:
                    print("No reviews fetched")
                    self._finalize_run_summary(
                        run_summary,
                        status="failed",
                        error_stage="fetch_reviews",
                        error_message="No reviews fetched",
                    )
                    self._log_event(
                        logging.ERROR,
                        "run_failed",
                        run_summary,
                        error_stage="fetch_reviews",
                        error_message="No reviews fetched",
                    )
                    return False

                # Step 4: Clean reviews
                print("\n[Step 3/4] Cleaning reviews...")
                cleaned_reviews_df = self.cleaner.clean_reviews(reviews)

                if cleaned_reviews_df.empty:
                    print("No valid reviews after cleaning")
                    self._finalize_run_summary(
                        run_summary,
                        status="failed",
                        error_stage="clean_reviews",
                        error_message="No valid reviews after cleaning",
                    )
                    self._log_event(
                        logging.ERROR,
                        "run_failed",
                        run_summary,
                        error_stage="clean_reviews",
                        error_message="No valid reviews after cleaning",
                    )
                    return False

                # Step 5: Load to database
                print("\n[Step 4/4] Loading data to database...")
                before_count = self.db_manager.get_review_count_by_app(self.app_id)
                success = self.db_manager.insert_reviews(
                    cleaned_reviews_df, self.app_id
                )
                after_count = self.db_manager.get_review_count_by_app(self.app_id)
                inserted_count = max(after_count - before_count, 0)
                self._record_batch_counts(
                    run_summary=run_summary,
                    fetched_count=len(reviews),
                    cleaned_count=len(cleaned_reviews_df),
                    inserted_count=inserted_count,
                )
                run_summary["batches_processed"] = 1

                if not success:
                    print("Failed to insert reviews into database")
                    self._finalize_run_summary(
                        run_summary,
                        status="failed",
                        error_stage="insert_reviews",
                        error_message="Failed to insert reviews into database",
                    )
                    self._log_event(
                        logging.ERROR,
                        "run_failed",
                        run_summary,
                        error_stage="insert_reviews",
                        error_message="Failed to insert reviews into database",
                    )
                    return False

                # Step 6: Display statistics
                self._display_statistics()
                self._finalize_run_summary(run_summary, status="success")
                self._log_event(
                    logging.INFO,
                    "run_completed",
                    run_summary,
                    total_reviews_fetched=run_summary["total_reviews_fetched"],
                    total_reviews_cleaned=run_summary["total_reviews_cleaned"],
                    total_reviews_inserted=run_summary["total_reviews_inserted"],
                    duplicates_or_existing=run_summary["total_duplicates_or_existing"],
                    duration_seconds=run_summary["duration_seconds"],
                    continuation_token_found=bool(continuation_token),
                )

                return True

        except Exception as e:
            print(f"\nPipeline error: {e}")
            self._finalize_run_summary(
                run_summary,
                status="failed",
                error_stage="unhandled_exception",
                error_message=str(e),
            )
            self._log_event(
                logging.ERROR,
                "run_failed",
                run_summary,
                error_stage="unhandled_exception",
                error_message=str(e),
            )
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
        print(f"Log file: {self.log_file or 'Disabled'}")
        print("=" * 80)
        run_summary = self._initialize_run_summary(mode="incremental")
        run_summary["fetch_app_info"] = True
        self._log_event(
            logging.INFO,
            "run_started",
            run_summary,
            batch_size=batch_size,
            max_batches=max_batches,
        )

        try:
            # Use context manager for database operations
            with self.db_manager:
                # Create schema
                if not self.db_manager.create_schema():
                    print("Failed to create database schema")
                    self._finalize_run_summary(
                        run_summary,
                        status="failed",
                        error_stage="create_schema",
                        error_message="Failed to create database schema",
                    )
                    self._log_event(
                        logging.ERROR,
                        "run_failed",
                        run_summary,
                        error_stage="create_schema",
                        error_message="Failed to create database schema",
                    )
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
                        before_count = self.db_manager.get_review_count_by_app(
                            self.app_id
                        )
                        success = self.db_manager.insert_reviews(
                            cleaned_reviews_df, self.app_id
                        )
                        after_count = self.db_manager.get_review_count_by_app(
                            self.app_id
                        )
                        inserted_count = max(after_count - before_count, 0)
                        self._record_batch_counts(
                            run_summary=run_summary,
                            fetched_count=len(reviews),
                            cleaned_count=len(cleaned_reviews_df),
                            inserted_count=inserted_count,
                        )
                        run_summary["batches_processed"] = batch_num
                        self._log_event(
                            logging.INFO,
                            "batch_processed",
                            run_summary,
                            batch_number=batch_num,
                            fetched_count=len(reviews),
                            cleaned_count=len(cleaned_reviews_df),
                            inserted_count=inserted_count,
                            continuation_token_found=bool(continuation_token),
                        )
                        if success:
                            total_fetched += len(cleaned_reviews_df)
                        else:
                            print(f"Warning: Failed to insert batch {batch_num}")
                            self._log_event(
                                logging.WARNING,
                                "batch_insert_failed",
                                run_summary,
                                batch_number=batch_num,
                            )
                    else:
                        run_summary["batches_processed"] = batch_num
                        self._log_event(
                            logging.WARNING,
                            "batch_clean_empty",
                            run_summary,
                            batch_number=batch_num,
                            fetched_count=len(reviews),
                        )

                    if not continuation_token:
                        print("Reached end of available reviews")
                        break

                print(f"\nIncremental fetch completed: {total_fetched:,} reviews")
                self._display_statistics()
                if run_summary["total_reviews_fetched"] == 0:
                    self._finalize_run_summary(
                        run_summary,
                        status="failed",
                        error_stage="fetch_reviews",
                        error_message="No reviews fetched during incremental run",
                    )
                    self._log_event(
                        logging.ERROR,
                        "run_failed",
                        run_summary,
                        error_stage="fetch_reviews",
                        error_message="No reviews fetched during incremental run",
                    )
                    return False
                self._finalize_run_summary(run_summary, status="success")
                self._log_event(
                    logging.INFO,
                    "run_completed",
                    run_summary,
                    batches_processed=run_summary["batches_processed"],
                    total_reviews_fetched=run_summary["total_reviews_fetched"],
                    total_reviews_cleaned=run_summary["total_reviews_cleaned"],
                    total_reviews_inserted=run_summary["total_reviews_inserted"],
                    duplicates_or_existing=run_summary["total_duplicates_or_existing"],
                    duration_seconds=run_summary["duration_seconds"],
                )

                return True

        except Exception as e:
            print(f"\nIncremental fetch error: {e}")
            self._finalize_run_summary(
                run_summary,
                status="failed",
                error_stage="unhandled_exception",
                error_message=str(e),
            )
            self._log_event(
                logging.ERROR,
                "run_failed",
                run_summary,
                error_stage="unhandled_exception",
                error_message=str(e),
            )
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
