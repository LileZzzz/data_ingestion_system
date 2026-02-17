"""
Pipeline Runner Script

A script to run the data ingestion pipeline.

Usage:
    python scripts/run_pipeline.py --reviews 100
    python scripts/run_pipeline.py --reviews 1000 --app-id com.example.app
    python scripts/run_pipeline.py --incremental --batch-size 500 --max-batches 10
    python scripts/run_pipeline.py --reviews 100 --no-app-info

Author: Lile Zhang
Date: January 2026
"""

import argparse
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

import config
from src.data_pipeline import DataPipeline


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Run the data ingestion pipeline to fetch and store Google Play reviews",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Fetch 100 reviews with default settings
  python scripts/run_pipeline.py --reviews 100

  # Fetch 1000 reviews for a specific app
  python scripts/run_pipeline.py --reviews 1000 --app-id com.example.app

  # Incremental fetch: 10 batches of 500 reviews each
  python scripts/run_pipeline.py --incremental --batch-size 500 --max-batches 10

  # Fetch reviews without app info
  python scripts/run_pipeline.py --reviews 100 --no-app-info

  # Use custom database path
  python scripts/run_pipeline.py --reviews 100 --db-path custom/path/reviews.db
        """,
    )

    # Mode selection
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument(
        "--reviews",
        type=int,
        metavar="N",
        help="Number of reviews to fetch (run mode)",
    )
    mode_group.add_argument(
        "--incremental",
        action="store_true",
        help="Use incremental fetch mode (batch processing)",
    )

    # App configuration
    parser.add_argument(
        "--app-id",
        type=str,
        default=None,
        help=f"Google Play app ID (default: {config.DEFAULT_CONFIG['app_id']})",
    )
    parser.add_argument(
        "--lang",
        type=str,
        default=None,
        help=f"Language code (default: {config.DEFAULT_CONFIG['lang']})",
    )
    parser.add_argument(
        "--country",
        type=str,
        default=None,
        help=f"Country code (default: {config.DEFAULT_CONFIG['country']})",
    )

    # Database configuration
    parser.add_argument(
        "--db-path",
        type=str,
        default=None,
        help=f"Path to SQLite database (default: {config.DEFAULT_CONFIG['db_path']})",
    )
    parser.add_argument(
        "--log-file",
        type=str,
        default=None,
        help=(
            "Path to save structured logs "
            f"(default: {config.DEFAULT_CONFIG.get('log_file', 'disabled')})"
        ),
    )

    # Pipeline options
    parser.add_argument(
        "--no-app-info",
        action="store_true",
        help="Skip fetching app information",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help=f"Batch size for scraping (default: {config.DEFAULT_CONFIG['batch_size']})",
    )

    # Incremental mode options
    parser.add_argument(
        "--max-batches",
        type=int,
        default=None,
        help="Maximum number of batches for incremental mode (default: unlimited)",
    )

    return parser.parse_args()


def run_pipeline(args):
    """Run the pipeline based on arguments"""
    # Initialize pipeline
    pipeline = DataPipeline(
        app_id=args.app_id,
        db_path=args.db_path,
        lang=args.lang,
        country=args.country,
        batch_size=args.batch_size,
        log_file=args.log_file,
    )

    print("=" * 80)
    print("PIPELINE CONFIGURATION")
    print("=" * 80)
    print(f"App ID:        {pipeline.app_id}")
    print(f"Database:      {pipeline.db_path}")
    print(f"Language:      {pipeline.scraper.lang}")
    print(f"Country:       {pipeline.scraper.country}")
    print(f"Batch Size:    {pipeline.scraper.batch_size}")
    print(f"Log File:      {pipeline.get_log_file_path() or 'Disabled'}")
    print("=" * 80)

    # Run based on mode
    if args.reviews is not None:
        # Standard run mode
        print(f"\nRunning pipeline to fetch {args.reviews} reviews...")
        success = pipeline.run(
            total_reviews=args.reviews,
            fetch_app_info=not args.no_app_info,
        )
    else:
        # Incremental fetch mode
        batch_size = args.batch_size or config.DEFAULT_CONFIG["batch_size"]
        print(f"\nRunning incremental fetch...")
        print(f"Batch size: {batch_size}")
        print(f"Max batches: {args.max_batches or 'Unlimited'}")
        success = pipeline.incrementally_fetch(
            batch_size=batch_size,
            max_batches=args.max_batches,
        )

    return success


def main():
    """Main entry point"""
    try:
        args = parse_arguments()
        success = run_pipeline(args)

        if success:
            print("\n" + "=" * 80)
            print("Pipeline completed successfully!")
            print("=" * 80)
            sys.exit(0)
        else:
            print("\n" + "=" * 80)
            print("Pipeline completed with errors.")
            print("=" * 80)
            sys.exit(1)

    except KeyboardInterrupt:
        print("\n\nPipeline interrupted by user.")
        sys.exit(130)
    except Exception as e:
        print(f"\n\nError: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
