"""
Scheduled Pipeline Runner

Lightweight entrypoint for unattended runs （task scheduler).
Includes a simple lock file guard to avoid overlapping executions.

Examples:
  python scripts/scheduled_run.py --mode standard --reviews 200
  python scripts/scheduled_run.py --mode incremental --batch-size 200 --max-batches 3

Author: Lile Zhang
Date: February 2026
"""

import argparse
import json
import os
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Optional

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

import config
from src.data_pipeline import DataPipeline


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Run scheduled ingestion with lock-file protection",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Standard unattended run
  python scripts/scheduled_run.py --mode standard --reviews 200

  # Incremental unattended run
  python scripts/scheduled_run.py --mode incremental --batch-size 200 --max-batches 5
        """,
    )

    parser.add_argument(
        "--mode",
        choices=["standard", "incremental"],
        default="standard",
        help="Pipeline mode (default: standard)",
    )
    parser.add_argument(
        "--reviews",
        type=int,
        default=config.DEFAULT_CONFIG["default_reviews_count"],
        help="Reviews target for standard mode",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=config.DEFAULT_CONFIG["batch_size"],
        help="Batch size for incremental mode",
    )
    parser.add_argument(
        "--max-batches",
        type=int,
        default=None,
        help="Max batches for incremental mode (default: unlimited)",
    )
    parser.add_argument(
        "--no-app-info",
        action="store_true",
        help="Skip app info fetch in standard mode",
    )

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
            "Path to pipeline log file "
            f"(default: {config.DEFAULT_CONFIG.get('log_file', 'disabled')})"
        ),
    )
    parser.add_argument(
        "--lock-file",
        type=str,
        default=config.DEFAULT_CONFIG["scheduler_lock_file"],
        help=(
            "Path to scheduler lock file "
            f"(default: {config.DEFAULT_CONFIG['scheduler_lock_file']})"
        ),
    )
    parser.add_argument(
        "--allow-concurrent",
        action="store_true",
        help="Disable lock guard (not recommended)",
    )
    return parser.parse_args()


def _is_pid_running(pid: int) -> bool:
    """Check whether a process is currently running."""
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        # Process exists but we cannot signal it.
        return True


@contextmanager
def lock_guard(lock_file: str) -> Iterator[None]:
    """Acquire file-based lock to avoid overlapping scheduled runs."""
    lock_path = Path(lock_file)
    lock_path.parent.mkdir(parents=True, exist_ok=True)

    if lock_path.exists():
        existing_text = lock_path.read_text(encoding="utf-8").strip()
        existing_pid: Optional[int] = None
        if existing_text:
            try:
                existing_payload = json.loads(existing_text)
                existing_pid = int(existing_payload.get("pid", 0))
            except (json.JSONDecodeError, TypeError, ValueError):
                existing_pid = None

        if existing_pid and _is_pid_running(existing_pid):
            raise RuntimeError(
                f"Another scheduled run is active (pid={existing_pid}). "
                f"Lock file: {lock_file}"
            )

        # Stale or invalid lock file.
        lock_path.unlink(missing_ok=True)

    payload = {
        "pid": os.getpid(),
        "lock_file": str(lock_path),
    }
    lock_path.write_text(json.dumps(payload), encoding="utf-8")

    try:
        yield
    finally:
        lock_path.unlink(missing_ok=True)


def run_scheduled_pipeline(args: argparse.Namespace) -> bool:
    """Run scheduled pipeline in selected mode."""
    pipeline = DataPipeline(
        app_id=args.app_id,
        db_path=args.db_path,
        lang=args.lang,
        country=args.country,
        batch_size=args.batch_size,
        log_file=args.log_file,
    )

    print("=" * 80)
    print("SCHEDULED PIPELINE RUN")
    print("=" * 80)
    print(f"Mode:          {args.mode}")
    print(f"App ID:        {pipeline.app_id}")
    print(f"Database:      {pipeline.db_path}")
    print(f"Log File:      {pipeline.get_log_file_path() or 'Disabled'}")
    print(
        f"Lock File:     {args.lock_file if not args.allow_concurrent else 'Disabled'}"
    )
    print("=" * 80)

    if args.mode == "standard":
        success = pipeline.run(
            total_reviews=args.reviews,
            fetch_app_info=not args.no_app_info,
        )
    else:
        success = pipeline.incrementally_fetch(
            batch_size=args.batch_size,
            max_batches=args.max_batches,
        )

    print("\nRun Summary:")
    print(json.dumps(pipeline.get_last_run_summary(), indent=2, default=str))
    return success


def main() -> None:
    """Main entrypoint for scheduler-friendly execution."""
    args = parse_arguments()
    try:
        if args.allow_concurrent:
            success = run_scheduled_pipeline(args)
        else:
            with lock_guard(args.lock_file):
                success = run_scheduled_pipeline(args)

        sys.exit(0 if success else 1)
    except RuntimeError as lock_error:
        print(f"Skipped scheduled run: {lock_error}")
        sys.exit(2)
    except KeyboardInterrupt:
        print("\nScheduled run interrupted by user.")
        sys.exit(130)
    except Exception as exc:
        print(f"Scheduled run failed: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
