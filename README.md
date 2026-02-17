### Setup

1. Clone or download this repository:

```bash
cd data_ingestion_system
```

2. Install required dependencies:

```bash
pip install -r requirements.txt
```

## Quick Start

### Basic Usage

Fetch reviews for the default app (ChatGPT):

```bash
python scripts/run_pipeline.py --reviews 100
```

This will:

- Fetch app information
- Fetch 100 reviews
- Clean and normalize the data
- Store everything in the database at `data/database/reviews.db`

### Fetch Reviews for a Specific App

```bash
python scripts/run_pipeline.py --reviews 1000 --app-id com.example.app
```

### Incremental Fetching

For large datasets, use incremental fetching:

```bash
python scripts/run_pipeline.py --incremental --batch-size 500 --max-batches 10
```

This fetches reviews in batches (500 per batch, up to 10 batches = 5000 reviews total).

### Scheduled (Unattended) Runs

Use the scheduler-safe runner for cron/automation:

```bash
python scripts/scheduled_run.py --mode standard --reviews 200
```

Or incremental mode:

```bash
python scripts/scheduled_run.py --mode incremental --batch-size 200 --max-batches 5
```

This runner includes a lock file guard to avoid overlapping executions.

## Usage Guide

### Command-Line Options

#### Mode Selection (Required - choose one)

- `--reviews N`: Fetch N reviews (standard mode)
- `--incremental`: Use incremental fetch mode

#### App Configuration

- `--app-id APP_ID`: Google Play app ID (default: `com.openai.chatgpt`)
- `--lang LANG`: Language code (default: `en`)
- `--country COUNTRY`: Country code (default: `us`)

#### Database Configuration

- `--db-path PATH`: Path to SQLite database (default: `data/database/reviews.db`)
- `--log-file PATH`: Path to pipeline logs (default: `logs/pipeline.log`)

#### Pipeline Options

- `--no-app-info`: Skip fetching app information
- `--batch-size N`: Batch size for scraping (default: 200)
- `--max-batches N`: Maximum number of batches for incremental mode (default: unlimited)

#### Scheduler Options (scheduled_run.py)

- `--mode standard|incremental`: Run mode for scheduled jobs
- `--lock-file PATH`: Lock file path to prevent overlap
- `--allow-concurrent`: Disable lock guard (not recommended)

### Examples

#### Example 1: Quick Test

```bash
# Fetch 10 reviews for quick testing
python scripts/run_pipeline.py --reviews 10
```

#### Example 2: Production Run

```bash
# Fetch 1000 reviews for ChatGPT app
python scripts/run_pipeline.py --reviews 1000 --app-id com.openai.chatgpt
```

#### Example 3: Incremental Fetching

```bash
# Fetch reviews in batches: 10 batches of 500 reviews each
python scripts/run_pipeline.py --incremental --batch-size 500 --max-batches 10
```

#### Example 4: Skip App Info

```bash
# Fetch only reviews, skip app information
python scripts/run_pipeline.py --reviews 100 --no-app-info
```

#### Example 5: Custom Database Location

```bash
# Use a custom database path
python scripts/run_pipeline.py --reviews 100 --db-path custom/path/reviews.db
```

#### Example 6: Save Logs to a Custom File

```bash
# Save structured logs to a custom path
python scripts/run_pipeline.py --reviews 100 --log-file logs/custom_run.log
```

#### Example 7: Scheduler-Safe Daily Run

```bash
# Scheduler-safe standard run with lock protection
python scripts/scheduled_run.py --mode standard --reviews 200 --log-file logs/scheduled.log
```

### View Help

```bash
python scripts/run_pipeline.py --help
```

## Configuration

Default configuration is defined in `src/config.py`. You can modify:

- `app_id`: Default Google Play app ID
- `lang`: Default language code
- `country`: Default country code
- `batch_size`: Default batch size for scraping
- `db_path`: Default database path (automatically resolved to project root)
- `log_file`: Default structured log output path
- `monitoring_history_file`: JSONL history of run summaries
- `monitoring_reports_dir`: Directory for per-run monitoring reports
- `scheduler_lock_file`: Lock file path used by scheduled runs
- `default_reviews_count`: Default number of reviews to fetch
- `fetch_app_info`: Whether to fetch app information by default

## Testing Individual Modules

Each module has a corresponding test script for quick testing:

### Test Scraper

```bash
python scripts/test_scraper.py
```

### Test Data Cleaner

```bash
python scripts/test_data_cleaner.py
```

### Test Database Manager

```bash
python scripts/test_db_manager.py
```

### Test Pipeline

```bash
python scripts/test_pipeline.py
```

## How It Works

### Data Flow

```
1. Scrape → Fetch app info and reviews from Google Play Store
2. Clean → Validate, normalize, and remove duplicates
3. Store → Insert into SQLite database with proper schema
```

### Duplicate Prevention

- Reviews are deduplicated based on `review_id` (PRIMARY KEY)
- Uses `INSERT OR IGNORE` to prevent duplicate inserts
- Database automatically handles duplicates at the schema level

### Monitoring and Observability

Each run writes structured events and a run summary with:

- Run status (`success` / `failed`)
- Counts: fetched, cleaned, inserted, duplicates/existing
- Timing: start, end, duration
- Error context (`error_stage`, `error_message`)
- Baseline comparison versus previous run (same app + mode)
- Basic anomaly flags (for example, large volume drops or zero inserts after prior non-zero runs)

Monitoring artifacts:

- `data/monitoring/run_history.jsonl`: append-only run history
- `data/monitoring/reports/<run_id>.json`: per-run report
- `data/monitoring/reports/latest.json`: latest run snapshot

Note: Monitoring artifacts are generated by `DataPipeline` itself, so both
`scripts/run_pipeline.py` and `scripts/scheduled_run.py` produce these reports.
`scripts/scheduled_run.py` adds scheduler-safe behavior (lock guard + skip code).

### Scheduling and Locking Behavior

`scripts/scheduled_run.py` uses a lock file (default: `data/runtime/pipeline_scheduler.lock`) to avoid overlapping executions.

Behavior:

- If a lock exists and its PID is still running, the new run is skipped with exit code `2`.
- If a lock exists but PID is not running, it is treated as stale and removed.
- On normal completion/error, lock is automatically released.

## Database Schema

### Apps Table

- `app_id` (PRIMARY KEY): Google Play app ID
- `title`, `developer`, `genre`, `category`: App metadata
- `score`, `ratings`, `reviews_count`: Rating information
- `released`, `last_updated`: Date information
- `raw_data`: Complete JSON data for reference

### Reviews Table

- `review_id` (PRIMARY KEY): Unique review identifier
- `app_id` (FOREIGN KEY): Associated app
- `user_name`, `score`, `content`: Review content
- `reviewed_at`: Review timestamp
- `raw_data`: Complete JSON data for reference

## Common Use Cases

### Daily Updates

```bash
# Recommended for unattended daily runs
python scripts/scheduled_run.py --mode standard --reviews 200 --log-file logs/daily.log
```

### Initial Data Collection

```bash
# Fetch a large initial dataset
python scripts/run_pipeline.py --incremental --batch-size 1000 --max-batches 50
```

### Testing New App

```bash
# Test with a different app
python scripts/run_pipeline.py --reviews 50 --app-id com.example.app
```

## Data Source and Scraper Trade-offs

This project uses Google Play reviews and the `google-play-scraper` library for ingestion.

Why this choice:

- High-volume, user-generated review data with useful metadata (ratings, timestamps, versions)
- Publicly accessible source suitable for prototyping
- Fast implementation path with stable Python tooling and pagination support
- Good fit for validating end-to-end ingestion architecture before multi-source expansion

Trade-offs and limitations:

- Dependency on third-party scraping behavior and upstream page/API changes
- Less contractual stability compared to official paid APIs
- Data model is source-specific, so cross-source standardization is future work

Decision rationale:

- For this phase, speed of learning and infrastructure validation was prioritized over broad source coverage.
- The modular architecture (`scraper -> cleaner -> storage`) keeps the door open to add alternative sources later without rewriting the full pipeline.

## SQLite vs PostgreSQL

SQLite is the default and recommended choice for this prototype stage:

- Single-node local execution
- Lightweight deployment
- Fast iteration for ingestion + validation

Consider moving to PostgreSQL when one or more conditions are true:

- Multiple concurrent writers/readers are required
- Query complexity and analytical workload increase substantially
- The pipeline is deployed as a shared service/cloud workload

For current scope, keep SQLite and prioritize pipeline stability, scheduling, and monitoring.

## Operations Runbook (Quick)

### 1) Run the pipeline

```bash
python scripts/run_pipeline.py --reviews 100
```

### 2) Run scheduler-safe mode

```bash
python scripts/scheduled_run.py --mode standard --reviews 200
```

### 3) Verify success

- Check terminal exit code (`0` means success)
- Check latest monitoring report: `data/monitoring/reports/latest.json`
- Check structured logs: `logs/pipeline.log` (or your custom log file)

### 4) Troubleshoot quickly

- If run is skipped due to lock, verify active process first.
- If lock is stale, re-run command; stale lock should auto-clean.
- If inserted count is unexpectedly low/zero, compare with previous run in `run_history.jsonl` and review anomaly flags.

## Dependencies

- `google-play-scraper==1.2.7`: Google Play Store scraping
- `pandas==2.0.3`: Data manipulation

See `requirements.txt` for complete list.
