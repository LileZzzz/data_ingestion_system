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

#### Pipeline Options

- `--no-app-info`: Skip fetching app information
- `--batch-size N`: Batch size for scraping (default: 200)
- `--max-batches N`: Maximum number of batches for incremental mode (default: unlimited)

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
# Run daily to fetch new reviews
python scripts/run_pipeline.py --reviews 100
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

## Dependencies

- `google-play-scraper==1.2.7`: Google Play Store scraping
- `pandas==2.0.3`: Data manipulation

See `requirements.txt` for complete list.