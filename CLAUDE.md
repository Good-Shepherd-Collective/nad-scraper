# CLAUDE.md

## Project Overview

This project scrapes daily incident reports from the PLO Negotiations Affairs Department (NAD) website (nad.ps), translates them from Arabic to English using MiniMax M2.5, and stores them in MongoDB. It is part of the Good Shepherd Collective's effort to centralize and automate data collection on incidents in Palestine.

For report generation and analysis tools, see the companion repo: **nad-reports**.

## Key Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run the full workflow (collect URLs then scrape)
python main.py

# Run just the scraper (processes unscraped URLs from database)
python scraper.py

# Run just the URL collector
python -m scripts.collect_urls

# Check for missing dates in database
python -m scripts.check_missing

# Remove duplicate entries
python -m scripts.deduplicate

# Create MongoDB indexes
python -m scripts.create_indexes

# Backfill translations with MiniMax
python backfill_translations.py [--limit N] [--date YYYY.MM.DD] [--dry-run]

# Test MiniMax translation quality
python test_minimax_translation.py [--date YYYY.MM.DD] [--sample N]
```

## Architecture

### Data Flow
1. `scripts/collect_urls.py` - Collects report URLs from NAD website, stores in MongoDB `Urls` collection
2. `scraper.py` - Processes unscraped URLs: scrapes page content, Highcharts data, and narrative data, stores in `new_daily_reports` collection
3. `main.py` - Orchestrates both steps

### Translation System
Two-tier approach:
1. Custom dictionary for common terms (`translations.py` - CUSTOM_TRANSLATIONS)
2. MiniMax M2.5 API for narrative descriptions (`minimax_translate.py`)
3. Normalization layer (`translations.py`) to fix inconsistent translations

Key translations include incident types (Airstrikes, Deaths, Settler attacks) and place names (e.g., جنين -> Jenin, not "fetal").

### Database Structure (MongoDB)
- Database name: from `MONGO_DB_NAME` env var
- Collections:
  - `Urls`: Report URLs with dates
  - `new_daily_reports`: Scraped report data with chart stats and narrative details

### GitHub Actions Workflows
- `daily-scraper.yml`: Runs daily at midnight UTC - collects URLs and scrapes new reports
- `backfill-translations.yml`: Manual dispatch - retranslates old reports with MiniMax

## Environment Variables

Required (set in .env or GitHub secrets):
- `MONGO_URI` - MongoDB connection string or cluster hostname
- `MONGO_USER` - MongoDB username
- `MONGO_PASSWORD` - MongoDB password
- `MONGO_DB_NAME` - Database name
- `MINIMAX_API_KEY` - MiniMax API key for translations
- `MONGO_COLLECTION_URLS` - URL collection name (default: "Urls")

## Adding New Translation Normalizations

Edit `translations.py` and add entries to `TRANSLATION_NORMALIZATIONS` dict:
```python
'Incorrect Variant': 'Correct Translation',
```
