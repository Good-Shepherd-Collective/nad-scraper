# CLAUDE.md

## Project Overview

This project scrapes daily incident reports from the PLO Negotiations Affairs Department (NAD) website (nad.ps), translates them from Arabic to English using MiniMax M2.5, and stores them in Neon Postgres. It is part of the Good Shepherd Collective's effort to centralize and automate data collection on incidents in Palestine.

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

# Backfill translations with MiniMax
python backfill_translations.py [--limit N] [--date YYYY.MM.DD] [--dry-run]

# Test MiniMax translation quality
python test_minimax_translation.py [--date YYYY.MM.DD] [--sample N]

# Migration from MongoDB (completed, one-time only — kept for reference)
# python migrations/migrate_from_mongodb.py [--dry-run] [--limit N]
```

## Architecture

### Data Flow
1. `scripts/collect_urls.py` - Collects report URLs from NAD website, stores in Postgres `urls` table
2. `scraper.py` - Processes unscraped URLs: scrapes page content, Highcharts data, and narrative data, stores in `nad_reports` and `nad_narrative_violations` tables
3. `main.py` - Orchestrates both steps

### Translation System
Two-tier approach:
1. Custom dictionary for common terms (`translations.py` - CUSTOM_TRANSLATIONS)
2. MiniMax M2.5 API for narrative descriptions (`minimax_translate.py`)
3. Normalization layer (`translations.py`) to fix inconsistent translations

### Database Structure (Neon Postgres)
- Project: `gsc-nad-reports` on cody@goodshepherdcollective.org
- Tables:
  - `urls`: Collected report URLs with dates
  - `nad_reports`: Scraped report data with chart stats (raw_data JSONB)
  - `nad_narrative_violations`: Individual violation details (normalized, FK to nad_reports)
  - `ingestion_log`: Tracks each scraper run
  - `sources`: Data source metadata
  - `data_quality`: Data quality flags

### GitHub Actions Workflows
- `daily-scraper.yml`: Runs daily at midnight UTC - collects URLs and scrapes new reports
- `backfill-translations.yml`: Manual dispatch - retranslates old reports with MiniMax

## Environment Variables

Required (set in .env or GitHub secrets):
- `DATABASE_URL` - Neon Postgres direct connection string
- `DATABASE_URL_POOLED` - Neon Postgres pooled connection string
- `MINIMAX_API_KEY` - MiniMax API key for translations

Legacy (migration completed, no longer needed):
- MongoDB credentials were used for the one-time migration to Neon Postgres (see `migrations/`). The project now uses Neon exclusively.

## Adding New Translation Normalizations

Edit `translations.py` and add entries to `TRANSLATION_NORMALIZATIONS` dict:
```python
'Incorrect Variant': 'Correct Translation',
```
