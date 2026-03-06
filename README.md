# nad-scraper

Scrapes daily incident reports from the [PLO Negotiations Affairs Department](https://www.nad.ps/) (NAD), translates Arabic to English, and stores structured data in MongoDB.

Part of the [Good Shepherd Collective](https://goodshepherdcollective.org) data pipeline.

## What it does

1. **Collects URLs** from the NAD daily reports listing page
2. **Scrapes each report** extracting:
   - Highcharts incident statistics (type + count)
   - Narrative violation details (region, governorate, type, description)
3. **Translates** Arabic text to English using:
   - Custom dictionary for common terms (incident types, place names)
   - MiniMax M2.5 API for narrative descriptions
4. **Stores** structured data in MongoDB

## Setup

```bash
git clone https://github.com/Good-Shepherd-Collective/nad-scraper.git
cd nad-scraper
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# Edit .env with your MongoDB and MiniMax credentials
```

## Usage

```bash
# Full daily workflow (collect URLs + scrape new reports)
python main.py

# Just scrape unprocessed URLs
python scraper.py

# Utility scripts
python -m scripts.collect_urls      # Collect new report URLs
python -m scripts.check_missing     # Find missing dates
python -m scripts.deduplicate       # Remove DB duplicates
python -m scripts.create_indexes    # Create MongoDB indexes

# Translation tools
python backfill_translations.py     # Retranslate with MiniMax
python test_minimax_translation.py  # Compare translation quality
```

## CI/CD

- **Daily Scraper** (`daily-scraper.yml`): Runs daily at midnight UTC via GitHub Actions
- **Backfill Translations** (`backfill-translations.yml`): Manual dispatch for bulk retranslation

## Related

- **[nad-reports](https://github.com/Good-Shepherd-Collective/nad-reports)** - Report generation and analysis tools
