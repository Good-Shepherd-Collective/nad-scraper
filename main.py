#!/usr/bin/env python3
"""
Daily Reports Scraper - Entry Point

This script orchestrates the daily scraping workflow:
1. Collect new report URLs from NAD website
2. Process all unscraped URLs and store in Neon Postgres
"""

import sys
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Run the complete scraping workflow."""
    logger.info("=" * 50)
    logger.info("Starting Daily Reports Scraper")
    logger.info("=" * 50)

    # Step 1: Collect new URLs
    logger.info("\n[Step 1/2] Collecting new report URLs...")
    try:
        from scripts.collect_urls import main as collect_urls
        collect_urls()
        logger.info("URL collection complete.")
    except Exception as e:
        logger.error(f"URL collection failed: {e}")
        # Continue to scraping - there may be existing unscraped URLs

    # Step 2: Process unscraped URLs
    logger.info("\n[Step 2/2] Processing unscraped URLs...")
    try:
        from scraper import main as run_scraper
        run_scraper()
        logger.info("Scraping complete.")
    except Exception as e:
        logger.error(f"Scraping failed: {e}")
        sys.exit(1)

    logger.info("\n" + "=" * 50)
    logger.info("Daily Reports Scraper finished successfully")
    logger.info("=" * 50)


if __name__ == "__main__":
    main()
