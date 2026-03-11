#!/usr/bin/env python3
"""
Check for missing dates and unscraped URLs in the database.

Usage:
    python scripts/check_missing.py [--days N] [--start-date YYYY-MM-DD] [--end-date YYYY-MM-DD]
"""

import os
import sys
import argparse
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db import get_conn, get_existing_url_dates, get_unscraped_urls

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def find_missing_dates(url_dates, start_date, end_date):
    """Find dates missing from the urls table within a date range."""
    missing = []
    current = start_date

    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")
        if date_str not in url_dates:
            if current.weekday() != 5:  # Skip Saturdays
                missing.append(date_str)
        current += timedelta(days=1)

    return missing


def main():
    parser = argparse.ArgumentParser(description="Check for missing dates and unscraped URLs")
    parser.add_argument("--days", type=int, default=30, help="Number of days to check (default: 30)")
    parser.add_argument("--start-date", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument("--show-unscraped", action="store_true", help="Show unscraped URLs")
    args = parser.parse_args()

    # Determine date range
    if args.end_date:
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
    else:
        end_date = datetime.now()

    if args.start_date:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    else:
        start_date = end_date - timedelta(days=args.days)

    logger.info(f"Checking date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

    url_dates = get_existing_url_dates()
    logger.info(f"Total dates in urls table: {len(url_dates)}")

    missing_dates = find_missing_dates(url_dates, start_date, end_date)

    if missing_dates:
        print(f"\n{'='*50}")
        print(f"MISSING DATES ({len(missing_dates)} found):")
        print('='*50)
        for date in sorted(missing_dates):
            weekday = datetime.strptime(date, "%Y-%m-%d").strftime("%A")
            print(f"  {date} ({weekday})")
    else:
        print(f"\nNo missing dates in the specified range.")

    unscraped = get_unscraped_urls()

    if args.show_unscraped and unscraped:
        print(f"\n{'='*50}")
        print(f"UNSCRAPED URLs ({len(unscraped)} found):")
        print('='*50)
        for url in unscraped:
            print(f"  {url}")
    else:
        print(f"\nUnscraped URLs: {len(unscraped)}")

    # Summary
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM urls")
            total_urls = cur.fetchone()[0]

    print(f"\n{'='*50}")
    print("SUMMARY:")
    print('='*50)
    print(f"  Date range checked: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print(f"  Total URLs in database: {total_urls}")
    print(f"  Missing dates: {len(missing_dates)}")
    print(f"  Unscraped URLs: {len(unscraped)}")

    return 0


if __name__ == "__main__":
    exit(main())
