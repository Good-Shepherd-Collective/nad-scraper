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
from pymongo.server_api import ServerApi
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db import get_mongo_client as _get_mongo_client, MONGO_DB_NAME

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_mongo_client():
    """Create and return MongoDB client."""
    client = _get_mongo_client(server_api=ServerApi('1'), serverSelectionTimeoutMS=5000)
    client.admin.command('ping')
    return client


def get_all_url_dates(db):
    """Get all dates from the Urls collection."""
    collection = db["Urls"]
    dates = collection.distinct("date")
    return set(dates)


def get_scraped_urls(db):
    """Get all scraped URLs from new_daily_reports collection."""
    collection = db["new_daily_reports"]
    urls = collection.distinct("Source URL")
    return set(urls)


def get_all_urls(db):
    """Get all URLs from Urls collection."""
    collection = db["Urls"]
    docs = collection.find({}, {"date": 1, "link": 1, "_id": 0})
    return {doc["link"]: doc["date"] for doc in docs}


def find_missing_dates(url_dates, start_date, end_date):
    """Find dates missing from the URL collection within a date range."""
    missing = []
    current = start_date

    while current <= end_date:
        date_str = current.strftime("%Y-%m-%d")
        if date_str not in url_dates:
            # Skip Saturdays (reports might not be published)
            if current.weekday() != 5:  # 5 = Saturday
                missing.append(date_str)
        current += timedelta(days=1)

    return missing


def find_unscraped_urls(db):
    """Find URLs that haven't been scraped yet."""
    all_urls = get_all_urls(db)
    scraped_urls = get_scraped_urls(db)

    unscraped = {}
    for url, date in all_urls.items():
        if url not in scraped_urls:
            unscraped[url] = date

    return unscraped


def main():
    parser = argparse.ArgumentParser(description="Check for missing dates and unscraped URLs")
    parser.add_argument("--days", type=int, default=30, help="Number of days to check (default: 30)")
    parser.add_argument("--start-date", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument("--show-unscraped", action="store_true", help="Show unscraped URLs")
    args = parser.parse_args()

    # Connect to MongoDB
    try:
        client = get_mongo_client()
        db = client[MONGO_DB_NAME]
        logger.info("Connected to MongoDB")
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        return 1

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

    # Get all dates from URLs collection
    url_dates = get_all_url_dates(db)
    logger.info(f"Total dates in Urls collection: {len(url_dates)}")

    # Find missing dates
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

    # Check unscraped URLs
    if args.show_unscraped:
        unscraped = find_unscraped_urls(db)

        if unscraped:
            print(f"\n{'='*50}")
            print(f"UNSCRAPED URLs ({len(unscraped)} found):")
            print('='*50)
            for url, date in sorted(unscraped.items(), key=lambda x: x[1], reverse=True):
                print(f"  {date}: {url}")
        else:
            print(f"\nAll URLs have been scraped.")
    else:
        # Just show count
        unscraped = find_unscraped_urls(db)
        print(f"\nUnscraped URLs: {len(unscraped)} (use --show-unscraped to list)")

    # Summary
    print(f"\n{'='*50}")
    print("SUMMARY:")
    print('='*50)
    print(f"  Date range checked: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print(f"  Total URLs in database: {len(get_all_urls(db))}")
    print(f"  Missing dates: {len(missing_dates)}")
    print(f"  Unscraped URLs: {len(unscraped)}")

    client.close()
    return 0


if __name__ == "__main__":
    exit(main())
