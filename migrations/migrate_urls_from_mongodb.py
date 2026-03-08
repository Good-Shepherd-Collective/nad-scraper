#!/usr/bin/env python3
"""
Migrate URLs from MongoDB's `Urls` collection to Neon Postgres `urls` table.
Uses executemany for batch performance.

Usage:
    python migrations/migrate_urls_from_mongodb.py [--dry-run]
"""

import argparse
import logging
import os
import sys

from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

import psycopg

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def get_mongo_db():
    uri = os.getenv("MONGO_URI", "")
    user = os.getenv("MONGO_USER", "")
    password = os.getenv("MONGO_PASSWORD", "")
    db_name = os.getenv("MONGO_DB_NAME", "")

    if uri.startswith("mongodb"):
        conn_str = uri
    else:
        conn_str = f"mongodb+srv://{user}:{password}@{uri}/{db_name}?retryWrites=true&w=majority"

    client = MongoClient(conn_str)
    return client[db_name]


def migrate(dry_run=False):
    mongo_db = get_mongo_db()
    collection_name = os.getenv("MONGO_COLLECTION_URLS", "Urls")
    collection = mongo_db[collection_name]

    total = collection.count_documents({})
    logger.info(f"Found {total} URLs in MongoDB '{collection_name}' collection")

    docs = list(collection.find())
    logger.info(f"Loaded {len(docs)} URL documents")

    # Prepare all rows
    rows = []
    for doc in docs:
        link = doc.get("link", "")
        if not link:
            continue
        rows.append((doc.get("date", ""), doc.get("title", ""), link))

    logger.info(f"Prepared {len(rows)} valid URL rows")

    if dry_run:
        logger.info(f"[DRY RUN] Would migrate {len(rows)} URLs")
        return

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        logger.error("DATABASE_URL not set")
        sys.exit(1)

    conn = psycopg.connect(database_url)

    # Batch insert using executemany
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO urls (date, title, link)
            VALUES (%s, %s, %s)
            ON CONFLICT (link) DO NOTHING
            """,
            rows,
        )
        inserted = cur.rowcount

    conn.commit()
    conn.close()

    logger.info(f"Migration complete: {inserted} inserted, {len(rows) - inserted} skipped (duplicates)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    migrate(dry_run=args.dry_run)
