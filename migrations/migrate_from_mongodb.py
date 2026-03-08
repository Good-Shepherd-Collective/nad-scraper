#!/usr/bin/env python3
"""
Migrate NAD daily reports from MongoDB to Neon Postgres.

Reads documents from MongoDB's `new_daily_reports` collection and inserts
them into Neon's `nad_reports` and `nad_narrative_violations` tables.

Uses COPY for violations and streaming from MongoDB for memory efficiency.

Usage:
    python migrations/migrate_from_mongodb.py [--dry-run] [--limit N]
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from io import StringIO

from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

import psycopg

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

BATCH_SIZE = 50


def get_mongo_db():
    """Connect to MongoDB using env vars."""
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


def parse_date(date_str):
    """Parse YYYY.MM.DD date string to a Python date object."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str.strip(), "%Y.%m.%d").date()
    except ValueError:
        for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
            try:
                return datetime.strptime(date_str.strip(), fmt).date()
            except ValueError:
                continue
        logger.warning(f"Could not parse date: {date_str}")
        return None


def escape_copy(val):
    """Escape a value for COPY format."""
    if val is None:
        return "\\N"
    s = str(val)
    return s.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r")


def process_batch(batch_docs, conn, ingestion_id):
    """Process a batch of MongoDB documents into Postgres. Returns (added, skipped, violations, errors)."""
    added = 0
    skipped = 0
    viols = 0
    errs = 0

    # Prepare report data
    report_rows = []
    violation_map = {}

    for doc in batch_docs:
        source_url = doc.get("Source URL", "")
        source_id = source_url or doc.get("Report Title Arabic", str(doc["_id"]))

        report_date = parse_date(doc.get("Date"))
        if not report_date:
            skipped += 1
            continue

        title_arabic = doc.get("Report Title Arabic")
        title_english = doc.get("Report Title English")
        raw_data = doc.get("raw_data", [])
        scraped_at = doc.get("Timestamp")

        if scraped_at and isinstance(scraped_at, str):
            try:
                scraped_at = datetime.strptime(scraped_at, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                scraped_at = None

        report_rows.append((
            source_id, source_url, report_date, title_arabic,
            title_english, json.dumps(raw_data), scraped_at, ingestion_id,
        ))

        narrative_data = doc.get("narrative_data", [])
        if narrative_data:
            violation_map[source_id] = [
                (
                    v.get("region"), v.get("region_arabic"),
                    v.get("governorate"), v.get("governorate_arabic"),
                    v.get("type"), v.get("type_arabic"),
                    v.get("description_english"), v.get("description_arabic"),
                    v.get("translation_source"),
                )
                for v in narrative_data
            ]

    if not report_rows:
        return added, skipped, viols, errs

    try:
        with conn.cursor() as cur:
            # Insert reports one by one (need RETURNING id for FK)
            source_id_to_uuid = {}
            for row in report_rows:
                cur.execute(
                    """
                    INSERT INTO nad_reports (source_id, source_url, report_date, title_arabic,
                        title_english, raw_data, scraped_at, ingestion_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (source_id) DO NOTHING
                    RETURNING id, source_id
                    """,
                    row,
                )
                result = cur.fetchone()
                if result:
                    source_id_to_uuid[result[1]] = result[0]
                    added += 1
                else:
                    skipped += 1

            # Batch insert violations using COPY
            violation_rows = []
            for source_id, v_list in violation_map.items():
                report_uuid = source_id_to_uuid.get(source_id)
                if not report_uuid:
                    continue
                for v in v_list:
                    violation_rows.append((str(report_uuid),) + v)

            if violation_rows:
                buf = StringIO()
                for row in violation_rows:
                    line = "\t".join(escape_copy(val) for val in row)
                    buf.write(line + "\n")
                buf.seek(0)

                with cur.copy(
                    "COPY nad_narrative_violations (report_id, region, region_arabic, "
                    "governorate, governorate_arabic, violation_type, violation_type_arabic, "
                    "description_english, description_arabic, translation_source) FROM STDIN"
                ) as copy:
                    copy.write(buf.read())

                viols += len(violation_rows)

        conn.commit()

    except Exception as e:
        logger.error(f"Batch error: {e}")
        errs += 1
        conn.rollback()

    return added, skipped, viols, errs


def migrate(dry_run=False, limit=None):
    """Run the migration."""
    mongo_db = get_mongo_db()
    collection = mongo_db["new_daily_reports"]

    total = collection.count_documents({})
    logger.info(f"Found {total} reports in MongoDB")

    query_kwargs = {}
    if limit:
        logger.info(f"Processing first {limit} reports (--limit)")

    # Use batch_size to control MongoDB cursor batching
    cursor = collection.find(batch_size=BATCH_SIZE)
    if limit:
        cursor = cursor.limit(limit)

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        logger.error("DATABASE_URL not set")
        sys.exit(1)

    if dry_run:
        logger.info("DRY RUN — no data will be written to Postgres")
        reports_added = 0
        violations_added = 0
        for doc in cursor:
            reports_added += 1
            violations_added += len(doc.get("narrative_data", []))
        logger.info(f"Would migrate {reports_added} reports, {violations_added} violations")
        return

    conn = psycopg.connect(database_url)

    # Create ingestion log entry
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO ingestion_log (status) VALUES ('running') RETURNING id"
        )
        ingestion_id = cur.fetchone()[0]
    conn.commit()

    reports_added = 0
    reports_skipped = 0
    violations_added = 0
    errors = 0
    docs_processed = 0

    batch = []
    for doc in cursor:
        batch.append(doc)
        docs_processed += 1

        if len(batch) >= BATCH_SIZE:
            a, s, v, e = process_batch(batch, conn, ingestion_id)
            reports_added += a
            reports_skipped += s
            violations_added += v
            errors += e
            logger.info(
                f"Progress: {reports_added} reports, {violations_added} violations "
                f"({docs_processed} docs processed)"
            )
            batch = []

    # Process remaining
    if batch:
        a, s, v, e = process_batch(batch, conn, ingestion_id)
        reports_added += a
        reports_skipped += s
        violations_added += v
        errors += e

    # Finalize ingestion log
    with conn.cursor() as cur:
        status = "success" if errors == 0 else "partial"
        cur.execute(
            """
            UPDATE ingestion_log
            SET finished_at = now(), records_added = %s, records_skipped = %s,
                errors = %s, status = %s
            WHERE id = %s
            """,
            (reports_added, reports_skipped, errors, status, ingestion_id),
        )
    conn.commit()
    conn.close()

    logger.info("=" * 50)
    logger.info("Migration complete")
    logger.info(f"  Reports added:   {reports_added}")
    logger.info(f"  Reports skipped: {reports_skipped}")
    logger.info(f"  Violations added: {violations_added}")
    logger.info(f"  Errors:          {errors}")
    logger.info("=" * 50)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate NAD reports from MongoDB to Neon Postgres")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing to Postgres")
    parser.add_argument("--limit", type=int, help="Limit number of reports to migrate")
    args = parser.parse_args()

    migrate(dry_run=args.dry_run, limit=args.limit)
