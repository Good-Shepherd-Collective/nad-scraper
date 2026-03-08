import os
import json
import logging
from datetime import datetime

from dotenv import load_dotenv
import psycopg
from psycopg.rows import dict_row

load_dotenv()

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "")


def get_conn(**kwargs):
    """Return a psycopg3 connection using DATABASE_URL."""
    return psycopg.connect(DATABASE_URL, **kwargs)


def get_unscraped_urls():
    """Find URLs that have been collected but not yet scraped.

    Replaces the MongoDB aggregation pipeline with a SQL LEFT JOIN.
    """
    with get_conn(row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT u.link
                FROM urls u
                LEFT JOIN nad_reports r ON r.source_url = u.link
                WHERE r.id IS NULL
            """)
            return [row["link"] for row in cur.fetchall()]


def get_existing_url_dates():
    """Get all dates already in the urls table (for dedup during URL collection)."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT date FROM urls")
            return {row[0] for row in cur.fetchall()}


def insert_urls(url_data):
    """Insert new URLs into the urls table.

    Args:
        url_data: dict of {date: {title, link}} from scrape_all_pages
    """
    if not url_data:
        return 0

    new_count = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for date, item in url_data.items():
                cur.execute(
                    """
                    INSERT INTO urls (date, title, link)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (link) DO NOTHING
                    """,
                    (date, item["title"], item["link"]),
                )
                if cur.rowcount > 0:
                    new_count += 1
        conn.commit()

    logger.info(f"Inserted {new_count} new URLs")
    return new_count


def insert_report(data, url, ingestion_id=None):
    """Insert a scraped report and its violations into Postgres.

    Args:
        data: dict with Report Title Arabic/English, Date, raw_data, narrative_data
        url: source URL
        ingestion_id: UUID of the current ingestion run

    Returns:
        True if inserted, False if already existed
    """
    source_id = url or data.get("Report Title Arabic", "")

    with get_conn() as conn:
        with conn.cursor() as cur:
            # Parse date
            date_str = data.get("Date", "")
            report_date = None
            for fmt in ("%Y.%m.%d", "%Y-%m-%d", "%Y/%m/%d"):
                try:
                    report_date = datetime.strptime(date_str.strip(), fmt).date()
                    break
                except ValueError:
                    continue

            if not report_date:
                logger.warning(f"Could not parse date: {date_str}")
                return False

            # Insert report
            cur.execute(
                """
                INSERT INTO nad_reports
                    (source_id, source_url, report_date, title_arabic, title_english,
                     raw_data, scraped_at, ingestion_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (source_id) DO NOTHING
                RETURNING id
                """,
                (
                    source_id,
                    url,
                    report_date,
                    data.get("Report Title Arabic"),
                    data.get("Report Title English"),
                    json.dumps(data.get("raw_data", [])),
                    data.get("Timestamp"),
                    ingestion_id,
                ),
            )
            row = cur.fetchone()
            if row is None:
                logger.info(f"Report already exists for '{source_id}'. Skipping.")
                return False

            report_id = row[0]

            # Insert narrative violations
            narrative_data = data.get("narrative_data", [])
            if narrative_data:
                cur.executemany(
                    """
                    INSERT INTO nad_narrative_violations
                        (report_id, region, region_arabic, governorate, governorate_arabic,
                         violation_type, violation_type_arabic, description_english,
                         description_arabic, translation_source)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    [
                        (
                            report_id,
                            v.get("region"), v.get("region_arabic"),
                            v.get("governorate"), v.get("governorate_arabic"),
                            v.get("type"), v.get("type_arabic"),
                            v.get("description_english"), v.get("description_arabic"),
                            v.get("translation_source"),
                        )
                        for v in narrative_data
                    ],
                )

        conn.commit()

    logger.info(f"Inserted report {report_date} with {len(data.get('narrative_data', []))} violations")
    return True


def create_ingestion_entry():
    """Create a new ingestion_log entry and return its UUID."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO ingestion_log (status) VALUES ('running') RETURNING id"
            )
            row = cur.fetchone()
        conn.commit()
    return row[0]


def update_ingestion_entry(ingestion_id, records_added=0, records_skipped=0, errors=0):
    """Finalize an ingestion_log entry."""
    status = "success" if errors == 0 else ("partial" if records_added > 0 else "failed")
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE ingestion_log
                SET finished_at = now(), records_added = %s, records_skipped = %s,
                    errors = %s, status = %s
                WHERE id = %s
                """,
                (records_added, records_skipped, errors, status, ingestion_id),
            )
        conn.commit()
