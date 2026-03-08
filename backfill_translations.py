#!/usr/bin/env python3
"""
Backfill existing description_english fields with MiniMax M2.5 translations.

Reads violations from Postgres that don't yet have translation_source='minimax',
translates them, and updates in place.
"""

import argparse
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

load_dotenv()

import psycopg
from psycopg.rows import dict_row

from minimax_translate import translate_batch

DEFAULT_WORKERS = 100
BATCH_SIZE = 10  # items per API call
MAX_RUNTIME_SECONDS = 110 * 60  # 110 min


def translate_batch_items(batch):
    """Translate a batch of (id, arabic_text) tuples. Returns list of (id, translation, success)."""
    ids = [b[0] for b in batch]
    texts = [b[1] for b in batch]

    translations = translate_batch(texts)

    results = []
    for vid, original, translated in zip(ids, texts, translations):
        success = translated != original
        results.append((vid, translated, success))
    return results


def main():
    parser = argparse.ArgumentParser(
        description="Backfill description_english with MiniMax translations"
    )
    parser.add_argument("--date", help="Process violations from a single report date (YYYY-MM-DD)")
    parser.add_argument("--limit", type=int, help="Max number of violations to process")
    parser.add_argument("--dry-run", action="store_true", help="Print only, no DB writes")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                        help=f"Concurrent API calls (default: {DEFAULT_WORKERS})")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE,
                        help=f"Items per API call (default: {BATCH_SIZE})")
    args = parser.parse_args()
    batch_size = args.batch_size

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        print("DATABASE_URL not set")
        sys.exit(1)

    conn = psycopg.connect(database_url, row_factory=dict_row)

    # Find violations needing translation
    query = """
        SELECT v.id, v.description_arabic
        FROM nad_narrative_violations v
        JOIN nad_reports r ON r.id = v.report_id
        WHERE v.description_arabic IS NOT NULL
          AND v.description_arabic != ''
          AND (v.translation_source IS NULL OR v.translation_source != 'minimax')
    """
    params = []
    if args.date:
        query += " AND r.report_date = %s"
        params.append(args.date)
    if args.limit:
        query += f" LIMIT {args.limit}"

    with conn.cursor() as cur:
        cur.execute(query, params)
        to_translate = [(row["id"], row["description_arabic"]) for row in cur.fetchall()]

    print(f"Found {len(to_translate)} violations needing translation")
    if args.dry_run:
        print("[DRY RUN] No changes will be written.")

    if not to_translate:
        print("Nothing to do.")
        conn.close()
        return

    total_translated = 0
    total_failed = 0
    start_time = time.time()

    # Split into batches and process concurrently
    batches = [to_translate[i:i + batch_size] for i in range(0, len(to_translate), batch_size)]

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(translate_batch_items, batch): i for i, batch in enumerate(batches)}

        for future in as_completed(futures):
            if time.time() - start_time >= MAX_RUNTIME_SECONDS:
                print(f"\n** Reached {MAX_RUNTIME_SECONDS // 60}-min runtime limit. Will resume on next run. **")
                break

            try:
                results = future.result()
                for vid, translated, success in results:
                    if success and not args.dry_run:
                        with conn.cursor() as cur:
                            cur.execute(
                                """
                                UPDATE nad_narrative_violations
                                SET description_english = %s, translation_source = 'minimax'
                                WHERE id = %s
                                """,
                                (translated, vid),
                            )
                        total_translated += 1
                    elif success:
                        total_translated += 1
                    else:
                        total_failed += 1
            except Exception as e:
                total_failed += 1
                print(f"  Batch error: {e}")

            if total_translated % 100 < batch_size:
                elapsed = time.time() - start_time
                rate = total_translated / elapsed if elapsed > 0 else 0
                print(f"  Progress: {total_translated} translated, {total_failed} failed, {rate:.0f}/sec")

    if not args.dry_run:
        conn.commit()
    conn.close()

    elapsed = time.time() - start_time
    print(f"\n{'=' * 50}")
    print(f"Translated: {total_translated}")
    print(f"Failed:     {total_failed}")
    print(f"Time:       {elapsed:.0f}s ({elapsed/60:.1f} min)")
    if args.dry_run:
        print("[DRY RUN] No changes written.")


if __name__ == "__main__":
    main()
