#!/usr/bin/env python3
"""
Backfill existing description_english fields with MiniMax M2.5 translations.

Uses a single shared thread pool to translate items across multiple reports
concurrently. Items already marked translation_source="minimax" are skipped
on re-runs.
"""

import argparse
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

load_dotenv()

from minimax_translate import translate_batch
from db import get_db

DEFAULT_WORKERS = 100
BATCH_SIZE = 10  # items per API call
MAX_RUNTIME_SECONDS = 110 * 60  # 110 min — exit gracefully before the 120-min job timeout


def translate_batch_items(batch):
    """Translate a batch of (index, arabic_text) tuples. Returns list of (index, translation, success)."""
    indices = [b[0] for b in batch]
    texts = [b[1] for b in batch]

    translations = translate_batch(texts)

    results = []
    for idx, original, translated in zip(indices, texts, translations):
        success = translated != original
        results.append((idx, translated, success))
    return results


def prepare_report(report):
    """Extract items needing translation from a report. Returns (to_translate, skipped)."""
    narrative_data = report.get("narrative_data", [])
    if not narrative_data:
        return [], 0

    to_translate = []
    skipped = 0

    for j, item in enumerate(narrative_data):
        arabic = item.get("description_arabic", "")
        if not arabic or not arabic.strip():
            skipped += 1
            continue
        if item.get("translation_source") == "minimax":
            skipped += 1
            continue
        to_translate.append((j, arabic))

    return to_translate, skipped


def main():
    parser = argparse.ArgumentParser(
        description="Backfill description_english with MiniMax translations"
    )
    parser.add_argument("--date", help="Process a single report by date (YYYY.MM.DD)")
    parser.add_argument("--limit", type=int, help="Max number of reports to process")
    parser.add_argument("--dry-run", action="store_true", help="Print only, no DB writes")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                        help=f"Concurrent API calls (default: {DEFAULT_WORKERS})")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE,
                        help=f"Items per API call (default: {BATCH_SIZE})")
    args = parser.parse_args()
    batch_size = args.batch_size

    db = get_db()
    collection = db["new_daily_reports"]

    # Build query
    query = {}
    if args.date:
        query["Date"] = args.date

    # Load all report IDs upfront to avoid cursor timeout (CursorNotFound)
    id_cursor = collection.find(query, {"_id": 1})
    if args.limit:
        id_cursor = id_cursor.limit(args.limit)
    report_ids = [doc["_id"] for doc in id_cursor]

    print(f"Starting backfill: {args.workers} workers, {batch_size} items/batch...")
    print(f"  Found {len(report_ids)} reports to process.")
    if args.dry_run:
        print("[DRY RUN] No changes will be written to the database.")
    print()

    total_translated = 0
    total_skipped = 0
    reports_processed = 0
    start_time = time.time()
    stopped_early = False

    projection = {"_id": 1, "Date": 1, "narrative_data": 1}

    # Process reports in chunks — submit all batches from multiple reports
    # into a single shared pool so all 100 workers stay busy
    REPORT_CHUNK = 20  # process 20 reports at a time
    i = 0

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        while i < len(report_ids):
            if time.time() - start_time >= MAX_RUNTIME_SECONDS:
                stopped_early = True
                print(f"\n  ** Reached {MAX_RUNTIME_SECONDS // 60}-min runtime limit, "
                      f"stopping gracefully. Next run will continue. **\n")
                break

            # Grab the next chunk of reports
            chunk_ids = report_ids[i:i + REPORT_CHUNK]
            i += REPORT_CHUNK

            # Fetch and prepare all reports in this chunk
            # Each entry: (report, narrative_data, to_translate, skipped)
            chunk_reports = []
            for rid in chunk_ids:
                report = collection.find_one({"_id": rid}, projection)
                if not report:
                    continue
                to_translate, skipped = prepare_report(report)
                total_skipped += skipped
                if not to_translate:
                    reports_processed += 1
                    continue
                chunk_reports.append((report, to_translate))

            if not chunk_reports:
                continue

            # Submit ALL batches from ALL reports in this chunk to the pool
            # future -> (report_index, batch)
            futures = {}
            for cr_idx, (report, to_translate) in enumerate(chunk_reports):
                batches = [
                    to_translate[b:b + batch_size]
                    for b in range(0, len(to_translate), batch_size)
                ]
                for batch in batches:
                    fut = pool.submit(translate_batch_items, batch)
                    futures[fut] = cr_idx

            # Collect results grouped by report
            report_results = {idx: {} for idx in range(len(chunk_reports))}
            report_translated = {idx: 0 for idx in range(len(chunk_reports))}
            report_failed = {idx: 0 for idx in range(len(chunk_reports))}

            for future in as_completed(futures):
                cr_idx = futures[future]
                try:
                    batch_results = future.result()
                    for idx, new_text, success in batch_results:
                        if success:
                            report_results[cr_idx][idx] = new_text
                            report_translated[cr_idx] += 1
                        else:
                            report_failed[cr_idx] += 1
                except Exception:
                    report_failed[cr_idx] += 1

            # Write results back to DB per report
            for cr_idx, (report, _) in enumerate(chunk_reports):
                results = report_results[cr_idx]
                translated = report_translated[cr_idx]
                failed = report_failed[cr_idx]
                report_date = report.get("Date", "Unknown")
                narrative_data = report["narrative_data"]

                if results and not args.dry_run:
                    for idx, new_text in results.items():
                        narrative_data[idx]["description_english"] = new_text
                        narrative_data[idx]["translation_source"] = "minimax"
                    collection.update_one(
                        {"_id": report["_id"]},
                        {"$set": {"narrative_data": narrative_data}},
                    )

                total_translated += translated
                reports_processed += 1

                status = "[DRY RUN] " if args.dry_run else ""
                warn = f", {failed} failed" if failed else ""
                print(f"  {status}{report_date}: {translated} translated{warn}")

            if reports_processed % 100 < REPORT_CHUNK:
                elapsed = time.time() - start_time
                rate = total_translated / elapsed if elapsed > 0 else 0
                print(f"\n  --- Progress: {reports_processed} reports | "
                      f"{total_translated} translated | "
                      f"{rate:.0f} items/sec | "
                      f"{elapsed:.0f}s elapsed ---\n")

    elapsed = time.time() - start_time
    print(f"\n{'=' * 50}")
    print(f"Summary:")
    print(f"  Reports processed:  {reports_processed}")
    print(f"  Translated:         {total_translated}")
    print(f"  Skipped:            {total_skipped}")
    print(f"  Time elapsed:       {elapsed:.0f}s ({elapsed/60:.1f} min)")
    if total_translated > 0:
        print(f"  Avg rate:           {total_translated / elapsed:.1f} items/sec")
    if stopped_early:
        print(f"  Status:             Stopped early (will resume on next run)")
    else:
        print(f"  Status:             Complete")
    if args.dry_run:
        print(f"  [DRY RUN] No changes were written.")


if __name__ == "__main__":
    main()
