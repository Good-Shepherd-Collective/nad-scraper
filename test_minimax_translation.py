#!/usr/bin/env python3
"""
Test MiniMax M2.5 Arabic-to-English Translation

Compares MiniMax M2.5 translations against existing translations for
narrative incident descriptions from the NAD daily reports (Postgres).

Usage:
    python test_minimax_translation.py                        # latest report, 8 samples
    python test_minimax_translation.py --date 2026-03-07      # specific date
    python test_minimax_translation.py --sample 5             # custom sample size
"""

import os
import sys
import argparse
from dotenv import load_dotenv
from db import get_conn
from minimax_translate import translate_with_minimax as _translate_minimax

load_dotenv()


def get_report_violations(date=None):
    """Get violations from a report, either by date or most recent."""
    from psycopg.rows import dict_row
    with get_conn(row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            if date:
                cur.execute("""
                    SELECT v.*, r.report_date
                    FROM nad_narrative_violations v
                    JOIN nad_reports r ON r.id = v.report_id
                    WHERE r.report_date = %s
                      AND v.description_arabic IS NOT NULL
                      AND v.description_arabic != ''
                    ORDER BY v.violation_type
                """, (date,))
            else:
                cur.execute("""
                    SELECT v.*, r.report_date
                    FROM nad_narrative_violations v
                    JOIN nad_reports r ON r.id = v.report_id
                    WHERE v.description_arabic IS NOT NULL
                      AND v.description_arabic != ''
                    ORDER BY r.report_date DESC, v.violation_type
                    LIMIT 100
                """)
            return cur.fetchall()


def select_diverse_samples(violations, sample_size):
    """Select a diverse sample, one per incident type."""
    by_type = {}
    for v in violations:
        t = v.get('violation_type', 'Unknown')
        if t not in by_type:
            by_type[t] = []
        by_type[t].append(v)

    samples = []
    for t in sorted(by_type.keys()):
        if by_type[t]:
            samples.append(by_type[t][0])
        if len(samples) >= sample_size:
            break

    if len(samples) < sample_size:
        seen = {s['id'] for s in samples}
        for t in sorted(by_type.keys(), key=lambda k: len(by_type[k]), reverse=True):
            for v in by_type[t]:
                if v['id'] not in seen:
                    samples.append(v)
                    seen.add(v['id'])
                    if len(samples) >= sample_size:
                        break
            if len(samples) >= sample_size:
                break

    return samples


def translate_with_minimax(arabic_text):
    """Translate Arabic text using the shared minimax_translate module.

    Returns (translation, 0, 0) -- token counts are not tracked by the shared module,
    but the interface is kept for compatibility with the test output format.
    """
    translation = _translate_minimax(arabic_text)
    return translation, 0, 0


def main():
    parser = argparse.ArgumentParser(description="Test MiniMax M2.5 Arabic→English translation")
    parser.add_argument("--date", type=str, help="Report date in YYYY-MM-DD format")
    parser.add_argument("--sample", type=int, default=8, help="Number of samples (default: 8)")
    args = parser.parse_args()

    if not os.getenv("DATABASE_URL"):
        print("Error: DATABASE_URL not set.")
        sys.exit(1)
    if not os.getenv("MINIMAX_API_KEY"):
        print("Error: MINIMAX_API_KEY not set.")
        sys.exit(1)

    print(f"Fetching violations{f' for {args.date}' if args.date else ' (most recent)'}...")
    violations = get_report_violations(args.date)

    if not violations:
        print("No violations found.")
        sys.exit(1)

    report_date = violations[0]['report_date']
    print(f"Using report: {report_date}")

    samples = select_diverse_samples(violations, args.sample)
    print(f"Selected {len(samples)} samples\n")

    total_in = total_out = 0
    results = []

    for i, v in enumerate(samples, 1):
        vtype = v.get('violation_type', 'Unknown')
        gov = v.get('governorate', 'Unknown')
        arabic = v['description_arabic']
        existing = v.get('description_english', '')

        print(f"[{i}/{len(samples)}] Translating: {vtype} | {gov}...")

        try:
            minimax, in_tok, out_tok = translate_with_minimax(arabic)
            total_in += in_tok
            total_out += out_tok
        except Exception as e:
            minimax = f"[ERROR] {e}"
            in_tok = out_tok = 0

        results.append({
            'type': vtype, 'gov': gov, 'arabic': arabic,
            'existing': existing, 'minimax': minimax,
            'in_tok': in_tok, 'out_tok': out_tok,
        })

    estimated_cost = (total_in / 1_000_000 * 0.30) + (total_out / 1_000_000 * 1.20)

    output_file = "minimax_translation_test.md"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(f"# MiniMax M2.5 Translation Test\n\n")
        f.write(f"**Report date:** {report_date}\n")
        f.write(f"**Samples:** {len(results)}\n\n")

        for i, r in enumerate(results, 1):
            f.write(f"---\n\n## [{i}] {r['type']} | {r['gov']}\n\n")
            f.write(f"**Arabic:**\n> {r['arabic']}\n\n")
            f.write(f"**Existing:**\n> {r['existing']}\n\n")
            f.write(f"**MiniMax:**\n> {r['minimax']}\n\n")

        f.write(f"---\n\n## Cost\n\n")
        f.write(f"Input: {total_in} tokens, Output: {total_out} tokens\n")
        f.write(f"Estimated cost: ${estimated_cost:.4f}\n")

    print(f"\nDone. Output: {output_file}")


if __name__ == "__main__":
    main()
