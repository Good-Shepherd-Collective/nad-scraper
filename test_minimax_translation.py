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
import re
import sys
import argparse
import requests
from dotenv import load_dotenv
from translations import CUSTOM_TRANSLATIONS
from db import get_conn

load_dotenv()

MINIMAX_API_KEY = os.getenv("MINIMAX_API_KEY")
MINIMAX_API_URL = "https://api.minimax.io/v1/chat/completions"

PLACE_NAMES = {
    arabic: english
    for arabic, english in CUSTOM_TRANSLATIONS.items()
    if arabic in (
        'القدس', 'رام الله', 'جنين', 'طوباس', 'طولكرم', 'قلقيلية',
        'نابلس', 'سلفيت', 'أريحا', 'بيت لحم', 'الخليل',
        'شمال غزة', 'غزة', 'الوسطى', 'خانيونس', 'رفح',
        'الضفة الغربية', 'قطاع غزة',
    )
}

SYSTEM_PROMPT = f"""You are a professional Arabic-to-English translator specializing in human rights reporting from Palestine.

RULES:
1. Use ACTIVE VOICE always. Write "Israeli forces raided" not "the village was raided by forces".
2. Preserve ALL facts exactly: times, dates, names, numbers, locations.
3. Use past tense throughout.
4. Write clear, direct, journalistic English. No commentary or editorializing.
5. Use natural English word order.
6. Standardized terminology:
   - "Israeli forces" (not "the occupation forces")
   - "settlers" (not "colonists")
   - "raided" or "stormed" for اقتحم
   - "arrested" or "detained" for اعتقل
   - "checkpoint" for حاجز
   - "settlement" for مستوطنة
7. Correct place name mappings:
{chr(10).join(f'   - {arabic} → {english}' for arabic, english in PLACE_NAMES.items())}

Translate the following Arabic text to English. Return ONLY the translation, no explanations."""


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
    """Translate Arabic text using MiniMax M2.5 API."""
    headers = {
        "Authorization": f"Bearer {MINIMAX_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": "MiniMax-M2.5",
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": arabic_text},
        ],
        "temperature": 0.3,
    }

    resp = requests.post(MINIMAX_API_URL, json=payload, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    raw = data["choices"][0]["message"]["content"].strip()
    translation = re.sub(r'<think>.*?</think>\s*', '', raw, flags=re.DOTALL).strip()
    usage = data.get("usage", {})

    return translation, usage.get("prompt_tokens", 0), usage.get("completion_tokens", 0)


def main():
    parser = argparse.ArgumentParser(description="Test MiniMax M2.5 Arabic→English translation")
    parser.add_argument("--date", type=str, help="Report date in YYYY-MM-DD format")
    parser.add_argument("--sample", type=int, default=8, help="Number of samples (default: 8)")
    args = parser.parse_args()

    if not os.getenv("DATABASE_URL"):
        print("Error: DATABASE_URL not set.")
        sys.exit(1)
    if not MINIMAX_API_KEY:
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
