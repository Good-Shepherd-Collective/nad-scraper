#!/usr/bin/env python3
"""
Test MiniMax M2.5 Arabic-to-English Translation

Compares MiniMax M2.5 translations against existing Google Translate output
for narrative incident descriptions from the NAD daily reports.

Usage:
    python test_minimax_translation.py                        # latest report, 8 samples
    python test_minimax_translation.py --date 2026.02.13      # specific date
    python test_minimax_translation.py --sample 5             # custom sample size
"""

import os
import re
import sys
import argparse
import requests
from dotenv import load_dotenv
from translations import CUSTOM_TRANSLATIONS
from db import get_db, MONGO_URI, MONGO_USER, MONGO_PASSWORD, MONGO_DB_NAME

# Load environment variables
load_dotenv()

# MiniMax API
MINIMAX_API_KEY = os.getenv("MINIMAX_API_KEY")
MINIMAX_API_URL = "https://api.minimax.io/v1/chat/completions"

# Build place name mappings from translations.py for the system prompt
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
5. Use natural English word order. Do NOT mirror Arabic sentence structure. The verb's object comes immediately after the verb, then the location:
   - CORRECT: "Israeli occupation forces arrested Magd Salah Darbas in Al-Isawiya village."
   - WRONG: "Israeli occupation forces arrested in Al-Isawiya village citizen Magd Salah Darbas."
   - CORRECT: "Israeli occupation forces detained two young men at the Qalandiya checkpoint."
   - WRONG: "Israeli occupation forces detained at the Qalandiya checkpoint two young men."
6. Standardized terminology:
   - "Israeli forces" (not "the occupation forces", not "Israeli occupation forces", not "IOF")
   - "settlers" (not "colonists")
   - "raided" or "stormed" for اقتحم
   - "arrested" or "detained" for اعتقل
   - "checkpoint" for حاجز
   - "settlement" for مستوطنة
7. Correct place name mappings (CRITICAL - use these exact English names):
{chr(10).join(f'   - {arabic} → {english}' for arabic, english in PLACE_NAMES.items())}

Translate the following Arabic text to English. Return ONLY the translation, no explanations."""


def get_db_connection():
    """Establish MongoDB connection."""
    return get_db()


def get_report(db, date=None):
    """Get a report from new_daily_reports, either by date or most recent."""
    collection = db['new_daily_reports']
    if date:
        report = collection.find_one({"Date": date})
        if not report:
            print(f"No report found for date: {date}")
            sys.exit(1)
    else:
        report = collection.find_one({}, sort=[("Date", -1)])
        if not report:
            print("No reports found in database.")
            sys.exit(1)
    return report


def select_diverse_samples(narrative_data, sample_size):
    """Select a diverse sample of narrative items, one per incident type."""
    # Group by incident type
    by_type = {}
    for item in narrative_data:
        t = item.get('type', 'Unknown')
        if t not in by_type:
            by_type[t] = []
        by_type[t].append(item)

    samples = []

    # Pick one from each type first
    for t in sorted(by_type.keys()):
        items = by_type[t]
        # Prefer items that have both arabic and english descriptions
        candidates = [
            i for i in items
            if i.get('description_arabic') and i.get('description_english')
        ]
        if candidates:
            samples.append(candidates[0])
        if len(samples) >= sample_size:
            break

    # If we still need more, grab additional items from the largest groups
    if len(samples) < sample_size:
        seen_ids = {id(s) for s in samples}
        for t in sorted(by_type.keys(), key=lambda k: len(by_type[k]), reverse=True):
            for item in by_type[t]:
                if id(item) not in seen_ids and item.get('description_arabic') and item.get('description_english'):
                    samples.append(item)
                    seen_ids.add(id(item))
                    if len(samples) >= sample_size:
                        break
            if len(samples) >= sample_size:
                break

    return samples


def translate_with_minimax(arabic_text):
    """Translate Arabic text using MiniMax M2.5 API. Returns (translation, input_tokens, output_tokens)."""
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
    # Strip <think>...</think> reasoning blocks if present
    translation = re.sub(r'<think>.*?</think>\s*', '', raw, flags=re.DOTALL).strip()
    usage = data.get("usage", {})
    input_tokens = usage.get("prompt_tokens", 0)
    output_tokens = usage.get("completion_tokens", 0)

    return translation, input_tokens, output_tokens


def main():
    parser = argparse.ArgumentParser(description="Test MiniMax M2.5 Arabic→English translation")
    parser.add_argument("--date", type=str, help="Report date in YYYY.MM.DD format")
    parser.add_argument("--sample", type=int, default=8, help="Number of samples to translate (default: 8)")
    args = parser.parse_args()

    # Validate env vars
    if not all([MONGO_URI, MONGO_USER, MONGO_PASSWORD, MONGO_DB_NAME]):
        print("Error: Missing MongoDB environment variables. Check your .env file.")
        sys.exit(1)
    if not MINIMAX_API_KEY:
        print("Error: MINIMAX_API_KEY not set. Add it to your .env file.")
        sys.exit(1)

    # Connect and fetch report
    print("Connecting to MongoDB...")
    db = get_db_connection()

    print(f"Fetching report{f' for {args.date}' if args.date else ' (most recent)'}...")
    report = get_report(db, args.date)
    report_date = report.get('Date', 'Unknown')
    print(f"Using report: {report_date}")

    narrative_data = report.get('narrative_data', [])
    if not narrative_data:
        print("No narrative data found in this report.")
        sys.exit(1)

    # Select diverse samples
    samples = select_diverse_samples(narrative_data, args.sample)
    print(f"Selected {len(samples)} samples across {len(set(s.get('type') for s in samples))} incident types\n")

    # Translate and collect results
    total_input_tokens = 0
    total_output_tokens = 0
    results = []

    for i, item in enumerate(samples, 1):
        incident_type = item.get('type', 'Unknown')
        governorate = item.get('governorate', 'Unknown')
        arabic = item.get('description_arabic', '')
        google_english = item.get('description_english', '')
        minimax_english = None
        in_tok = out_tok = 0

        print(f"[{i}/{len(samples)}] Translating: {incident_type} | {governorate}...")

        try:
            minimax_english, in_tok, out_tok = translate_with_minimax(arabic)
            total_input_tokens += in_tok
            total_output_tokens += out_tok
        except Exception as e:
            minimax_english = f"[ERROR] {e}"

        results.append({
            'incident_type': incident_type,
            'governorate': governorate,
            'arabic': arabic,
            'google': google_english,
            'minimax': minimax_english,
            'in_tok': in_tok,
            'out_tok': out_tok,
        })

    # Cost summary
    # MiniMax M2.5 pricing: ~$0.0015/1K input, ~$0.0060/1K output (approximate)
    estimated_cost = (total_input_tokens / 1000 * 0.0015) + (total_output_tokens / 1000 * 0.0060)

    # Write markdown file
    output_file = "minimax_translation_test.md"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(f"# MiniMax M2.5 Translation Test\n\n")
        f.write(f"**Report date:** {report_date}\n\n")
        f.write(f"**Samples:** {len(results)} across {len(set(r['incident_type'] for r in results))} incident types\n\n")

        for i, r in enumerate(results, 1):
            f.write(f"---\n\n")
            f.write(f"## [{i}/{len(results)}] {r['incident_type']} | {r['governorate']}\n\n")
            f.write(f"**Arabic:**\n> {r['arabic']}\n\n")
            f.write(f"**Google Translate (current):**\n> {r['google']}\n\n")
            f.write(f"**MiniMax M2.5:**\n> {r['minimax']}\n\n")
            f.write(f"*Tokens: {r['in_tok']} in + {r['out_tok']} out*\n\n")

        f.write(f"---\n\n")
        f.write(f"## Cost Summary\n\n")
        f.write(f"| Metric | Value |\n")
        f.write(f"|--------|-------|\n")
        f.write(f"| Items translated | {len(results)} |\n")
        f.write(f"| Total input tokens | {total_input_tokens} |\n")
        f.write(f"| Total output tokens | {total_output_tokens} |\n")
        f.write(f"| Estimated cost | ${estimated_cost:.4f} |\n")

    print(f"\nDone. Output saved to: {output_file}")


if __name__ == "__main__":
    main()
