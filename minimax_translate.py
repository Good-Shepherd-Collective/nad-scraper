"""
MiniMax M2.5 Arabic-to-English translation for narrative descriptions.

Shared module used by scraper.py (for new reports) and backfill scripts.
Supports single-item and batch translation modes.
"""

import os
import re
import time
import logging
import threading
import requests
from translations import CUSTOM_TRANSLATIONS

logger = logging.getLogger(__name__)

MINIMAX_API_URL = "https://api.minimax.io/v1/chat/completions"

# Thread-safe in-memory translation cache
_cache = {}
_cache_lock = threading.Lock()

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
   - CORRECT: "Israeli forces arrested Magd Salah Darbas in Al-Isawiya village."
   - WRONG: "Israeli forces arrested in Al-Isawiya village citizen Magd Salah Darbas."
   - CORRECT: "Israeli forces detained two young men at the Qalandiya checkpoint."
   - WRONG: "Israeli forces detained at the Qalandiya checkpoint two young men."
6. Standardized terminology:
   - "Israeli forces" (not "the occupation forces", not "Israeli occupation forces", not "IOF")
   - "settlers" (not "colonists")
   - "raided" or "stormed" for اقتحم
   - "arrested" or "detained" for اعتقل
   - "checkpoint" for حاجز
   - "settlement" for مستوطنة
7. Correct place name mappings (CRITICAL - use these exact English names):
{chr(10).join(f'   - {arabic} → {english}' for arabic, english in PLACE_NAMES.items())}"""

SINGLE_INSTRUCTION = "\n\nTranslate the following Arabic text to English. Return ONLY the translation, no explanations."

BATCH_INSTRUCTION = """

You will receive multiple Arabic texts separated by "---[N]---" markers where N is the item number.
Translate each one to English. Return translations in the EXACT same format:
---[1]---
(translation of item 1)
---[2]---
(translation of item 2)
...and so on. Return ONLY the translations with their markers, nothing else."""


def _call_minimax(user_content, system_prompt, max_retries=3):
    """Make an API call to MiniMax with retry. Returns response text or None on error."""
    api_key = os.getenv("MINIMAX_API_KEY")
    if not api_key:
        logger.error("MINIMAX_API_KEY not set")
        return None

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": "MiniMax-M2.5",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ],
        "temperature": 0.3,
    }

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.post(MINIMAX_API_URL, json=payload, headers=headers, timeout=120)
            resp.raise_for_status()
            data = resp.json()

            raw = data["choices"][0]["message"]["content"].strip()
            # Strip <think>...</think> reasoning blocks if present
            return re.sub(r'<think>.*?</think>\s*', '', raw, flags=re.DOTALL).strip()

        except Exception as e:
            logger.warning(f"MiniMax attempt {attempt}/{max_retries} failed: {e}")
            if attempt < max_retries:
                time.sleep(2 ** attempt)  # exponential backoff: 2s, 4s

    logger.error("MiniMax translation failed after all retries")
    return None


def translate_with_minimax(text):
    """
    Translate a single Arabic text to English using MiniMax M2.5.
    Falls back to original text on error. Results are cached in-memory.
    """
    if not text or not text.strip():
        return text

    with _cache_lock:
        if text in _cache:
            return _cache[text]

    result = _call_minimax(text, SYSTEM_PROMPT + SINGLE_INSTRUCTION)
    translated = result if result else text

    with _cache_lock:
        _cache[text] = translated

    return translated


def translate_batch(texts):
    """
    Translate multiple Arabic texts in a single API call.
    Checks cache first; only sends uncached texts to the API.

    Args:
        texts: list of Arabic strings to translate

    Returns:
        list of English translations (same length as input).
        Failed items return the original Arabic text.
    """
    if not texts:
        return []

    # Check cache for each item
    translations = [None] * len(texts)
    uncached = []  # (original_index, text) pairs
    with _cache_lock:
        for i, text in enumerate(texts):
            if text in _cache:
                translations[i] = _cache[text]
            else:
                uncached.append((i, text))

    if not uncached:
        return translations

    # Only translate uncached texts
    uncached_texts = [text for _, text in uncached]

    if len(uncached_texts) == 1:
        api_results = [translate_with_minimax(uncached_texts[0])]
    else:
        # Build numbered input
        parts = []
        for i, text in enumerate(uncached_texts, 1):
            parts.append(f"---[{i}]---")
            parts.append(text)
        user_content = "\n".join(parts)

        result = _call_minimax(user_content, SYSTEM_PROMPT + BATCH_INSTRUCTION)

        if not result:
            api_results = list(uncached_texts)
        else:
            api_results = list(uncached_texts)  # default to originals
            for i in range(1, len(uncached_texts) + 1):
                pattern = rf'---\[{i}\]---\s*'
                next_pattern = rf'---\[{i + 1}\]---' if i < len(uncached_texts) else r'\Z'
                match = re.search(pattern + r'(.*?)' + f'(?={next_pattern})', result, re.DOTALL)
                if match:
                    translated = match.group(1).strip()
                    if translated:
                        api_results[i - 1] = translated

    # Store results in cache and fill translations
    with _cache_lock:
        for (orig_idx, text), translated in zip(uncached, api_results):
            _cache[text] = translated
            translations[orig_idx] = translated

    return translations
