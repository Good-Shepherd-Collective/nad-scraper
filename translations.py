"""
Translation normalization mapping for consistent terminology.

This module provides a standardized mapping to fix common translation
variations from Google Translate and ensure consistency across the database.

Also includes custom Arabic->English translations for common terms to
avoid slow Google Translate API calls.
"""

# Custom Arabic to English translations (used before Google Translate)
# This drastically reduces API calls for commonly recurring terms
CUSTOM_TRANSLATIONS = {
    # Violation types
    'قصف جوي': 'Airstrikes',
    'قتل': 'Deaths',
    'إغلاق كلي لمنافذ ومعابر': 'Closure of entrances and crossings',
    'حواجز عسكرية': 'Military checkpoints',
    'تدمير ممتلكات': 'Destruction of property',
    'جرح': 'Injuries',
    'إعتداء مستوطنين': 'Settler attacks',
    'حواجز عسكرية مفاجئة': 'Temporary checkpoints',
    'إغلاق': 'Closures',
    'هدم منازل': 'Home demolitions',
    'إعتقال': 'Arrests',
    'إطلاق نار فلسطيني': 'Palestinian live ammunition',
    'إطلاق نار': 'Israeli live ammunition',
    'إغلاق طرق': 'Road closures',
    'إقتحام': 'Invasions',
    'إعتداء على الطواقم الطبية': 'Assaults on medical personnel',
    'إعتداء على قطاع التعليم': 'Assaults on education sector',
    'إعتداء على أماكن العبادة': 'Assault on places of worship',
    'إعتداء على أماكن العبادة ': 'Assault on places of worship',  # with trailing space
    'مصادرة ممتلكات': 'Confiscation of property',
    'إحتجاز': 'Detention',
    'إيذاء جسدي': 'Physical harm',
    'نشاطات إستيطانية': 'Settlement activities',
    'مصادرة أراضي': 'Land confiscation',
    'إستيلاء على منازل': 'Occupying homes',
    'إحتلال منازل': 'Occupying homes',

    # Regions
    'الضفة الغربية': 'West Bank',
    'قطاع غزة': 'Gaza Strip',

    # Governorates - West Bank
    'القدس': 'Jerusalem',
    'رام الله': 'Ramallah',
    'جنين': 'Jenin',
    'طوباس': 'Tubas',
    'طولكرم': 'Tulkarm',
    'قلقيلية': 'Qalqilya',
    'نابلس': 'Nablus',
    'سلفيت': 'Salfit',
    'أريحا': 'Jericho',
    'بيت لحم': 'Bethlehem',
    'الخليل': 'Hebron',
    'جميع المحافظات': 'All Governorates',

    # Governorates - Gaza Strip
    'شمال غزة': 'North Gaza',
    'غزة': 'Gaza',
    'الوسطى': 'Central Gaza',
    'خانيونس': 'Khan Yunis',
    'رفح': 'Rafah',

    # Common title words
    'التقارير اليومية': 'Daily reports',
    'التقرير اليومية': 'Daily reports',  # variant
}

# Normalization map: incorrect_variant -> correct_translation
TRANSLATION_NORMALIZATIONS = {
    # Settler-related
    'Settlers attack': 'Settler attacks',
    'Settlement assault': 'Settler attacks',

    # Place names (Google Translate errors)
    'fetal': 'Jenin',  # جنين (Jenin city) mistranslated as "fetal"
    'Fetal': 'Jenin',

    # Common variations that may occur
    'Physical abuse': 'Physical harm',
    'My body abuse': 'Physical harm',  # Known bad translation
    'Assaults on property': 'Assault on property',
    'Assault on properties': 'Assault on property',
    'Occupying home': 'Occupying homes',
    'Occupation of home': 'Occupying homes',
    'House seizure': 'Occupying homes',
    'Home demolition': 'Home demolitions',
    'Temporary checkpoint': 'Temporary checkpoints',
    'Military checkpoint': 'Military checkpoints',
    'Road closure': 'Road closures',
    'Closing area': 'Closing areas',
    'Confiscation of properties': 'Confiscation of property',
    'Land confiscations': 'Land confiscation',
    'Settlement activity': 'Settlement activities',
    'Airstrike': 'Airstrikes',
    'detention': 'Detention',  # lowercase variant

    # Standardize similar concepts
    'Arrest': 'Arrests',
    'Invasion': 'Invasions',
    'Injury': 'Injuries',
    'Death': 'Deaths',
    'Closure': 'Closures',
}


# Substring replacements for mistranslations embedded in longer text.
# These are applied via str.replace() so they fix occurrences anywhere in a string.
# Use case-sensitive entries; add both cases if needed.
SUBSTRING_FIXES = {
    'fetal': 'Jenin',
    'Fetal': 'Jenin',
}


def normalize_translation(text):
    """
    Normalize a translated text to ensure consistency.

    Args:
        text (str): The text to normalize

    Returns:
        str: The normalized text
    """
    if not text or not isinstance(text, str):
        return text

    # Direct mapping (exact match)
    if text in TRANSLATION_NORMALIZATIONS:
        return TRANSLATION_NORMALIZATIONS[text]

    # Substring replacements for critical mistranslations that appear
    # inside longer strings (e.g. "fetal" in narrative descriptions)
    for wrong, correct in SUBSTRING_FIXES.items():
        if wrong in text:
            text = text.replace(wrong, correct)

    return text


def normalize_raw_data(raw_data):
    """
    Normalize all types in a raw_data array.

    Args:
        raw_data (list): List of dictionaries with 'type' and 'value' keys

    Returns:
        list: Normalized raw_data array
    """
    if not raw_data:
        return raw_data

    for item in raw_data:
        if 'type' in item and not item['type'].endswith('_arabic'):
            original = item['type']
            normalized = normalize_translation(original)

            if original != normalized:
                print(f"  Normalized: '{original}' → '{normalized}'")
                item['type'] = normalized

    return raw_data


if __name__ == "__main__":
    # Test the normalization
    test_cases = [
        'Settlers attack',
        'Settler attacks',
        'Physical abuse',
        'Physical harm',
        'My body abuse',
    ]

    print("Testing normalization:")
    for test in test_cases:
        result = normalize_translation(test)
        status = "✓" if result == normalize_translation(result) else "⚠"
        print(f"{status} '{test}' → '{result}'")
