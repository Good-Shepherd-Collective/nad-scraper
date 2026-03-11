import requests
from bs4 import BeautifulSoup
import json
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import os
import signal
from dotenv import load_dotenv
from translations import normalize_translation, CUSTOM_TRANSLATIONS
from minimax_translate import translate_with_minimax, translate_batch
from db import get_unscraped_urls, insert_report, create_ingestion_entry, update_ingestion_entry

# Load environment variables from .env file (no-op if vars already set, e.g. in CI)
load_dotenv()

# Thread-local sessions for connection pooling (requests.Session is not thread-safe)
_thread_local = threading.local()


def _get_session():
    """Return a thread-local requests.Session instance."""
    if not hasattr(_thread_local, "session"):
        _thread_local.session = requests.Session()
    return _thread_local.session

# Setup logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


# Lock for thread-safe file writes
_failed_urls_lock = threading.Lock()


# Function to handle failed URLs
def write_failed_url(url, error):
    failed_url = {
        "url": url,
        "error": str(error),
        "timestamp": datetime.now().isoformat()
    }
    filename = 'failed_urls.json'

    with _failed_urls_lock:
        try:
            try:
                with open(filename, 'r') as f:
                    data = json.load(f)
            except (FileNotFoundError, json.JSONDecodeError):
                data = []
            data.append(failed_url)
            with open(filename, 'w') as f:
                json.dump(data, f, indent=2)
            logger.info(f"Failed URL written to {filename}: {url}")
        except Exception as e:
            logger.error(f"Error writing failed URL to file: {str(e)}")


# Flag for graceful shutdown
_shutdown_requested = False


# Function to handle signals for graceful shutdown
def signal_handler(sig, frame):
    global _shutdown_requested
    _shutdown_requested = True
    logger.info("Interrupt received, shutting down after current tasks complete.")


# Register the signal handler
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


# Function to parse report date from title
def parse_report_date(title):
    months = {
        'كانون الثاني': '01', 'شباط': '02', 'آذار': '03', 'نيسان': '04',
        'أيار': '05', 'حزيران': '06', 'تموز': '07', 'آب': '08',
        'أيلول': '09', 'تشرين الأول': '10', 'تشرين الثاني': '11', 'كانون الأول': '12',
        'تمور': '07', 'كانون الول': '12'  # Additional mappings
    }

    parts = title.split()
    if len(parts) < 2:
        raise ValueError(f"Invalid title format: {title}")

    year = parts[-1]
    if not year.isdigit() or len(year) != 4:
        raise ValueError(f"Invalid year in title: {title}")

    # Find the month in the title (match longest key first to avoid
    # partial matches, e.g. 'تشرين الأول' vs 'تشرين الثاني')
    month = None
    for key in sorted(months.keys(), key=len, reverse=True):
        if key in title:
            month = months[key]
            break

    if month is None:
        raise ValueError(f"Unable to parse month from title: {title}")

    # Try to find the day
    day = next((part for part in parts if part.isdigit() and int(part) <= 31), '01')
    day = day.zfill(2)

    return f"{year}.{month}.{day}"


# Function to extract Highcharts data from HTML (replaces Selenium)
def extract_highcharts_data(soup):
    """Extract chart categories and values from the Highcharts data-chart attribute."""
    chart_div = soup.find('div', attrs={'data-chart': True})
    if not chart_div:
        logger.warning("No Highcharts data-chart div found")
        return [], []

    try:
        chart_config = json.loads(chart_div['data-chart'])

        categories = []
        for axis in chart_config.get('xAxis', []):
            categories = axis.get('categories', [])
            if categories:
                break

        values = []
        for series in chart_config.get('series', []):
            values = series.get('data', [])
            if values:
                break

        logger.info(f"Extracted {len(categories)} categories and {len(values)} values from Highcharts data")
        return categories, values
    except (json.JSONDecodeError, KeyError) as e:
        logger.error(f"Failed to parse Highcharts data: {e}")
        return [], []


# Function to scrape the NAD page content
def scrape_nad_page(url, html_content=None, soup=None):
    logger.info(f"Starting to scrape URL: {url}")

    if soup is None:
        if html_content is None:
            try:
                response = _get_session().get(url, timeout=30)
                response.raise_for_status()
                html_content = response.text
                logger.info(f"Successfully fetched the page. Status code: {response.status_code}")
            except Exception as e:
                logger.error(f"Failed to fetch the page. Error: {e}")
                return None

        soup = BeautifulSoup(html_content, 'lxml')

    logger.info("Successfully parsed the HTML content")

    main_content = soup.select_one('#block-system-main > div > div > div > div > div > div.panel-pane.pane-views-panes.pane-violations-reports-panel-pane-4.violations-records.clearfix')

    if not main_content:
        logger.error("Main content div not found using the new selector")
        return None

    logger.info("Found main content div using the new selector")

    data = {}

    view_groupings = main_content.find_all('div', class_='view-grouping')
    logger.info(f"Found {len(view_groupings)} view-grouping divs")

    if not view_groupings:
        logger.warning("No view-grouping divs found. Trying alternative structure.")
        data = scrape_alternative_structure(main_content)
    else:
        for region in view_groupings:
            region_header = region.find('div', class_='view-grouping-header')
            if region_header:
                region_name = region_header.text.strip()
                logger.info(f"Processing region: {region_name}")
                data[region_name] = {}

                for governorate in region.find_all('h3'):
                    governorate_name = governorate.text.strip()
                    logger.info(f"Processing governorate: {governorate_name}")
                    data[region_name][governorate_name] = []

                    content_div = governorate.find_next_sibling('div', class_='views-row')

                    if content_div:
                        for violation in content_div.find_all('div', class_='field-collection-view'):
                            violation_type_div = violation.find('div', class_='field-item even')
                            description_div = violation.find('div', class_='field-name-field-body')

                            if violation_type_div and description_div:
                                violation_type = violation_type_div.text.strip()
                                description = description_div.find('p').text.strip() if description_div.find('p') else ""

                                logger.info(f"Found violation - Type: {violation_type}")
                                data[region_name][governorate_name].append({
                                    'type': violation_type,
                                    'description': description
                                })
                            else:
                                logger.warning(f"Incomplete violation data found in governorate: {governorate_name}")
                    else:
                        logger.warning(f"No content found for governorate: {governorate_name}")
            else:
                logger.warning("View-grouping div found but no header")

    logger.info("Finished scraping the page")
    return data


# Alternative structure scraping function
def scrape_alternative_structure(main_content):
    data = {"Alternative Structure": []}
    for item in main_content.find_all('div', class_='views-row'):
        violation_type_div = item.find('div', class_='field-item even')
        description_div = item.find('div', class_='field-name-field-body')

        if violation_type_div and description_div:
            violation_type = violation_type_div.text.strip()
            description = description_div.find('p').text.strip() if description_div.find('p') else ""

            logger.info(f"Found violation - Type: {violation_type}")
            data["Alternative Structure"].append({
                'type': violation_type,
                'description': description
            })
    return data


# Function to restructure NAD page data with batch translation
def restructure_data(nad_data, custom_translations):
    violations = []
    for region, governorates in nad_data.items():
        region_english = custom_translations.get(region, translate_with_minimax(region))
        region_english = normalize_translation(region_english)
        for governorate, incidents in governorates.items():
            governorate_english = custom_translations.get(governorate, translate_with_minimax(governorate))
            governorate_english = normalize_translation(governorate_english)
            for incident in incidents:
                incident_type = custom_translations.get(incident['type'], incident['type'])
                incident_type = normalize_translation(incident_type)

                violation = {
                    "region": region_english,
                    "region_arabic": region,
                    "governorate": governorate_english,
                    "governorate_arabic": governorate,
                    "type": incident_type,
                    "type_arabic": incident['type'],
                    "description_arabic": incident['description'],
                }
                violations.append(violation)

    # Batch translate all descriptions at once
    descriptions = [v['description_arabic'] for v in violations]
    non_empty_indices = [i for i, d in enumerate(descriptions) if d and d.strip()]

    if non_empty_indices:
        texts_to_translate = [descriptions[i] for i in non_empty_indices]
        logger.info(f"Batch translating {len(texts_to_translate)} descriptions with MiniMax...")

        # Translate in concurrent chunks to avoid API timeouts
        BATCH_SIZE = 20
        batches = [
            texts_to_translate[i:i + BATCH_SIZE]
            for i in range(0, len(texts_to_translate), BATCH_SIZE)
        ]
        # Fire all batch calls concurrently
        batch_results = [None] * len(batches)
        with ThreadPoolExecutor(max_workers=min(len(batches), 3)) as executor:
            future_to_idx = {
                executor.submit(translate_batch, batch): idx
                for idx, batch in enumerate(batches)
            }
            for future in as_completed(future_to_idx):
                idx = future_to_idx[future]
                batch_results[idx] = future.result()
        all_translated = []
        for result in batch_results:
            all_translated.extend(result)

        for idx, translated_text in zip(non_empty_indices, all_translated):
            violations[idx]['description_english'] = translated_text
            violations[idx]['translation_source'] = 'minimax'

    # Fill in empty descriptions (no translation was performed)
    for v in violations:
        if 'description_english' not in v:
            v['description_english'] = ''
            v['translation_source'] = None

    return violations


# Function to process each URL (no Selenium required)
def process_url(url, ingestion_id=None):
    logger.info(f'Starting scraping process for URL: {url}')

    custom_translations = CUSTOM_TRANSLATIONS

    try:
        # Fetch the page once, reuse for both chart and narrative extraction
        response = _get_session().get(url, timeout=30)
        response.raise_for_status()
        html_content = response.text
        logger.info(f'Successfully fetched page. Status code: {response.status_code}')

        soup = BeautifulSoup(html_content, 'lxml')

        # Extract title from HTML
        report_title_arabic = None
        for selector in ['h2.page-header', 'h2', 'h1']:
            title_element = soup.select_one(selector)
            if title_element:
                report_title_arabic = title_element.text.strip()
                logger.info(f'Found title using selector: {selector}')
                break

        if report_title_arabic:
            logger.info(f'Report title (Arabic): {report_title_arabic}')
            report_title_english = translate_with_minimax(report_title_arabic)
            logger.info(f'Report title (English): {report_title_english}')
            try:
                report_date = parse_report_date(report_title_arabic)
                logger.info(f'Parsed report date: {report_date}')
            except ValueError as e:
                logger.error(f"Error parsing date: {str(e)}")
                year = next((part for part in report_title_arabic.split() if part.isdigit() and len(part) == 4),
                            datetime.now().strftime('%Y'))
                report_date = f"{year}.01.01"
                logger.info(f'Using fallback date: {report_date}')
        else:
            logger.error('Failed to find report title')
            report_title_arabic = None
            report_title_english = "Unknown Title"
            report_date = datetime.now().strftime('%Y.%m.%d')
            logger.info(f'Using current date as report date: {report_date}')

        scraped_data = {
            'Report Title Arabic': report_title_arabic,
            'Report Title English': report_title_english,
            'Date': report_date,
            'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "raw_data": [],
            "narrative_data": []
        }

        # Extract Highcharts data from HTML (no Selenium needed)
        categories, values = extract_highcharts_data(soup)

        for desc_arabic, value in zip(categories, values):
            desc_english = custom_translations.get(desc_arabic.strip(), translate_with_minimax(desc_arabic.strip()))
            desc_english = normalize_translation(desc_english)

            logger.info(f'Processing: {desc_arabic} -> {desc_english} with value {value}')
            scraped_data["raw_data"].append({
                'type_arabic': desc_arabic.strip(),
                'type': desc_english,
                'value': str(value)
            })

        # Extract narrative data (reuse already-fetched HTML)
        nad_data = scrape_nad_page(url, soup=soup)
        if nad_data:
            violations = restructure_data(nad_data, custom_translations)
            scraped_data["narrative_data"].extend(violations)
            logger.info(f'Added {len(violations)} violations from NAD page data.')
        else:
            logger.warning('Failed to scrape NAD page data')

        inserted = insert_report(scraped_data, url, ingestion_id=ingestion_id)
        logger.info(f'Scraping process for URL {url} completed.')
        return inserted

    except Exception as e:
        logger.error(f"An unexpected error occurred while processing URL {url}: {str(e)}")
        write_failed_url(url, e)
        logger.info(f'Scraping process for URL {url} completed.')
        return False


# Main function to execute the script
def main():
    try:
        logger.info('Starting the main process.')
        url_list = get_unscraped_urls()
        if not url_list:
            logger.info("No new URLs to process. Exiting.")
            return

        logger.info(f"Processing {len(url_list)} URLs.")

        ingestion_id = create_ingestion_entry()
        records_added = 0
        errors = 0

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {
                executor.submit(process_url, url, ingestion_id): url
                for url in url_list
            }
            for i, future in enumerate(as_completed(futures), 1):
                if _shutdown_requested:
                    logger.info("Shutdown requested, cancelling remaining tasks.")
                    for f in futures:
                        f.cancel()
                    break
                url = futures[future]
                try:
                    inserted = future.result()
                    if inserted:
                        records_added += 1
                    logger.info(f"Completed URL {i}/{len(url_list)}: {url}")
                except Exception as e:
                    errors += 1
                    logger.error(f"URL {i}/{len(url_list)} failed: {url} - {e}")

        update_ingestion_entry(ingestion_id, records_added=records_added, errors=errors)

    except Exception as e:
        logger.error(f"An unexpected error occurred in the main function: {str(e)}")
    finally:
        logger.info("Scraping process finished.")


if __name__ == "__main__":
    main()
