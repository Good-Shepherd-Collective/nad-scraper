import os
import requests
from bs4 import BeautifulSoup
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from pymongo.server_api import ServerApi

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db import get_mongo_client, MONGO_DB_NAME

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

session = requests.Session()

MONGO_COLLECTION = os.getenv("MONGO_COLLECTION_URLS", "Urls")

# Create MongoDB client
try:
    logger.info("Attempting to create MongoDB client...")
    client = get_mongo_client(server_api=ServerApi('1'), serverSelectionTimeoutMS=5000)
    logger.info("MongoDB client created. Attempting to ping...")
    client.admin.command('ping')
    logger.info("Successfully connected to MongoDB!")
    db = client[MONGO_DB_NAME]
    collection = db[MONGO_COLLECTION]
except Exception as e:
    logger.error(f"Failed to connect to MongoDB: {str(e)}")
    raise

def scrape_page(url):
    logger.info(f"Scraping URL: {url}")
    try:
        response = session.get(url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch URL: {url}. Error: {e}")
        return {}

    soup = BeautifulSoup(response.content, 'lxml')

    results = {}

    for div in soup.find_all('div', class_='views-row'):
        title_div = div.find('div', class_='views-field-title')
        if title_div:
            link_element = title_div.find('a')
            if link_element:
                full_link = 'https://www.nad.ps' + link_element['href']
                title = link_element.text.strip()

                date_div = div.find('div', class_='views-field-field-date')
                if date_div:
                    date_span = date_div.find('span', class_='date-display-single')
                    if date_span and 'content' in date_span.attrs:
                        date_str = date_span['content'].split('T')[0]  # Extract date part

                        logger.info(f"Extracted: Title: {title}, Date: {date_str}, Link: {full_link}")
                        results[date_str] = {'title': title, 'link': full_link}
                    else:
                        logger.warning(f"Could not extract date for title: {title}")
                else:
                    logger.warning(f"No date div found for title: {title}")
            else:
                logger.warning(f"No link found in title div: {title_div}")
        else:
            logger.warning(f"No title div found in row: {div}")

    logger.info(f"Scraped {len(results)} items from {url}")
    return results

def scrape_all_pages(start_page=None, end_page=0, existing_dates=None):
    base_url = 'https://www.nad.ps/ar/violations-reports/daily-report'
    all_data = {}

    if existing_dates is None:
        existing_dates = set()

    # Build list of URLs to fetch: main page + numbered pages
    urls_in_order = [base_url]
    if start_page is not None:
        for page in range(start_page, end_page, -1):
            urls_in_order.append(f"{base_url}?page={page}")

    # Fetch all pages in parallel
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [(url, executor.submit(scrape_page, url)) for url in urls_in_order]

    # Merge results in order; early-exit if all dates on a page already exist
    for url, future in futures:
        page_data = future.result()
        if page_data and existing_dates and all(d in existing_dates for d in page_data):
            logger.info(f"All dates on {url} already exist in DB. Stopping early.")
            break
        all_data.update(page_data)
        logger.info(f"Completed scraping {url}. Total items so far: {len(all_data)}")

    return all_data

def get_existing_dates():
    return set(collection.distinct('date'))

def upload_to_mongodb(data, existing_dates):
    new_items = 0
    for date, item in data.items():
        if date not in existing_dates:
            document = {
                "date": date,
                "title": item['title'],
                "link": item['link']
            }
            collection.insert_one(document)
            new_items += 1

    logger.info(f"Uploaded {new_items} new items to MongoDB")

def main():
    logger.info("Starting scraping process for all pages")

    try:
        existing_dates = get_existing_dates()
        logger.info(f"Found {len(existing_dates)} existing dates in MongoDB")

        data = scrape_all_pages(start_page=4, existing_dates=existing_dates)

        upload_to_mongodb(data, existing_dates)

        logger.info(f"Scraped a total of {len(data)} items, uploaded only new items to MongoDB")
    except Exception as e:
        logger.error(f"An error occurred during execution: {str(e)}")
        raise

if __name__ == "__main__":
    main()
