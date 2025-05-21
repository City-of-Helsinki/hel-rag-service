import datetime
import logging
import uuid
import urllib.parse
import requests
from bs4 import BeautifulSoup
import re

from openwebui_data_importer import OpenWebUIDataImporter

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class WebScraperImporter(OpenWebUIDataImporter):


    def read_data(self):
        base_url = self.config['data']['base_url']
        start_paths = self.config['data']['paths']
        content_selectors = self.config['data']['content_selectors']
        entry_config = self.config['data']['entry_config']
        visited_urls = set()
        data_json = []
        max_pages = self.config['data'].get('max_pages', 100)  # Set a default max limit
        pages_scraped = 0

        def normalize_url(url):
            parsed_url = urllib.parse.urlparse(url)
            normalized_url = parsed_url._replace(query="").geturl()
            return normalized_url

        def add_space_to_camel_case(text):
            # Add a space before each uppercase letter, except the first one
            return re.sub(r'(?<!^)(?=[A-Z])', ' ', text)

        def scrape_page(url):
            # Safety check to avoid scraping too many pages
            nonlocal pages_scraped
            nonlocal visited_urls
            nonlocal data_json

            if pages_scraped >= max_pages:
                logging.info(f"Reached maximum limit of {max_pages} pages. Stopping import.")
                return

            # Normalize the URL to avoid duplicates (removes query parameters)
            # Note: this works for only for sites with good SEO practices.
            normalized_url = normalize_url(url)
            if normalized_url in visited_urls:
                logging.debug(f"Been there before: {normalized_url}")
                return
            # Add the URL to the set of visited URLs to avoid revisiting
            visited_urls.add(normalized_url)
            logging.info(f"Scraping URL: {url}")
            try:
                # Fetch the page content
                response = requests.get(url)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')

                # Read the title using the title selector (CSS selector). Fallback to "Untitled" if not found.
                if entry_config['title_selector']:
                    title_el = soup.select_one(entry_config['title_selector'])
                    if title_el:
                        title = title_el.get_text(strip=True)
                    else:
                        title = "Untitled"
                else:
                    title = "Untitled"
                # Read the content using content selectors (CSS selectors)
                text_content = f"{title}\n\n" + ' '.join(
                    soup.select_one(selector).get_text(strip=False) for selector in content_selectors)

                # Fix text that has CamelCase words to have whitespace between them
                text_content = add_space_to_camel_case(text_content)

                # Set metadata for the file
                file_config = {'date_imported': datetime.datetime.now().isoformat()}
                # Define the file name
                file_name = soup.select_one(entry_config['file_name_selector']).get_text(strip=True) if entry_config[
                    'file_name_selector'] else f"unknown_entry_{url.replace('/', '_')}"
                file_name = self.safe_filename(file_name) + "_" + uuid.uuid4().hex[:6]
                # Append the data to the list
                data_json.append({"content": text_content, "file_config": file_config, "file_name": file_name})
                logging.info(f"Scraped content for URL: {url}")
                pages_scraped += 1

                # Follow links to the next pages if the configuration allows it
                allowed_url = f"{base_url}{path}"
                if self.config['data']['follow_links']:
                    for link in soup.select('a[href]'):
                        next_page = link['href']

                        # Convert relative URLs to absolute URLs
                        if next_page.startswith('/'):
                            next_page = base_url + next_page

                        # Check if the absolute URL is allowed (URL is under same domain and base path)
                        if next_page.startswith(allowed_url):
                            logging.debug(f"Found link to follow: {url}")
                            scrape_page(next_page)

            except Exception as e:
                logging.error(f"Failed to scrape URL: {url}, error: {e}")

        # Start scraping from the configured paths
        logging.info(f"Starting scraping from site: {base_url}")
        logging.info(f"Paths to scrape: {start_paths}")
        for path in start_paths:
            scrape_page(f"{base_url}{path}")
        return data_json

    def safe_filename(self, file_name):
        return file_name.replace(' ', '_').replace('/', '_').replace('\\', '_').replace(':', '_')