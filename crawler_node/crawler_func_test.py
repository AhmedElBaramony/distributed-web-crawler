import time
import logging
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Crawler - %(levelname)s - %(message)s')

CRAWL_DELAY = 2  # seconds

def extract_urls(base_url, html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    links = set()
    for tag in soup.find_all('link' or 'a', href=True):
        href = tag['href']
        joined_url = urljoin(base_url, href)
        if urlparse(joined_url).scheme in ['http', 'https']:
            links.add(joined_url)
    return list(links)

def extract_text(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    return soup.get_text(separator=' ', strip=True)

def simple_crawler(url_to_crawl):
    logging.info(f"Starting crawl for URL: {url_to_crawl}")

    try:
        time.sleep(CRAWL_DELAY)
        response = requests.get(url_to_crawl, timeout=10)
        response.raise_for_status()

        html_content = response.text
        new_urls = extract_urls(url_to_crawl, html_content)
        page_text = extract_text(html_content)

        print("\n✅ Extracted URLs:")
        for link in new_urls:
            print(link)

        print("\n📝 Extracted Page Text (first 500 chars):")
        print(page_text[:500])

    except Exception as e:
        logging.error(f"Error crawling {url_to_crawl}: {e}")

if __name__ == '__main__':
    url_input = input("Enter a URL to crawl: ")
    simple_crawler(url_input)