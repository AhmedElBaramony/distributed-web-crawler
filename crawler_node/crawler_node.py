from mpi4py import MPI
import time
import logging
import requests
from bs4 import BeautifulSoup  # For parsing HTML.
from urllib.parse import urljoin, urlparse # To combine relative URLs with the base URL. /////// To check that the link is an HTTP/HTTPS link.

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Crawler - %(levelname)s - %(message)s')

CRAWL_DELAY = 2  # Basic politeness delay (in seconds)

def extract_urls(base_url, html_content):
    """Extract URLs from <a href=""> tags in the page"""
    soup = BeautifulSoup(html_content, 'html.parser') # Parse HTML content
    links = set() # Use a set to avoid duplicates
    
    # Find all <a> tags with href attributes
    for tag in soup.find_all('a', href=True):
        href = tag['href'] # Get the href attribute
        joined_url = urljoin(base_url, href) # Combine base URL with href (to handle relative URLs)
        if urlparse(joined_url).scheme in ['http', 'https']: # Check if the URL is HTTP or HTTPS
            links.add(joined_url) # Add to set to avoid duplicates 
    return list(links)

def extract_text(html_content): # Extract visible text from HTML (For keyword extraction)
    """Extract visible text from HTML for potential indexing"""
    soup = BeautifulSoup(html_content, 'html.parser') # Parse HTML content
    return soup.get_text(separator=' ', strip=True) # Get all text from the page and strip leading/trailing whitespace

def crawler_process():
    comm = MPI.COMM_WORLD # Initialize MPI communication
    rank = comm.Get_rank() # Get the rank of the process
    size = comm.Get_size() # Get the total number of processes

    logging.info(f"Crawler node started with rank {rank} of {size}")

    while True:
        status = MPI.Status() # Create a status object to receive messages (Message Structure)
        url_to_crawl = comm.recv(source=0, tag=0, status=status) # Receive URL from Master (rank 0)

        if not url_to_crawl:
            logging.info(f"Crawler {rank} received shutdown signal. Exiting.")
            break

        logging.info(f"Crawler {rank} received URL: {url_to_crawl}")

        try:
            time.sleep(CRAWL_DELAY) # Basic politeness delay
            response = requests.get(url_to_crawl, timeout=10) # Send GET request to the URL
            response.raise_for_status() # Check for HTTP errors (200 OK , 404 Not Found, etc.)

            html_content = response.text # Get the HTML content of the page
            new_urls = extract_urls(url_to_crawl, html_content) # Extract URLs from the page (As a list)
            page_text = extract_text(html_content) # Extract visible text from the page (For keyword extraction)

            # Send extracted URLs to Master
            comm.send(new_urls, dest=0, tag=1)

            # Send content to Indexer (tag 2)
            content_package = {
                'url': url_to_crawl,
                'text': page_text
            }
            indexer_rank = size - 1  # Assuming last process is the Indexer
            comm.send(content_package, dest=indexer_rank, tag=2)

            # Send status to Master
            comm.send(f"Crawler {rank} successfully crawled: {url_to_crawl}", dest=0, tag=99)

        except Exception as e:
            logging.error(f"Crawler {rank} error: {e}")
            comm.send(f"Error crawling {url_to_crawl}: {e}", dest=0, tag=999)

if __name__ == '__main__':
    crawler_process()