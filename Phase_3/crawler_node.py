from mpi4py import MPI
import time
import logging
import threading
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

# Setup organized logging
def setup_logger(role, rank):
    logger = logging.getLogger(f"{role}-{rank}")
    formatter = logging.Formatter('%(asctime)s | %(name)s | %(levelname)s | %(message)s')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    return logger

CRAWL_DELAY = 2 

def extract_urls(base_url, html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    links = set()
    for tag in soup.find_all('a', href=True):
        href = tag['href']
        joined_url = urljoin(base_url, href)
        if urlparse(joined_url).scheme in ['http', 'https']:
            links.add(joined_url)
    return list(links)

def extract_text(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    return soup.get_text(separator=' ', strip=True)

def send_heartbeat(comm, rank):
    while True:
        comm.send(f"Heartbeat from Crawler {rank}", dest=0, tag=98)
        time.sleep(5) 

def crawler_process():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    logger = setup_logger("Crawler", rank)
    status = MPI.Status()

    logger.info(f"Crawler node {rank} started.")

    heartbeat_thread = threading.Thread(target=send_heartbeat, args=(comm, rank), daemon=True)
    heartbeat_thread.start()

    while True:
        url_info = comm.recv(source=0, tag=0, status=status)

        if url_info is None:
            logger.info(f"Crawler {rank} received shutdown signal. Exiting.")
            break

        url_to_crawl, depth = url_info
        logger.info(f"Crawler {rank} received URL: '{url_to_crawl}' (Depth {depth})")

        try:
            time.sleep(CRAWL_DELAY)
            response = requests.get(url_to_crawl, timeout=10)
            response.raise_for_status()

            html_content = response.text
            new_urls = extract_urls(url_to_crawl, html_content)
            page_text = extract_text(html_content)

            content_package = {
                'source_url': url_to_crawl,
                'source_depth': depth,
                'new_urls': new_urls
            }
            comm.send(content_package, dest=0, tag=1)

            indexer_rank = size - 1
            comm.send({'url': url_to_crawl, 'text': page_text}, dest=indexer_rank, tag=2)

            comm.send(f"Crawler {rank} successfully crawled: {url_to_crawl}", dest=0, tag=99)

        except Exception as e:
            logger.error(f"Crawler {rank} error: {e}")
            comm.send(f"Error crawling {url_to_crawl}: {e}", dest=0, tag=999)

if __name__ == '__main__':
    crawler_process()
